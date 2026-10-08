// Larger acceptance scenarios derived from the assignment's required behavior.
// CLI scenarios share the production namespace, so serialize them within this file.
#[allow(dead_code)]
#[path = "support/process.rs"]
mod process_support;
#[allow(dead_code)]
#[path = "support/redis.rs"]
mod redis_support;
#[allow(dead_code)]
mod support;

use distributed_crawler::fetch::FetchOutcome;
use distributed_crawler::page::PageAnalysis;
use distributed_crawler::store::{JobStats, Store};
use distributed_crawler::url_rules::normalize_url;
use process_support::Process;
use redis_support::TestRedis;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use support::{Response, Site};
use tokio::sync::{Barrier, Mutex, Semaphore};
use tokio::task::JoinSet;
use url::Url;

static CLI_CLUSTER: Mutex<()> = Mutex::const_new(());

fn file(words: u64, links: Vec<Url>) -> FetchOutcome {
    FetchOutcome::File {
        extension: "html".to_owned(),
        analysis: PageAnalysis {
            links,
            word_count: words,
        },
    }
}

fn html(body: impl Into<String>) -> Response {
    Response::new(200, body).header("Content-Type", "text/html; charset=utf-8")
}

fn job_ids(output: &str) -> Vec<u64> {
    output
        .lines()
        .map(|line| line.split_whitespace().next().unwrap().parse().unwrap())
        .collect()
}

async fn production_store(test: &TestRedis) -> Store {
    let mut store = Store::connect(&test.url, "crawler").await.unwrap();
    assert!(
        store.active_jobs().await.unwrap().is_empty(),
        "CLI acceptance tests require dedicated Redis with no unrelated active jobs"
    );
    store
}

async fn wait_for_blocked(site: &Site, count: usize, nodes: &mut [Process]) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while site.blocked_requests() < count {
            for node in nodes.iter_mut() {
                assert!(node.is_running(), "Node exited: {}", node.errors());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Expected simultaneous requests did not reach the fixture");
}

async fn wait_for_done(store: &mut Store, jobs: &[u64], nodes: &mut [Process]) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut all_done = true;
            for &job in jobs {
                all_done &= store.status(job).await.unwrap().unwrap().done;
            }
            if all_done {
                break;
            }
            for node in nodes.iter_mut() {
                assert!(node.is_running(), "Node exited: {}", node.errors());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Cluster failed to complete the fixture jobs");
}

#[tokio::test]
async fn concurrent_resubmissions_reuse_waiting_running_and_completed_jobs() {
    let mut test = TestRedis::new().await;
    for phase in ["waiting", "running", "done"] {
        let base =
            normalize_url(&format!("https://example.test/{}/{phase}/", test.namespace)).unwrap();
        let job = test.store.submit(&base).await.unwrap();
        if phase != "waiting" {
            assert_eq!(test.store.claim(job).await.unwrap(), Some(base.to_string()));
        }
        if phase == "done" {
            test.store
                .finish(job, &base, Some(&file(2, vec![])))
                .await
                .unwrap();
        }
        let before = test.store.status(job).await.unwrap().unwrap();
        let barrier = Arc::new(Barrier::new(32));
        let mut submissions = JoinSet::new();
        for number in 0..32 {
            let redis_url = test.url.clone();
            let namespace = test.namespace.clone();
            let base = normalize_url(&format!("{base}#fragment-{number}")).unwrap();
            let barrier = Arc::clone(&barrier);
            submissions.spawn(async move {
                let mut store = Store::connect(&redis_url, &namespace).await.unwrap();
                barrier.wait().await;
                store.submit(&base).await.unwrap()
            });
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(result) = submissions.join_next().await {
                assert_eq!(result.unwrap(), job, "Duplicate created another job");
            }
        })
        .await
        .expect("Concurrent submissions did not finish");
        assert_eq!(test.store.status(job).await.unwrap().unwrap(), before);
        let seen: usize = redis::cmd("SCARD")
            .arg(test.key(&format!("job:{job}:seen")))
            .query_async(&mut test.connection)
            .await
            .unwrap();
        assert_eq!(seen, 1);
        if phase == "done" {
            let Some(JobStats::Done(stats)) = test.store.stats(job).await.unwrap() else {
                panic!("Completed results disappeared after duplicate submissions");
            };
            assert_eq!((stats.num_files, stats.total_word_count), (1, 2));
        }
    }
    test.cleanup().await;
}

#[tokio::test]
async fn concurrent_finishers_deduplicate_children_and_count_each_result_once() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    assert_eq!(test.store.claim(job).await.unwrap(), Some(base.to_string()));
    let parents: Vec<_> = (0..64)
        .map(|i| base.join(&format!("parent-{i}.html")).unwrap())
        .collect();
    let children: Vec<_> = (0..12)
        .map(|i| base.join(&format!("child-{i}.html")).unwrap())
        .collect();
    test.store
        .finish(job, &base, Some(&file(1, parents.clone())))
        .await
        .unwrap();
    for parent in &parents {
        assert_eq!(
            test.store.claim(job).await.unwrap(),
            Some(parent.to_string())
        );
    }
    let barrier = Arc::new(Barrier::new(64));
    let mut finishers = JoinSet::new();
    for (number, parent) in parents.into_iter().enumerate() {
        let redis_url = test.url.clone();
        let namespace = test.namespace.clone();
        let children = children.clone();
        let barrier = Arc::clone(&barrier);
        finishers.spawn(async move {
            let mut store = Store::connect(&redis_url, &namespace).await.unwrap();
            let outcome = match number % 8 {
                0 => None,
                1 => Some(FetchOutcome::Redirect {
                    target: Some(children[0].clone()),
                }),
                _ => Some(file(2, children)),
            };
            barrier.wait().await;
            assert!(store.finish(job, &parent, outcome.as_ref()).await.unwrap());
            assert!(!store.finish(job, &parent, outcome.as_ref()).await.unwrap());
        });
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(result) = finishers.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("Concurrent completions did not finish");
    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(
        (status.num_files, status.processed, status.unsuccessful),
        (49, 65, 8)
    );
    assert_eq!(
        (status.frontier, status.in_flight, status.done),
        (12, 0, false)
    );
    assert!(matches!(
        test.store.stats(job).await.unwrap(),
        Some(JobStats::Running)
    ));
    for child in &children {
        assert_eq!(
            test.store.claim(job).await.unwrap(),
            Some(child.to_string())
        );
        // Back-links and already-discovered siblings must not extend the queue.
        let mut links = children.clone();
        links.push(base.clone());
        test.store
            .finish(job, child, Some(&file(3, links)))
            .await
            .unwrap();
    }
    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!((status.processed, status.unsuccessful), (77, 8));
    assert_eq!(
        (status.frontier, status.in_flight, status.done),
        (0, 0, true)
    );
    let Some(JobStats::Done(stats)) = test.store.stats(job).await.unwrap() else {
        panic!("Final statistics were not immediately available");
    };
    assert_eq!(
        (stats.num_files, stats.num_exts, stats.total_word_count),
        (61, 1, 133)
    );
    assert_eq!(stats.ext_counts, HashMap::from([("html".to_owned(), 61)]));
    test.cleanup().await;
}

#[tokio::test]
async fn cli_submission_and_status_use_only_redis_without_fetching_the_site() {
    let _cluster = CLI_CLUSTER.lock().await;
    let site = Site::start(vec![("/site/", html("Must not be fetched"))]).await;
    let mut test = TestRedis::new().await;
    let mut store = production_store(&test).await;
    let duplicate = format!("{}#another-fragment", site.base);
    let other = site.url("other/");
    let mut submit = Process::start(
        &test.url,
        &["submit", site.base.as_str(), &duplicate, other.as_str()],
    );
    assert!(submit.wait().await.success(), "{}", submit.errors());
    let ids = job_ids(&submit.output());
    assert_eq!(ids.len(), 3);
    assert_eq!(ids[0], ids[1]);
    assert_ne!(ids[0], ids[2]);
    for &id in &[ids[0], ids[2]] {
        let status = test.cli(&["status", &id.to_string()]);
        assert!(status.status.success());
        let output = String::from_utf8(status.stdout).unwrap();
        assert!(output.contains("crawled: 0\nfrontier: 1\nin-flight: 0"));
        assert!(output.ends_with("done: false\n"));
        assert!(matches!(
            store.stats(id).await.unwrap(),
            Some(JobStats::Running)
        ));
    }
    assert!(
        site.requests().is_empty(),
        "The CLI contacted the website without a node"
    );
    test.cleanup_cli(&[(ids[0], site.base.as_str()), (ids[2], other.as_str())])
        .await;
    test.cleanup().await;
}

#[tokio::test]
async fn dense_cyclic_site_has_identical_stats_on_one_two_and_three_nodes() {
    let _cluster = CLI_CLUSTER.lock().await;
    const EXPECTED: &str = "files: 36\nextensions: 6\n  css: 1\n  html: 30\n  jpeg: 1\n  jpg: 1\n  pdf: 2\n  zip: 1\nwords: 82\n";
    for (node_count, workers) in [(1usize, 1u8), (1, 10), (2, 10), (3, 10)] {
        let gate = Arc::new(Semaphore::new(0));
        let foreign = Site::start(vec![("/site/", html("Outside origin"))]).await;
        let paths: Vec<String> = (0..24).map(|i| format!("/site/p{i}.html")).collect();
        let edges: String = paths
            .iter()
            .map(|path| {
                format!(
                    "<a href='{path}'></a><a href='{path}#intro'></a><a href='{path}#usage'></a>"
                )
            })
            .collect();
        let root = format!(
            "<title>Root</title><p>Ant ant 123</p>{edges}\
             <img src='photo.JPG'><img src='photo.jpeg'><a href='bundle.ZIP'></a>\
             <a href='manual.pdf'></a><link href='styles.CSS'><a href='fake.html'></a>\
             <a href='html.PDF'></a><a href='intro'></a>\
             <a href='query?id=1#intro'></a><a href='query?id=1#usage'></a>\
             <a href='query?id=2'></a><a href='redirect'></a>\
             <a href='missing'></a><a href='denied'></a><a href='server-error'></a>\
             <a href='../outside'></a><a href='/site-other/forbidden'></a>\
             <a href='/site/%2e%2e/outside'></a><a href='{}'></a>",
            foreign.base,
        );
        let parent = format!(
            "<p title='Hidden attribute words'>Ant ant 123 4ever zebra</p>\
             <!-- Hidden comment words --><script>Hidden script words</script>\
             <style>Hidden style words</style>{edges}\
             <a href='/site/'></a><a href='/site/shared.html#one'></a>\
             <a href='/site/shared.html#two'></a>",
        );
        let mut routes = vec![
            ("/site/", html(root)),
            (
                "/site/shared.html",
                html("Shared words<a href='/site/'></a>"),
            ),
            (
                "/site/photo.JPG",
                Response::new(200, "image").header("Content-Type", "image/jpeg"),
            ),
            (
                "/site/photo.jpeg",
                Response::new(200, "image").header("Content-Type", "image/jpeg"),
            ),
            (
                "/site/bundle.ZIP",
                Response::new(200, "zip").header("Content-Type", "application/zip"),
            ),
            (
                "/site/manual.pdf",
                Response::new(200, "pdf").header("Content-Type", "application/pdf"),
            ),
            (
                "/site/styles.CSS",
                Response::new(200, "These words must not count").header("Content-Type", "text/css"),
            ),
            (
                "/site/fake.html",
                Response::new(
                    200,
                    "<p>Must not count these words</p><a href='/site/trap.html'></a>",
                )
                .header("Content-Type", "application/octet-stream"),
            ),
            (
                "/site/html.PDF",
                Response::new(200, "Hello Rust").header("Content-Type", "TEXT/HTML; charset=utf-8"),
            ),
            ("/site/intro", html("Intro")),
            ("/site/query?id=1", html("Query")),
            ("/site/query?id=2", html("Query")),
            (
                "/site/redirect",
                Response::new(307, "Redirect body must not count")
                    .header("Location", "/site/p0.html#redirect"),
            ),
            (
                "/site/missing",
                Response::new(404, "Broken words must not count"),
            ),
            (
                "/site/denied",
                Response::new(403, "Forbidden words must not count"),
            ),
            (
                "/site/server-error",
                Response::new(500, "Error words must not count"),
            ),
            ("/outside", html("Must never fetch")),
            ("/site-other/forbidden", html("Must never fetch")),
            ("/site/trap.html", html("Binary body must not be parsed")),
        ];
        routes.extend(
            paths
                .iter()
                .map(|path| (path.as_str(), html(parent.clone()).gated(Arc::clone(&gate)))),
        );
        let site = Site::start(routes).await;
        let mut test = TestRedis::new().await;
        let mut store = production_store(&test).await;
        let narrow = site.url("p0.html");
        let html_pdf = site.url("html.PDF");
        let mut nodes: Vec<_> = (0..node_count)
            .map(|_| Process::start(&test.url, &["node", "--workers", &workers.to_string()]))
            .collect();
        for node in &mut nodes {
            node.wait_for_output("Node started").await;
        }
        let submitted = test.cli(&[
            "submit",
            site.base.as_str(),
            narrow.as_str(),
            html_pdf.as_str(),
        ]);
        assert!(submitted.status.success());
        let jobs = job_ids(&String::from_utf8(submitted.stdout).unwrap());
        assert_eq!(jobs.len(), 3);
        let mut follow = Process::start(&test.url, &["status", "-f", &jobs[0].to_string()]);
        follow.wait_for_output("done: false").await;
        // Twenty-four parents and the independent p0 job can be held at once.
        wait_for_blocked(
            &site,
            (node_count * usize::from(workers)).min(25),
            &mut nodes,
        )
        .await;
        assert!(!store.status(jobs[0]).await.unwrap().unwrap().done);
        let partial = test.cli(&["stats", &jobs[0].to_string()]);
        assert!(partial.status.success());
        let partial = String::from_utf8(partial.stdout).unwrap();
        assert!(partial.contains("not finished"));
        assert!(
            !partial.contains("files:"),
            "Partial totals leaked through stats"
        );
        gate.add_permits(25);
        wait_for_done(&mut store, &jobs, &mut nodes).await;
        for (&job, expected) in jobs.iter().zip([
            EXPECTED,
            "files: 1\nextensions: 1\n  html: 1\nwords: 3\n",
            "files: 1\nextensions: 1\n  pdf: 1\nwords: 2\n",
        ]) {
            let output = test.cli(&["stats", &job.to_string()]);
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                expected,
                "Incorrect statistics for {node_count} nodes / {workers} workers"
            );
            let status = store.status(job).await.unwrap().unwrap();
            assert_eq!(
                (status.frontier, status.in_flight, status.done),
                (0, 0, true)
            );
        }
        let status = store.status(jobs[0]).await.unwrap().unwrap();
        assert_eq!((status.processed, status.unsuccessful), (40, 3));
        assert!(follow.wait().await.success(), "{}", follow.errors());
        assert!(follow.output().ends_with("done: true\n"));
        let mut actual = HashMap::new();
        for (method, path) in site.requests() {
            assert_eq!(method, "GET");
            *actual.entry(path).or_insert(0usize) += 1;
        }
        let mut expected = HashMap::new();
        for path in paths.iter().map(String::as_str).chain([
            "/site/",
            "/site/shared.html",
            "/site/photo.JPG",
            "/site/photo.jpeg",
            "/site/bundle.ZIP",
            "/site/manual.pdf",
            "/site/styles.CSS",
            "/site/fake.html",
            "/site/html.PDF",
            "/site/intro",
            "/site/query?id=1",
            "/site/query?id=2",
            "/site/redirect",
            "/site/missing",
            "/site/denied",
            "/site/server-error",
        ]) {
            expected.insert(path.to_owned(), 1usize);
        }
        expected.insert("/site/p0.html".to_owned(), 2);
        expected.insert("/site/html.PDF".to_owned(), 2);
        assert_eq!(actual, expected, "Repeated or out-of-scope HTTP requests");
        assert!(foreign.requests().is_empty(), "Crawled another origin");
        let duplicate = test.cli(&["submit", site.base.as_str()]);
        assert!(duplicate.status.success());
        assert_eq!(
            job_ids(&String::from_utf8(duplicate.stdout).unwrap()),
            vec![jobs[0]]
        );
        assert_eq!(store.status(jobs[0]).await.unwrap().unwrap(), status);
        drop(nodes);
        let retained = test.cli(&["stats", &jobs[0].to_string()]);
        assert!(retained.status.success());
        assert_eq!(String::from_utf8(retained.stdout).unwrap(), EXPECTED);
        test.cleanup_cli(&[
            (jobs[0], site.base.as_str()),
            (jobs[1], narrow.as_str()),
            (jobs[2], html_pdf.as_str()),
        ])
        .await;
        test.cleanup().await;
    }
}

#[tokio::test]
async fn two_real_nodes_each_obey_ten_request_limit_across_many_jobs() {
    let _cluster = CLI_CLUSTER.lock().await;
    let gate = Arc::new(Semaphore::new(0));
    let paths: Vec<_> = (0..24).map(|i| format!("/site/job-{i}.html")).collect();
    let site = Site::start(
        paths
            .iter()
            .map(|path| (path.as_str(), html("Word").gated(Arc::clone(&gate))))
            .collect(),
    )
    .await;
    let mut test = TestRedis::new().await;
    let mut store = production_store(&test).await;
    let bases: Vec<_> = paths.iter().map(|path| site.url(path)).collect();
    let mut arguments = vec!["submit"];
    arguments.extend(bases.iter().map(Url::as_str));
    let submitted = test.cli(&arguments);
    assert!(submitted.status.success());
    let jobs = job_ids(&String::from_utf8(submitted.stdout).unwrap());
    assert_eq!(jobs.len(), 24);
    let mut nodes = vec![Process::start(&test.url, &["node", "--workers", "10"])];
    wait_for_blocked(&site, 10, &mut nodes).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        site.blocked_requests(),
        10,
        "First node exceeded its per-node limit"
    );
    nodes.push(Process::start(&test.url, &["node", "--workers", "10"]));
    wait_for_blocked(&site, 20, &mut nodes).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        site.blocked_requests(),
        20,
        "Two nodes exceeded twenty requests"
    );
    let mut in_flight = 0;
    for &job in &jobs {
        in_flight += store.status(job).await.unwrap().unwrap().in_flight;
    }
    assert_eq!(in_flight, 20);
    gate.add_permits(24);
    wait_for_done(&mut store, &jobs, &mut nodes).await;
    assert_eq!(site.peak_blocked_requests(), 20);
    let mut requested: Vec<_> = site.requests().into_iter().map(|(_, path)| path).collect();
    requested.sort();
    let mut expected = paths.clone();
    expected.sort();
    assert_eq!(
        requested, expected,
        "A submitted URL was missed or fetched twice"
    );
    for &job in &jobs {
        let Some(JobStats::Done(stats)) = store.stats(job).await.unwrap() else {
            panic!("Completed job lacks final statistics");
        };
        assert_eq!(
            (stats.num_files, stats.num_exts, stats.total_word_count),
            (1, 1, 1)
        );
        assert_eq!(stats.ext_counts, HashMap::from([("html".to_owned(), 1)]));
    }
    drop(nodes);
    let cleanup: Vec<_> = jobs
        .iter()
        .copied()
        .zip(bases.iter().map(Url::as_str))
        .collect();
    test.cleanup_cli(&cleanup).await;
    test.cleanup().await;
}
