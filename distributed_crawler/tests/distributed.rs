// Real nodes use the production prefix; keep all process scenarios in one test
// so they cannot consume one another's jobs concurrently.
#[allow(dead_code)]
#[path = "support/process.rs"]
mod process_support;
#[allow(dead_code)]
#[path = "support/redis.rs"]
mod redis_support;
#[allow(dead_code)]
mod support;

use distributed_crawler::store::{JobStats, Store};
use process_support::Process;
use redis_support::TestRedis;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use support::{Response, Site};
use tokio::sync::Semaphore;

const EXPECTED_STATS: &str = "files: 7\nextensions: 3\n  html: 5\n  jpg: 1\n  pdf: 1\nwords: 10\n";

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
    .expect("Expected requests did not reach the fixture");
}

async fn exercise_cluster(
    test: &mut TestRedis,
    site: &Site,
    root_gate: &Semaphore,
    parent_gate: &Semaphore,
    node_count: usize,
    workers: u8,
    overlapping_job: bool,
) -> String {
    let mut store = Store::connect(&test.url, "crawler").await.unwrap();
    // Never let test nodes consume unrelated jobs in an existing test database.
    assert!(
        store.active_jobs().await.unwrap().is_empty(),
        "Process tests need Redis with no other active crawler jobs; use a dedicated test instance"
    );
    let mut nodes: Vec<Process> = (0..node_count)
        .map(|_| Process::start(&test.url, &["node", "--workers", &workers.to_string()]))
        .collect();
    for node in &mut nodes {
        node.wait_for_output("Node started").await;
    }
    let first_request = site.requests().len();
    let other_base = site.url("a.html");
    let mut arguments = vec!["submit", site.base.as_str()];
    if overlapping_job {
        arguments.push(other_base.as_str());
    }
    let submitted = test.cli(&arguments);
    assert!(submitted.status.success());
    let jobs: Vec<u64> = String::from_utf8(submitted.stdout)
        .unwrap()
        .lines()
        .map(|line| line.split_whitespace().next().unwrap().parse().unwrap())
        .collect();
    assert_eq!(jobs.len(), 1 + usize::from(overlapping_job));
    let job = jobs[0];
    let mut follow = Process::start(&test.url, &["status", "-f", &job.to_string()]);
    follow.wait_for_output("done: false").await;

    wait_for_blocked(site, jobs.len(), &mut nodes).await;
    let status = store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 0);
    assert_eq!(status.in_flight, 1);
    assert!(!status.done, "A held parent may still discover children");
    assert!(matches!(
        store.stats(job).await.unwrap(),
        Some(JobStats::Running)
    ));
    let unfinished = test.cli(&["stats", &job.to_string()]);
    assert!(unfinished.status.success());
    assert!(
        String::from_utf8(unfinished.stdout)
            .unwrap()
            .contains("not finished")
    );
    if overlapping_job {
        let other = store.status(jobs[1]).await.unwrap().unwrap();
        assert_eq!(
            other.in_flight, 1,
            "Both independent jobs must make progress"
        );
        assert!(!other.done);
    }
    root_gate.add_permits(1);

    // Each parent discovers the same shared child. Hold their responses so
    // different processes overlap before releasing those discoveries together.
    let parents = 3 + usize::from(overlapping_job);
    let simultaneous = (node_count * usize::from(workers)).min(parents);
    wait_for_blocked(site, simultaneous, &mut nodes).await;
    if node_count > 1 && workers == 1 {
        for node in &nodes {
            assert!(
                node.output().contains(": fetching "),
                "Every node must participate"
            );
        }
    }
    assert!(!store.status(job).await.unwrap().unwrap().done);
    parent_gate.add_permits(parents);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut all_done = true;
            for &id in &jobs {
                all_done &= store.status(id).await.unwrap().unwrap().done;
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
    .expect("Cluster did not finish the jobs");
    // Final stats must be available as soon as status says done.
    let output = test.cli(&["stats", &job.to_string()]);
    assert!(output.status.success());
    let stats = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stats, EXPECTED_STATS);
    let status = store.status(job).await.unwrap().unwrap();
    assert_eq!((status.frontier, status.in_flight), (0, 0));
    assert_eq!((status.processed, status.unsuccessful), (8, 1));
    if overlapping_job {
        let output = test.cli(&["stats", &jobs[1].to_string()]);
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            "files: 1\nextensions: 1\n  html: 1\nwords: 2\n"
        );
    }
    assert!(follow.wait().await.success(), "{}", follow.errors());
    assert!(follow.output().ends_with("done: true\n"));
    // A completed duplicate must reuse its ID without starting more work.
    let duplicate = test.cli(&["submit", site.base.as_str()]);
    assert!(duplicate.status.success());
    assert_eq!(
        String::from_utf8(duplicate.stdout).unwrap(),
        format!("{job} {}\n", site.base)
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    let requests: Vec<_> = site.requests().into_iter().skip(first_request).collect();
    assert_eq!(requests.len(), 8 + usize::from(overlapping_job));
    assert!(
        requests
            .iter()
            .all(|(method, path)| method == "GET" && path.starts_with("/site/"))
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(_, path)| path == "/site/shared.html")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(_, path)| path == "/site/a.html")
            .count(),
        1 + usize::from(overlapping_job)
    );
    for (&id, expected) in jobs.iter().zip([8, 1]) {
        let prefix = format!("job {id}: fetching ");
        let fetched: Vec<_> = nodes
            .iter()
            .flat_map(|node| {
                node.output()
                    .lines()
                    .filter_map(|line| line.strip_prefix(&prefix).map(str::to_owned))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(fetched.len(), expected);
        assert_eq!(
            fetched.iter().collect::<HashSet<_>>().len(),
            expected,
            "Duplicate URL within job {id}"
        );
    }
    drop(nodes); // kill/reap only our own processes, after all their work finished.
    let retained = test.cli(&["stats", &job.to_string()]);
    assert!(retained.status.success());
    assert_eq!(String::from_utf8(retained.stdout).unwrap(), stats);
    let mut cleanup = vec![(job, site.base.as_str())];
    if overlapping_job {
        cleanup.push((jobs[1], other_base.as_str()));
    }
    test.cleanup_cli(&cleanup).await;
    stats
}

#[tokio::test]
async fn separate_node_processes_preserve_stats_deduplication_and_job_isolation() {
    let root_gate = Arc::new(Semaphore::new(0));
    let parent_gate = Arc::new(Semaphore::new(0));
    let html = |text| Response::new(200, text).header("Content-Type", "text/html");
    let parent = || {
        html("Hello Rust<a href='/site/shared.html'></a><a href='/site/shared.html#again'></a>")
            .gated(Arc::clone(&parent_gate))
    };
    let site = Site::start(vec![
        ("/site/", html("Ant ant<a href='a.html'></a><a href='b.html'></a><a href='c.html'></a><img src='pic.JPG'><a href='file.pdf'></a><a href='missing'></a><a href='/outside'></a>").gated(Arc::clone(&root_gate))),
        ("/site/a.html", parent()),
        ("/site/b.html", parent()),
        ("/site/c.html", parent()),
        ("/site/shared.html", html("Shared word<a href='/site/'></a>")),
        ("/site/pic.JPG", Response::new(200, "image").header("Content-Type", "image/jpeg")),
        ("/site/file.pdf", Response::new(200, "pdf").header("Content-Type", "application/pdf")),
        ("/outside", html("Must never be fetched")),
    ]).await;
    let mut test = TestRedis::new().await;
    let mut baseline = None;
    for (nodes, workers, overlapping) in [
        (1, 1, false),
        (1, 10, false),
        (2, 1, false),
        (3, 1, false),
        (2, 2, true),
    ] {
        let stats = exercise_cluster(
            &mut test,
            &site,
            &root_gate,
            &parent_gate,
            nodes,
            workers,
            overlapping,
        )
        .await;
        if let Some(expected) = &baseline {
            assert_eq!(
                &stats, expected,
                "Changing the cluster changed final counts"
            );
        } else {
            baseline = Some(stats);
        }
    }
    test.cleanup().await;
}
