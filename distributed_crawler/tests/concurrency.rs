// Each test uses only the fixture helpers it needs.
#[allow(dead_code)]
#[path = "support/redis.rs"]
mod redis_support;
#[allow(dead_code)]
mod support;

use distributed_crawler::node;
use distributed_crawler::store::Store;
use redis_support::TestRedis;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use support::{Response, Site};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

struct RunningNode(JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>);

impl RunningNode {
    fn start(store: Store, workers: u8) -> Self {
        Self(tokio::spawn(node::run(store, workers)))
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn wait_for_requests(site: &Site, count: usize, node: &RunningNode) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(!node.0.is_finished(), "Node exited unexpectedly");
            if site.blocked_requests() >= count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Workers did not start the expected simultaneous requests");
}

async fn wait_for_jobs(test: &mut TestRedis, jobs: &[u64], node: &RunningNode) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(!node.0.is_finished(), "Node exited unexpectedly");
            let mut all_done = true;
            for &job in jobs {
                all_done &= test.store.status(job).await.unwrap().unwrap().done;
            }
            if all_done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Jobs did not complete");
}

#[tokio::test]
async fn two_and_ten_workers_overlap_requests_and_preserve_exact_counts() {
    for workers in [2, 10] {
        let gate = Arc::new(Semaphore::new(0));
        let paths: Vec<String> = (0..24)
            .map(|number| format!("/site/{number}.html"))
            .collect();
        let links: String = paths
            .iter()
            .map(|path| format!("<a href='{path}'></a>"))
            .collect();
        let mut routes = vec![(
            "/site/",
            Response::new(200, links).header("Content-Type", "text/html"),
        )];
        routes.extend(paths.iter().map(|path| {
            (
                path.as_str(),
                Response::new(200, "Child")
                    .header("Content-Type", "text/html")
                    .gated(Arc::clone(&gate)),
            )
        }));
        let site = Site::start(routes).await;
        let mut test = TestRedis::new().await;
        let job = test.store.submit(&site.base).await.unwrap();
        let node = RunningNode::start(test.store.clone(), workers);
        wait_for_requests(&site, usize::from(workers), &node).await;
        let status = test.store.status(job).await.unwrap().unwrap();
        assert_eq!(status.in_flight, usize::from(workers));
        assert!(!status.done, "A held request cannot be declared finished");
        gate.add_permits(24);
        wait_for_jobs(&mut test, &[job], &node).await;
        assert_eq!(site.peak_blocked_requests(), usize::from(workers));
        assert_eq!(site.requests().len(), 25);
        let status = test.store.status(job).await.unwrap().unwrap();
        assert_eq!(status.num_files, 25);
        assert_eq!(status.unsuccessful, 0);
        let words: u64 = redis::cmd("HGET")
            .arg(test.key(&format!("job:{job}:meta")))
            .arg("total_word_count")
            .query_async(&mut test.connection)
            .await
            .unwrap();
        assert_eq!(words, 24);
        let mut urls: Vec<String> = site.requests().into_iter().map(|(_, path)| path).collect();
        urls.sort();
        urls.dedup();
        assert_eq!(urls.len(), 25, "Concurrent workers fetched a URL twice");
        drop(node);
        test.cleanup().await;
    }
}

#[tokio::test]
async fn ten_request_limit_is_shared_across_all_jobs_in_the_node() {
    let gate = Arc::new(Semaphore::new(0));
    let paths: Vec<String> = (0..20)
        .map(|number| format!("/site/{number}.html"))
        .collect();
    let site = Site::start(
        paths
            .iter()
            .map(|path| {
                (
                    path.as_str(),
                    Response::new(200, "Word")
                        .header("Content-Type", "text/html")
                        .gated(Arc::clone(&gate)),
                )
            })
            .collect(),
    )
    .await;
    let mut test = TestRedis::new().await;
    let mut jobs = Vec::new();
    for path in &paths {
        jobs.push(
            test.store
                .submit(&site.base.join(path).unwrap())
                .await
                .unwrap(),
        );
    }
    let node = RunningNode::start(test.store.clone(), 10);
    wait_for_requests(&site, 10, &node).await;
    // Give any incorrectly spawned extra workers time to reach the server.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(site.blocked_requests(), 10);
    let mut in_flight = 0;
    for &job in &jobs {
        in_flight += test.store.status(job).await.unwrap().unwrap().in_flight;
    }
    assert_eq!(in_flight, 10);
    gate.add_permits(20);
    wait_for_jobs(&mut test, &jobs, &node).await;
    assert_eq!(site.peak_blocked_requests(), 10);
    assert_eq!(site.requests().len(), 20);
    for job in jobs {
        assert_eq!(test.store.status(job).await.unwrap().unwrap().num_files, 1);
    }
    drop(node);
    test.cleanup().await;
}

#[tokio::test]
async fn node_rejects_worker_counts_outside_one_to_ten() {
    let mut test = TestRedis::new().await;
    for workers in [0, 11] {
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            node::run(test.store.clone(), workers),
        )
        .await
        .expect("Invalid worker count started a node");
        assert!(result.unwrap_err().to_string().contains("1 to 10"));
    }
    test.cleanup().await;
}

#[test]
fn cli_offers_ten_workers_by_default_and_accepts_the_worker_option() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crawl"))
        .args(["node", "--workers", "2", "--help"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--workers"));
    assert!(help.contains("default: 10"));
    for workers in ["0", "11"] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_crawl"))
            .args(["node", "--workers", workers])
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(2),
            "Invalid count must be rejected by argument parsing"
        );
    }
}

#[tokio::test]
async fn a_worker_error_is_reported_and_cancels_the_other_workers() {
    let gate = Arc::new(Semaphore::new(0));
    let site = Site::start(vec![
        (
            "/site/a.html",
            Response::new(200, "A")
                .header("Content-Type", "text/html")
                .gated(Arc::clone(&gate)),
        ),
        (
            "/site/b.html",
            Response::new(200, "B")
                .header("Content-Type", "text/html")
                .gated(Arc::clone(&gate)),
        ),
    ])
    .await;
    let mut test = TestRedis::new().await;
    let a = test.store.submit(&site.url("a.html")).await.unwrap();
    let b = test.store.submit(&site.url("b.html")).await.unwrap();
    let mut node = RunningNode::start(test.store.clone(), 2);
    wait_for_requests(&site, 2, &node).await;
    // Corrupt only this test's metadata to force one worker to return a Redis error.
    let bad = test.store.submit(&site.url("bad.html")).await.unwrap();
    redis::cmd("SET")
        .arg(test.key(&format!("job:{bad}:meta")))
        .arg("not a hash")
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();
    gate.add_permits(1);
    let error = tokio::time::timeout(Duration::from_secs(5), &mut node.0)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("WRONGTYPE"));
    // The node supervisor must cancel the request held by its other worker.
    gate.add_permits(1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let a = test.store.status(a).await.unwrap().unwrap();
    let b = test.store.status(b).await.unwrap().unwrap();
    assert_eq!(a.processed + b.processed, 1);
    assert_eq!(a.in_flight + b.in_flight, 1);
    test.cleanup().await;
}
