#[allow(dead_code)]
#[path = "support/redis.rs"]
mod redis_support;
// This fixture also includes helpers used only by concurrency tests.
#[allow(dead_code)]
mod support;

use distributed_crawler::node;
use distributed_crawler::store::{JobStatus, Store};
use redis_support::TestRedis;
use std::error::Error;
use std::time::Duration;
use support::{Response, Site};
use tokio::task::JoinHandle;

struct RunningNode(JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>);

impl RunningNode {
    fn start(store: Store) -> Self {
        Self(tokio::spawn(node::run(store, 1)))
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn wait_until_done(test: &mut TestRedis, job: u64, node: &RunningNode) -> JobStatus {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(
                !node.0.is_finished(),
                "Node stopped before completing the job"
            );
            let status = test.store.status(job).await.unwrap().unwrap();
            if status.done {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Node did not complete the job")
}

fn html(body: &'static str) -> Response {
    Response::new(200, body).header("Content-Type", "text/html")
}

#[tokio::test]
async fn node_follows_fifo_links_and_saves_counts_without_duplicates() {
    let site = Site::start(vec![
        ("/site/", html("Hello world <a href='a.html'>A</a> <a href='b.html'>B</a> <a href='a.html#again'>Again</a> <a href='/outside.html'>Outside</a> <img src='cover.JPG'>")),
        ("/site/a.html", html("Alpha <a href='/site/'>Home</a> <a href='leaf.html'>Leaf</a>")),
        ("/site/b.html", html("Beta")),
        ("/site/leaf.html", html("Leaf words")),
        ("/site/cover.JPG", Response::new(200, "image").header("Content-Type", "image/jpeg")),
    ]).await;
    let mut test = TestRedis::new().await;
    let job = test.store.submit(&site.base).await.unwrap();
    let node = RunningNode::start(test.store.clone());
    let status = wait_until_done(&mut test, job, &node).await;
    assert_eq!(status.num_files, 5);
    assert_eq!(status.processed, 5);
    assert_eq!(status.unsuccessful, 0);
    assert_eq!(status.frontier, 0);
    assert_eq!(status.in_flight, 0);
    assert_eq!(
        site.requests(),
        [
            "/site/",
            "/site/a.html",
            "/site/b.html",
            "/site/cover.JPG",
            "/site/leaf.html"
        ]
        .map(|path| ("GET".to_owned(), path.to_owned()))
    );
    let words: u64 = redis::cmd("HGET")
        .arg(test.key(&format!("job:{job}:meta")))
        .arg("total_word_count")
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert_eq!(words, 12);
    let extensions: std::collections::HashMap<String, usize> = redis::cmd("HGETALL")
        .arg(test.key(&format!("job:{job}:extensions")))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert_eq!(extensions.get("html"), Some(&4));
    assert_eq!(extensions.get("jpg"), Some(&1));
    assert_eq!(test.store.submit(&site.base).await.unwrap(), job);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        site.requests().len(),
        5,
        "Completed jobs must not be crawled again"
    );
    drop(node);
    test.cleanup().await;
}

#[tokio::test]
async fn node_keeps_polling_for_jobs_submitted_after_it_starts() {
    let site = Site::start(vec![
        ("/site/", html("First")),
        ("/site/later.html", html("Later")),
    ])
    .await;
    let mut test = TestRedis::new().await;
    let node = RunningNode::start(test.store.clone());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!node.0.is_finished());
    let first = test.store.submit(&site.base).await.unwrap();
    assert_eq!(wait_until_done(&mut test, first, &node).await.num_files, 1);
    let later = test.store.submit(&site.url("later.html")).await.unwrap();
    assert_eq!(wait_until_done(&mut test, later, &node).await.num_files, 1);
    assert_eq!(site.requests().len(), 2);
    drop(node);
    test.cleanup().await;
}

#[tokio::test]
async fn node_rotates_between_jobs_instead_of_draining_one_first() {
    let site = Site::start(vec![
        ("/site/", html("<a href='child.html'>Child</a>")),
        ("/site/child.html", html("Child")),
        ("/site/other.html", html("Other")),
    ])
    .await;
    let mut test = TestRedis::new().await;
    let first = test.store.submit(&site.base).await.unwrap();
    let second = test.store.submit(&site.url("other.html")).await.unwrap();
    let node = RunningNode::start(test.store.clone());
    assert_eq!(wait_until_done(&mut test, first, &node).await.num_files, 2);
    assert_eq!(wait_until_done(&mut test, second, &node).await.num_files, 1);
    let paths: Vec<String> = site.requests().into_iter().map(|(_, path)| path).collect();
    assert_eq!(paths, ["/site/", "/site/other.html", "/site/child.html"]);
    drop(node);
    test.cleanup().await;
}

#[tokio::test]
async fn node_finishes_http_and_body_failures_and_follows_queued_redirects() {
    let mut truncated = html("Short");
    truncated.declared_length = Some(100);
    let site = Site::start(vec![
        ("/site/", html("<a href='missing'>Missing</a> <a href='bad.html'>Bad</a> <a href='alias'>Alias</a> <a href='target.html'>Target</a>")),
        ("/site/bad.html", truncated),
        ("/site/alias", Response::new(302, "").header("Location", "target.html")),
        ("/site/target.html", html("Target words")),
    ]).await;
    let mut test = TestRedis::new().await;
    let job = test.store.submit(&site.base).await.unwrap();
    let node = RunningNode::start(test.store.clone());
    let status = wait_until_done(&mut test, job, &node).await;
    assert_eq!(status.num_files, 2);
    assert_eq!(status.unsuccessful, 2);
    assert_eq!(status.processed, 5);
    assert_eq!(status.in_flight, 0);
    assert_eq!(
        site.requests()
            .iter()
            .filter(|(_, path)| path == "/site/target.html")
            .count(),
        1
    );
    drop(node);
    test.cleanup().await;
}

#[test]
fn cli_exposes_the_node_command() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crawl"))
        .args(["node", "--help"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
