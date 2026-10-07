#[allow(dead_code)]
#[path = "support/process.rs"]
mod process_support;
#[allow(dead_code)]
#[path = "support/redis.rs"]
mod redis_support;

use distributed_crawler::store::Store;
use distributed_crawler::url_rules::normalize_url;
use process_support::Process;
use redis_support::TestRedis;
use std::time::Duration;

#[test]
fn status_help_offers_short_and_long_follow_options() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crawl"))
        .args(["status", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("-f, --follow"), "{help}");
}

#[tokio::test]
async fn follow_prints_changes_withholds_identical_snapshots_and_exits_when_done() {
    let mut test = TestRedis::new().await;
    let base = normalize_url(&format!("https://example.test/{}/follow/", test.namespace)).unwrap();
    let mut store = Store::connect(&test.url, "crawler").await.unwrap();
    let job = store.submit(&base).await.unwrap();
    let mut follow = Process::start(&test.url, &["status", "-f", &job.to_string()]);
    follow.wait_for_output("done: false").await;
    assert!(follow.output().contains("frontier: 1\nin-flight: 0"));
    // Allow another poll while the state is unchanged: it must not repeat output.
    tokio::time::sleep(Duration::from_millis(1250)).await;
    assert!(follow.is_running());
    assert_eq!(follow.output().matches("job:").count(), 1);
    store.claim(job).await.unwrap();
    follow.wait_for_output("frontier: 0\nin-flight: 1").await;
    assert!(follow.is_running(), "Empty frontier does not mean done");
    store.finish(job, &base, None).await.unwrap();
    assert!(follow.wait().await.success(), "{}", follow.errors());
    let output = follow.output();
    assert_eq!(output.matches("job:").count(), 3, "{output}");
    assert!(output.ends_with("unsuccessful: 1\ndone: true\n"));
    test.cleanup_cli(&[(job, base.as_str())]).await;
    test.cleanup().await;
}

#[tokio::test]
async fn long_follow_option_prints_an_already_completed_job_once_and_exits() {
    let mut test = TestRedis::new().await;
    let base = normalize_url(&format!("https://example.test/{}/done/", test.namespace)).unwrap();
    let mut store = Store::connect(&test.url, "crawler").await.unwrap();
    let job = store.submit(&base).await.unwrap();
    store.claim(job).await.unwrap();
    store.finish(job, &base, None).await.unwrap();
    let mut follow = Process::start(&test.url, &["status", "--follow", &job.to_string()]);
    assert!(follow.wait().await.success(), "{}", follow.errors());
    let output = follow.output();
    assert_eq!(output.matches("job:").count(), 1);
    assert!(output.ends_with("done: true\n"));
    test.cleanup_cli(&[(job, base.as_str())]).await;
    test.cleanup().await;
}

#[tokio::test]
async fn ordinary_status_still_prints_once_and_exits_for_a_waiting_job() {
    let mut test = TestRedis::new().await;
    let base = normalize_url(&format!(
        "https://example.test/{}/snapshot/",
        test.namespace
    ))
    .unwrap();
    let mut store = Store::connect(&test.url, "crawler").await.unwrap();
    let job = store.submit(&base).await.unwrap();
    let mut status = Process::start(&test.url, &["status", &job.to_string()]);
    assert!(status.wait().await.success(), "{}", status.errors());
    assert_eq!(status.output().matches("job:").count(), 1);
    assert!(status.output().ends_with("done: false\n"));
    test.cleanup_cli(&[(job, base.as_str())]).await;
    test.cleanup().await;
}

#[tokio::test]
async fn following_an_unknown_job_reports_an_error_and_exits() {
    let test = TestRedis::new().await;
    let mut follow = Process::start(&test.url, &["status", "-f", "18446744073709551615"]);
    assert!(!follow.wait().await.success());
    assert!(follow.output().is_empty());
    assert!(follow.errors().contains("does not exist"));
}

#[tokio::test]
async fn follow_reports_a_redis_error_instead_of_silently_waiting_forever() {
    let mut test = TestRedis::new().await;
    let base = normalize_url(&format!("https://example.test/{}/error/", test.namespace)).unwrap();
    let mut store = Store::connect(&test.url, "crawler").await.unwrap();
    let job = store.submit(&base).await.unwrap();
    let mut follow = Process::start(&test.url, &["status", "-f", &job.to_string()]);
    follow.wait_for_output("done: false").await;
    // Corrupt only our own test job to force the next snapshot read to fail.
    redis::cmd("DEL")
        .arg(format!("crawler:job:{job}:meta"))
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(format!("crawler:job:{job}:meta"))
        .arg("not a hash")
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();
    assert!(!follow.wait().await.success());
    assert!(follow.errors().contains("WRONGTYPE"));
    test.cleanup_cli(&[(job, base.as_str())]).await;
    test.cleanup().await;
}
