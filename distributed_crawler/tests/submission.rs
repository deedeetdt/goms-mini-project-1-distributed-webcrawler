#[path = "support/redis.rs"]
mod support;

use distributed_crawler::store::Store;
use distributed_crawler::url_rules::normalize_url;
use support::TestRedis;

#[tokio::test]
async fn submitting_creates_one_job_with_a_seen_seed_and_fifo_frontier() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let id = test.store.submit(&base).await.unwrap();
    assert_eq!(id, 1);

    let frontier: Vec<String> = redis::cmd("LRANGE")
        .arg(test.key("job:1:frontier"))
        .arg(0)
        .arg(-1)
        .query_async(&mut test.connection)
        .await
        .unwrap();
    let seen: Vec<String> = redis::cmd("SMEMBERS")
        .arg(test.key("job:1:seen"))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    let active: Vec<u64> = redis::cmd("SMEMBERS")
        .arg(test.key("active"))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert_eq!(frontier, vec![base.to_string()]);
    assert_eq!(seen, frontier);
    assert_eq!(active, vec![id]);
    test.cleanup().await;
}

#[tokio::test]
async fn repeated_normalized_submissions_reuse_the_job_without_another_seed() {
    let mut test = TestRedis::new().await;
    let first = normalize_url("https://EXAMPLE.test:443/site/#first").unwrap();
    let second = normalize_url("https://example.test/site/#second").unwrap();
    let id = test.store.submit(&first).await.unwrap();
    assert_eq!(test.store.submit(&second).await.unwrap(), id);
    let length: usize = redis::cmd("LLEN")
        .arg(test.key(&format!("job:{id}:frontier")))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert_eq!(length, 1);
    test.cleanup().await;
}

#[tokio::test]
async fn simultaneous_submitters_create_only_one_job() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let mut other = Store::connect(&test.url, &test.namespace).await.unwrap();

    let (left, right) = tokio::join!(test.store.submit(&base), other.submit(&base));
    let id = left.unwrap();
    assert_eq!(right.unwrap(), id);
    let jobs: usize = redis::cmd("HLEN")
        .arg(test.key("submissions"))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    let seeds: usize = redis::cmd("LLEN")
        .arg(test.key(&format!("job:{id}:frontier")))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    let active: Vec<u64> = redis::cmd("SMEMBERS")
        .arg(test.key("active"))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert_eq!(jobs, 1);
    assert_eq!(seeds, 1);
    assert_eq!(active, vec![id]);
    test.cleanup().await;
}

#[tokio::test]
async fn different_bases_have_independent_jobs_and_seed_queues() {
    let mut test = TestRedis::new().await;
    let left = normalize_url("https://example.test/site/").unwrap();
    let right = normalize_url("https://example.test/other/").unwrap();
    let left_id = test.store.submit(&left).await.unwrap();
    let right_id = test.store.submit(&right).await.unwrap();
    assert_ne!(left_id, right_id);

    for (id, base) in [(left_id, left), (right_id, right)] {
        let frontier: Vec<String> = redis::cmd("LRANGE")
            .arg(test.key(&format!("job:{id}:frontier")))
            .arg(0)
            .arg(-1)
            .query_async(&mut test.connection)
            .await
            .unwrap();
        assert_eq!(frontier, vec![base.to_string()]);
    }
    test.cleanup().await;
}

#[tokio::test]
async fn a_new_job_reports_one_waiting_url_and_zero_in_flight() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let id = test.store.submit(&base).await.unwrap();
    let status = test.store.status(id).await.unwrap().unwrap();
    assert_eq!(status.base_url, base.as_str());
    assert_eq!(status.num_files, 0);
    assert_eq!(status.processed, 0);
    assert_eq!(status.unsuccessful, 0);
    assert_eq!(status.frontier, 1);
    assert_eq!(status.in_flight, 0);
    assert!(!status.done);
    test.cleanup().await;
}

#[tokio::test]
async fn an_unknown_job_returns_no_status() {
    let mut test = TestRedis::new().await;
    assert!(test.store.status(99).await.unwrap().is_none());
    test.cleanup().await;
}

#[tokio::test]
async fn resubmitting_a_completed_job_preserves_its_results_and_empty_queue() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let id = test.store.submit(&base).await.unwrap();
    // Synthetic completed state: fetching/finishing is a later milestone.
    redis::cmd("HSET")
        .arg(test.key(&format!("job:{id}:meta")))
        .arg("state")
        .arg("done")
        .arg("num_files")
        .arg(7)
        .arg("processed")
        .arg(8)
        .arg("unsuccessful")
        .arg(1)
        .arg("total_word_count")
        .arg(42)
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();
    redis::cmd("DEL")
        .arg(test.key(&format!("job:{id}:frontier")))
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();
    redis::cmd("SREM")
        .arg(test.key("active"))
        .arg(id)
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();

    assert_eq!(test.store.submit(&base).await.unwrap(), id);
    let status = test.store.status(id).await.unwrap().unwrap();
    assert!(status.done);
    assert_eq!(status.num_files, 7);
    assert_eq!(status.processed, 8);
    assert_eq!(status.unsuccessful, 1);
    assert_eq!(status.frontier, 0);
    let words: u64 = redis::cmd("HGET")
        .arg(test.key(&format!("job:{id}:meta")))
        .arg("total_word_count")
        .query_async(&mut test.connection)
        .await
        .unwrap();
    let active: usize = redis::cmd("SCARD")
        .arg(test.key("active"))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert_eq!(words, 42);
    assert_eq!(active, 0);
    test.cleanup().await;
}

#[tokio::test]
async fn cli_submits_multiple_urls_reuses_duplicates_and_reports_status() {
    let mut test = TestRedis::new().await;
    let first = format!("https://example.test/{}/cli/", test.namespace);
    let duplicate = format!("{first}#same");
    let second = format!("https://example.test/{}/cli-other/", test.namespace);
    let output = test.cli(&["submit", &first, &duplicate, &second]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3);
    let first_id = lines[0].split_whitespace().next().unwrap();
    assert_eq!(lines[1].split_whitespace().next().unwrap(), first_id);
    assert_ne!(lines[2].split_whitespace().next().unwrap(), first_id);
    let output = test.cli(&["status", first_id]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("crawled: 0"));
    assert!(stdout.contains("frontier: 1"));
    assert!(stdout.contains("in-flight: 0"));
    assert!(stdout.contains("done: false"));
    assert!(stdout.contains(&format!("base: {first}")));
    let second_id = lines[2].split_whitespace().next().unwrap();
    test.cleanup_cli(&[
        (first_id.parse().unwrap(), &first),
        (second_id.parse().unwrap(), &second),
    ])
    .await;
}

#[tokio::test]
async fn cli_rejects_an_invalid_submission_without_creating_any_of_its_jobs() {
    let mut test = TestRedis::new().await;
    let output = test.cli(&[
        "submit",
        "https://example.test/invalid-batch/",
        "mailto:person@example.test",
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Only HTTP and HTTPS"));
    let id: Option<String> = redis::cmd("HGET")
        .arg("crawler:submissions")
        .arg("https://example.test/invalid-batch/")
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert!(id.is_none());
    test.cleanup().await;
}

#[tokio::test]
async fn cli_reports_unknown_jobs() {
    let test = TestRedis::new().await;
    let output = test.cli(&["status", "18446744073709551615"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not exist"));
}
