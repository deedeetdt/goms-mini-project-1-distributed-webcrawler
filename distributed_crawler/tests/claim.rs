// This shared fixture also has helpers used only by the CLI tests.
#[allow(dead_code)]
#[path = "support/redis.rs"]
mod support;

use distributed_crawler::store::Store;
use distributed_crawler::url_rules::normalize_url;
use support::TestRedis;

#[tokio::test]
async fn claiming_moves_the_seed_from_waiting_to_in_flight() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();

    assert_eq!(test.store.claim(job).await.unwrap(), Some(base.to_string()));

    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 0);
    assert_eq!(status.in_flight, 1);
    assert_eq!(status.num_files, 0);
    assert_eq!(status.processed, 0);
    assert!(!status.done);
    let in_flight: Vec<String> = redis::cmd("SMEMBERS")
        .arg(test.key(&format!("job:{job}:inflight")))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert_eq!(in_flight, vec![base.to_string()]);
    let state: String = redis::cmd("HGET")
        .arg(test.key(&format!("job:{job}:meta")))
        .arg("state")
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert_eq!(state, "running");
    test.cleanup().await;
}

#[tokio::test]
async fn two_callers_competing_for_one_url_receive_it_only_once() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    let mut other = Store::connect(&test.url, &test.namespace).await.unwrap();

    let (left, right) = tokio::join!(test.store.claim(job), other.claim(job));
    let claimed: Vec<String> = [left.unwrap(), right.unwrap()]
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(claimed, vec![base.to_string()]);

    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 0);
    assert_eq!(status.in_flight, 1);
    assert!(!status.done);
    test.cleanup().await;
}

#[tokio::test]
async fn claims_follow_fifo_discovery_order() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    let first = base.join("a.html").unwrap().to_string();
    let second = base.join("b.html").unwrap().to_string();
    // Simulate discoveries being appended; the finish operation comes later.
    redis::cmd("RPUSH")
        .arg(test.key(&format!("job:{job}:frontier")))
        .arg(&first)
        .arg(&second)
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();
    redis::cmd("SADD")
        .arg(test.key(&format!("job:{job}:seen")))
        .arg(&first)
        .arg(&second)
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();

    for expected in [base.to_string(), first, second] {
        assert_eq!(test.store.claim(job).await.unwrap(), Some(expected));
    }
    assert!(test.store.claim(job).await.unwrap().is_none());
    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 0);
    assert_eq!(status.in_flight, 3);
    test.cleanup().await;
}

#[tokio::test]
async fn an_empty_queue_with_a_request_in_flight_does_not_complete_the_job() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap().unwrap();

    assert!(test.store.claim(job).await.unwrap().is_none());

    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 0);
    assert_eq!(status.in_flight, 1);
    assert!(!status.done);
    let active: bool = redis::cmd("SISMEMBER")
        .arg(test.key("active"))
        .arg(job)
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert!(active);
    test.cleanup().await;
}

#[tokio::test]
async fn an_unknown_job_returns_no_work_and_creates_no_keys() {
    let mut test = TestRedis::new().await;
    assert!(test.store.claim(99).await.unwrap().is_none());
    assert!(test.store.status(99).await.unwrap().is_none());
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(test.key("*"))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert!(keys.is_empty());
    test.cleanup().await;
}

#[tokio::test]
async fn a_completed_job_returns_no_work() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    // Even an unexpected leftover queue entry must not revive a done job.
    redis::cmd("HSET")
        .arg(test.key(&format!("job:{job}:meta")))
        .arg("state")
        .arg("done")
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();

    assert!(test.store.claim(job).await.unwrap().is_none());
    let status = test.store.status(job).await.unwrap().unwrap();
    assert!(status.done);
    assert_eq!(status.frontier, 1);
    assert_eq!(status.in_flight, 0);
    test.cleanup().await;
}

#[tokio::test]
async fn an_empty_queue_without_in_flight_work_is_not_finalized_by_claim() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    redis::cmd("DEL")
        .arg(test.key(&format!("job:{job}:frontier")))
        .query_async::<()>(&mut test.connection)
        .await
        .unwrap();

    assert!(test.store.claim(job).await.unwrap().is_none());
    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 0);
    assert_eq!(status.in_flight, 0);
    assert!(!status.done);
    test.cleanup().await;
}
