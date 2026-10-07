#[allow(dead_code)]
#[path = "support/redis.rs"]
mod support;

use distributed_crawler::fetch::FetchOutcome;
use distributed_crawler::model::WebStats;
use distributed_crawler::page::PageAnalysis;
use distributed_crawler::store::Store;
use distributed_crawler::url_rules::normalize_url;
use url::Url;

use support::TestRedis;

fn file(extension: &str, word_count: u64, links: Vec<Url>) -> FetchOutcome {
    FetchOutcome::File {
        extension: extension.to_owned(),
        analysis: PageAnalysis { links, word_count },
    }
}

// Inspect persisted results directly; final-statistics retrieval comes later.
async fn stored_stats(test: &mut TestRedis, job: u64) -> WebStats {
    let (num_files, total_word_count): (usize, u64) = redis::cmd("HMGET")
        .arg(test.key(&format!("job:{job}:meta")))
        .arg("num_files")
        .arg("total_word_count")
        .query_async(&mut test.connection)
        .await
        .unwrap();
    let ext_counts: std::collections::HashMap<String, usize> = redis::cmd("HGETALL")
        .arg(test.key(&format!("job:{job}:extensions")))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    WebStats {
        num_files,
        num_exts: ext_counts.len(),
        ext_counts,
        total_word_count,
    }
}

#[tokio::test]
async fn finishing_the_last_file_saves_results_before_marking_done() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap().unwrap();

    assert!(
        test.store
            .finish(job, &base, Some(&file("html", 4, vec![])))
            .await
            .unwrap()
    );

    let status = test.store.status(job).await.unwrap().unwrap();
    assert!(status.done);
    assert_eq!(status.frontier, 0);
    assert_eq!(status.in_flight, 0);
    assert_eq!(status.processed, 1);
    assert_eq!(status.unsuccessful, 0);
    let stats = stored_stats(&mut test, job).await;
    assert_eq!(stats.num_files, 1);
    assert_eq!(stats.num_exts, 1);
    assert_eq!(stats.ext_counts.get("html"), Some(&1));
    assert_eq!(stats.total_word_count, 4);
    let active: bool = redis::cmd("SISMEMBER")
        .arg(test.key("active"))
        .arg(job)
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert!(!active);
    assert_eq!(test.store.submit(&base).await.unwrap(), job);
    assert!(test.store.status(job).await.unwrap().unwrap().done);
    test.cleanup().await;
}

#[tokio::test]
async fn a_last_parent_publishes_its_child_before_completion_is_checked() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let child = base.join("pic.JPG").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap().unwrap();

    let result = file("html", 2, vec![child.clone(), child.clone(), base.clone()]);
    test.store.finish(job, &base, Some(&result)).await.unwrap();

    let status = test.store.status(job).await.unwrap().unwrap();
    assert!(!status.done);
    assert_eq!(status.frontier, 1);
    assert_eq!(status.in_flight, 0);
    assert_eq!(
        test.store.claim(job).await.unwrap(),
        Some(child.to_string())
    );
    test.store
        .finish(job, &child, Some(&file("jpg", 0, vec![])))
        .await
        .unwrap();

    assert!(test.store.status(job).await.unwrap().unwrap().done);
    let stats = stored_stats(&mut test, job).await;
    assert_eq!(stats.num_files, 2);
    assert_eq!(stats.num_exts, 2);
    assert_eq!(stats.ext_counts.get("html"), Some(&1));
    assert_eq!(stats.ext_counts.get("jpg"), Some(&1));
    assert_eq!(stats.total_word_count, 2);
    test.cleanup().await;
}

#[tokio::test]
async fn simultaneous_parents_discovering_one_child_enqueue_it_once() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let a = base.join("a.html").unwrap();
    let b = base.join("b.html").unwrap();
    let child = base.join("child.html").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap().unwrap();
    test.store
        .finish(
            job,
            &base,
            Some(&file("html", 1, vec![a.clone(), b.clone()])),
        )
        .await
        .unwrap();
    assert_eq!(test.store.claim(job).await.unwrap(), Some(a.to_string()));
    assert_eq!(test.store.claim(job).await.unwrap(), Some(b.to_string()));
    let mut other = Store::connect(&test.url, &test.namespace).await.unwrap();
    let a_result = file("html", 2, vec![child.clone()]);
    let b_result = file("html", 3, vec![child.clone()]);

    let (left, right) = tokio::join!(
        test.store.finish(job, &a, Some(&a_result)),
        other.finish(job, &b, Some(&b_result)),
    );
    assert!(left.unwrap());
    assert!(right.unwrap());
    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 1);
    assert_eq!(status.in_flight, 0);
    assert!(!status.done);
    assert_eq!(
        test.store.claim(job).await.unwrap(),
        Some(child.to_string())
    );
    assert!(test.store.claim(job).await.unwrap().is_none());
    test.store
        .finish(job, &child, Some(&file("html", 4, vec![])))
        .await
        .unwrap();
    let stats = stored_stats(&mut test, job).await;
    assert_eq!(stats.num_files, 4);
    assert_eq!(stats.ext_counts.get("html"), Some(&4));
    assert_eq!(stats.total_word_count, 10);
    assert!(test.store.status(job).await.unwrap().unwrap().done);
    test.cleanup().await;
}

#[tokio::test]
async fn an_in_flight_parent_can_publish_more_work_after_the_queue_is_empty() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let a = base.join("a.html").unwrap();
    let b = base.join("b.html").unwrap();
    let child = base.join("child.html").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap().unwrap();
    test.store
        .finish(
            job,
            &base,
            Some(&file("html", 1, vec![a.clone(), b.clone()])),
        )
        .await
        .unwrap();
    test.store.claim(job).await.unwrap().unwrap();
    test.store.claim(job).await.unwrap().unwrap();
    test.store
        .finish(job, &a, Some(&file("html", 1, vec![])))
        .await
        .unwrap();

    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 0);
    assert_eq!(status.in_flight, 1);
    assert!(!status.done);

    test.store
        .finish(job, &b, Some(&file("html", 1, vec![child.clone()])))
        .await
        .unwrap();
    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 1);
    assert_eq!(status.in_flight, 0);
    assert!(!status.done);
    assert_eq!(
        test.store.claim(job).await.unwrap(),
        Some(child.to_string())
    );
    test.store
        .finish(job, &child, Some(&file("html", 1, vec![])))
        .await
        .unwrap();
    assert!(test.store.status(job).await.unwrap().unwrap().done);
    assert_eq!(stored_stats(&mut test, job).await.num_files, 4);
    test.cleanup().await;
}

#[tokio::test]
async fn duplicate_finishes_do_not_count_twice_or_publish_late_discoveries() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap().unwrap();
    let mut other = Store::connect(&test.url, &test.namespace).await.unwrap();
    let result = file("html", 7, vec![]);
    let (left, right) = tokio::join!(
        test.store.finish(job, &base, Some(&result)),
        other.finish(job, &base, Some(&result)),
    );
    assert_ne!(left.unwrap(), right.unwrap());
    let late_result = file("html", 99, vec![base.join("late.html").unwrap()]);
    assert!(
        !test
            .store
            .finish(job, &base, Some(&late_result))
            .await
            .unwrap()
    );
    let stats = stored_stats(&mut test, job).await;
    assert_eq!(stats.num_files, 1);
    assert_eq!(stats.total_word_count, 7);
    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.processed, 1);
    assert_eq!(status.frontier, 0);
    assert!(status.done);
    test.cleanup().await;
}

#[tokio::test]
async fn finishing_an_unclaimed_url_does_not_change_the_job() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    let result = file("html", 8, vec![base.join("child.html").unwrap()]);
    assert!(!test.store.finish(job, &base, Some(&result)).await.unwrap());
    let status = test.store.status(job).await.unwrap().unwrap();
    assert_eq!(status.frontier, 1);
    assert_eq!(status.in_flight, 0);
    assert_eq!(status.processed, 0);
    assert!(!status.done);
    assert_eq!(stored_stats(&mut test, job).await.num_files, 0);
    test.cleanup().await;
}

#[tokio::test]
async fn finishing_an_unknown_job_creates_no_state() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    assert!(!test.store.finish(99, &base, None).await.unwrap());
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(test.key("*"))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert!(keys.is_empty());
    test.cleanup().await;
}

#[tokio::test]
async fn http_and_transport_failures_release_work_without_counting_files() {
    let mut test = TestRedis::new().await;
    let broken = FetchOutcome::Unsuccessful { status: 404 };
    for (path, outcome) in [("http", Some(&broken)), ("transport", None)] {
        let base = normalize_url(&format!("https://example.test/{path}/")).unwrap();
        let job = test.store.submit(&base).await.unwrap();
        test.store.claim(job).await.unwrap().unwrap();
        assert!(test.store.finish(job, &base, outcome).await.unwrap());
        let status = test.store.status(job).await.unwrap().unwrap();
        assert!(status.done);
        assert_eq!(status.in_flight, 0);
        assert_eq!(status.processed, 1);
        assert_eq!(status.unsuccessful, 1);
        let stats = stored_stats(&mut test, job).await;
        assert_eq!(stats.num_files, 0);
        assert_eq!(stats.num_exts, 0);
        assert_eq!(stats.total_word_count, 0);
    }
    test.cleanup().await;
}

#[tokio::test]
async fn redirects_queue_targets_without_counting_aliases_or_words() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let target = base.join("target.html").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap().unwrap();
    let redirect = FetchOutcome::Redirect {
        target: Some(target.clone()),
    };
    test.store
        .finish(job, &base, Some(&redirect))
        .await
        .unwrap();
    assert_eq!(stored_stats(&mut test, job).await.num_files, 0);
    assert!(!test.store.status(job).await.unwrap().unwrap().done);
    assert_eq!(
        test.store.claim(job).await.unwrap(),
        Some(target.to_string())
    );
    test.store
        .finish(job, &target, Some(&file("html", 3, vec![])))
        .await
        .unwrap();
    let stats = stored_stats(&mut test, job).await;
    assert_eq!(stats.num_files, 1);
    assert_eq!(stats.total_word_count, 3);
    let status = test.store.status(job).await.unwrap().unwrap();
    assert!(status.done);
    assert_eq!(status.processed, 2);
    assert_eq!(status.unsuccessful, 0);
    test.cleanup().await;
}

#[tokio::test]
async fn redirects_without_targets_and_redirect_cycles_can_complete() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/no-target/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap().unwrap();
    test.store
        .finish(job, &base, Some(&FetchOutcome::Redirect { target: None }))
        .await
        .unwrap();
    assert!(test.store.status(job).await.unwrap().unwrap().done);

    let base = normalize_url("https://example.test/cycle/").unwrap();
    let target = base.join("target.html").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap().unwrap();
    test.store
        .finish(
            job,
            &base,
            Some(&FetchOutcome::Redirect {
                target: Some(target.clone()),
            }),
        )
        .await
        .unwrap();
    test.store.claim(job).await.unwrap().unwrap();
    test.store
        .finish(
            job,
            &target,
            Some(&FetchOutcome::Redirect { target: Some(base) }),
        )
        .await
        .unwrap();
    let status = test.store.status(job).await.unwrap().unwrap();
    assert!(status.done);
    assert_eq!(status.processed, 2);
    assert_eq!(stored_stats(&mut test, job).await.num_files, 0);
    test.cleanup().await;
}

#[tokio::test]
async fn overlapping_jobs_keep_their_statistics_independent() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let shared = base.join("shared.html").unwrap();
    let parent_job = test.store.submit(&base).await.unwrap();
    let child_job = test.store.submit(&shared).await.unwrap();
    test.store.claim(parent_job).await.unwrap().unwrap();
    test.store
        .finish(
            parent_job,
            &base,
            Some(&file("html", 2, vec![shared.clone()])),
        )
        .await
        .unwrap();
    assert_eq!(
        test.store.claim(parent_job).await.unwrap(),
        Some(shared.to_string())
    );
    assert_eq!(
        test.store.claim(child_job).await.unwrap(),
        Some(shared.to_string())
    );
    let result = file("html", 3, vec![]);
    test.store
        .finish(parent_job, &shared, Some(&result))
        .await
        .unwrap();
    test.store
        .finish(child_job, &shared, Some(&result))
        .await
        .unwrap();
    let parent_stats = stored_stats(&mut test, parent_job).await;
    let child_stats = stored_stats(&mut test, child_job).await;
    assert_eq!(parent_stats.num_files, 2);
    assert_eq!(parent_stats.total_word_count, 5);
    assert_eq!(child_stats.num_files, 1);
    assert_eq!(child_stats.total_word_count, 3);
    assert!(test.store.status(parent_job).await.unwrap().unwrap().done);
    assert!(test.store.status(child_job).await.unwrap().unwrap().done);
    test.cleanup().await;
}
