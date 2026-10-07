#[path = "support/redis.rs"]
mod redis_support;
// This shared fixture also supports delayed and binary HTTP tests.
#[allow(dead_code)]
mod support;

use distributed_crawler::fetch::FetchOutcome;
use distributed_crawler::model::WebStats;
use distributed_crawler::node;
use distributed_crawler::page::PageAnalysis;
use distributed_crawler::store::{JobStats, Store};
use distributed_crawler::url_rules::normalize_url;
use redis_support::TestRedis;
use std::time::Duration;
use support::{Response, Site};
use url::Url;

fn file(extension: &str, words: u64, links: Vec<Url>) -> FetchOutcome {
    FetchOutcome::File {
        extension: extension.to_owned(),
        analysis: PageAnalysis {
            links,
            word_count: words,
        },
    }
}

async fn final_stats(store: &mut Store, job: u64) -> WebStats {
    match store.stats(job).await.unwrap() {
        Some(JobStats::Done(stats)) => stats,
        other => panic!("Expected completed statistics, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_job_has_no_stats_and_creates_no_keys() {
    let mut test = TestRedis::new().await;
    assert!(test.store.stats(99).await.unwrap().is_none());
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(test.key("*"))
        .query_async(&mut test.connection)
        .await
        .unwrap();
    assert!(keys.is_empty());
    test.cleanup().await;
}

#[tokio::test]
async fn stats_withhold_partial_results_until_the_last_url_finishes() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/site/").unwrap();
    let child = base.join("pic.jpg").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    assert!(matches!(
        test.store.stats(job).await.unwrap(),
        Some(JobStats::Running)
    ));
    test.store.claim(job).await.unwrap();
    test.store
        .finish(job, &base, Some(&file("html", 5, vec![child.clone()])))
        .await
        .unwrap();
    assert!(matches!(
        test.store.stats(job).await.unwrap(),
        Some(JobStats::Running)
    ));
    test.store.claim(job).await.unwrap();
    assert!(matches!(
        test.store.stats(job).await.unwrap(),
        Some(JobStats::Running)
    ));
    test.store
        .finish(job, &child, Some(&file("jpg", 0, vec![])))
        .await
        .unwrap();
    let stats = final_stats(&mut test.store, job).await;
    assert_eq!(stats.num_files, 2);
    assert_eq!(stats.num_exts, 2);
    assert_eq!(stats.ext_counts.get("html"), Some(&1));
    assert_eq!(stats.ext_counts.get("jpg"), Some(&1));
    assert_eq!(stats.total_word_count, 5);
    test.cleanup().await;
}

#[tokio::test]
async fn all_broken_completed_job_has_zero_files_extensions_and_words() {
    let mut test = TestRedis::new().await;
    let base = normalize_url("https://example.test/missing/").unwrap();
    let job = test.store.submit(&base).await.unwrap();
    test.store.claim(job).await.unwrap();
    test.store
        .finish(
            job,
            &base,
            Some(&FetchOutcome::Unsuccessful { status: 404 }),
        )
        .await
        .unwrap();
    let stats = final_stats(&mut test.store, job).await;
    assert_eq!(stats.num_files, 0);
    assert_eq!(stats.num_exts, 0);
    assert!(stats.ext_counts.is_empty());
    assert_eq!(stats.total_word_count, 0);
    test.cleanup().await;
}

#[tokio::test]
async fn real_crawl_stats_are_immediate_and_readable_after_the_node_stops() {
    let html = |body| Response::new(200, body).header("Content-Type", "text/html");
    let site = Site::start(vec![
        ("/site/", html("Ant ant<a href='about.html'></a><img src='pic.JPG'><a href='file.pdf'></a><a href='missing'></a><a href='about.html#again'></a>")),
        ("/site/about.html", html("Hello Rust<a href='/site/'></a>")),
        ("/site/pic.JPG", Response::new(200, "image").header("Content-Type", "image/jpeg")),
        ("/site/file.pdf", Response::new(200, "pdf").header("Content-Type", "application/pdf")),
    ]).await;
    let mut test = TestRedis::new().await;
    let job = test.store.submit(&site.base).await.unwrap();
    let worker = tokio::spawn(node::run(test.store.clone(), 2));
    let stats = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(!worker.is_finished());
            if test.store.status(job).await.unwrap().unwrap().done {
                break final_stats(&mut test.store, job).await;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    worker.abort();
    let _ = worker.await;
    assert_eq!(stats.num_files, 4);
    assert_eq!(stats.num_exts, 3);
    assert_eq!(stats.total_word_count, 4);
    assert_eq!(stats.ext_counts.get("html"), Some(&2));
    assert_eq!(stats.ext_counts.get("jpg"), Some(&1));
    assert_eq!(stats.ext_counts.get("pdf"), Some(&1));
    let mut fresh_connection = Store::connect(&test.url, &test.namespace).await.unwrap();
    let later = final_stats(&mut fresh_connection, job).await;
    assert_eq!(later.ext_counts, stats.ext_counts);
    assert_eq!(later.total_word_count, 4);
    for suffix in ["meta", "extensions"] {
        let ttl: i64 = redis::cmd("TTL")
            .arg(test.key(&format!("job:{job}:{suffix}")))
            .query_async(&mut test.connection)
            .await
            .unwrap();
        assert_eq!(ttl, -1, "Completed statistics must not expire");
    }
    test.cleanup().await;
}

#[tokio::test]
async fn cli_prints_final_counts_with_sorted_extensions_and_reuses_completed_jobs() {
    let mut test = TestRedis::new().await;
    let base = normalize_url(&format!("https://example.test/{}/stats/", test.namespace)).unwrap();
    let mut store = Store::connect(&test.url, "crawler").await.unwrap();
    let submitted = test.cli(&["submit", base.as_str()]);
    assert!(submitted.status.success());
    let job: u64 = String::from_utf8(submitted.stdout)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let jpg = base.join("pic.jpg").unwrap();
    let pdf = base.join("doc.pdf").unwrap();
    store.claim(job).await.unwrap();
    store
        .finish(
            job,
            &base,
            Some(&file("html", 2, vec![pdf.clone(), jpg.clone()])),
        )
        .await
        .unwrap();
    for (url, extension) in [(&pdf, "pdf"), (&jpg, "jpg")] {
        assert_eq!(store.claim(job).await.unwrap(), Some(url.to_string()));
        store
            .finish(job, url, Some(&file(extension, 0, vec![])))
            .await
            .unwrap();
    }
    let output = test.cli(&["stats", &job.to_string()]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "files: 3\nextensions: 3\n  html: 1\n  jpg: 1\n  pdf: 1\nwords: 2\n"
    );
    assert_eq!(store.submit(&base).await.unwrap(), job);
    let stats = final_stats(&mut store, job).await;
    assert_eq!(stats.num_files, 3);
    test.cleanup_cli(&[(job, base.as_str())]).await;
    test.cleanup().await;
}

#[tokio::test]
async fn cli_reports_an_unfinished_job_without_printing_partial_numbers() {
    let mut test = TestRedis::new().await;
    let base = normalize_url(&format!(
        "https://example.test/{}/unfinished/",
        test.namespace
    ))
    .unwrap();
    let mut store = Store::connect(&test.url, "crawler").await.unwrap();
    let job = store.submit(&base).await.unwrap();
    store.claim(job).await.unwrap();
    store
        .finish(
            job,
            &base,
            Some(&file("html", 123, vec![base.join("child.html").unwrap()])),
        )
        .await
        .unwrap();
    let output = test.cli(&["stats", &job.to_string()]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("Job {job} is not finished; final statistics are not available yet.\n")
    );
    test.cleanup_cli(&[(job, base.as_str())]).await;
    test.cleanup().await;
}

#[tokio::test]
async fn cli_reports_an_unknown_job_instead_of_empty_statistics() {
    let test = TestRedis::new().await;
    let output = test.cli(&["stats", "18446744073709551615"]);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not exist"));
}
