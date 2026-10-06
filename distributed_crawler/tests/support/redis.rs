use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use distributed_crawler::store::Store;
use redis::aio::MultiplexedConnection;

static NEXT_NAMESPACE: AtomicU64 = AtomicU64::new(0);

pub struct TestRedis {
    pub url: String,
    pub namespace: String,
    pub store: Store,
    pub connection: MultiplexedConnection,
}

impl TestRedis {
    pub async fn new() -> Self {
        let url = std::env::var("CRAWLER_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_owned());
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let namespace = format!(
            "crawler:test:{}:{timestamp}:{}",
            std::process::id(),
            NEXT_NAMESPACE.fetch_add(1, Ordering::Relaxed)
        );
        let store = Store::connect(&url, &namespace)
            .await
            .expect("Start Redis before these tests, or set CRAWLER_TEST_REDIS_URL");
        let connection = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        Self {
            url,
            namespace,
            store,
            connection,
        }
    }

    pub fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.namespace)
    }

    pub async fn cleanup(&mut self) {
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(self.key("*"))
            .query_async(&mut self.connection)
            .await
            .unwrap();
        if !keys.is_empty() {
            redis::cmd("DEL")
                .arg(keys)
                .query_async::<()>(&mut self.connection)
                .await
                .unwrap();
        }
    }

    pub fn cli(&self, arguments: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_crawl"))
            .args(["--redis-url", &self.url])
            .args(arguments)
            .output()
            .unwrap()
    }

    pub async fn cleanup_cli(&mut self, jobs: &[(u64, &str)]) {
        // Delete only the jobs created by this test, never unrelated Redis data.
        for (id, base) in jobs {
            redis::cmd("HDEL")
                .arg("crawler:submissions")
                .arg(base)
                .query_async::<()>(&mut self.connection)
                .await
                .unwrap();
            redis::cmd("SREM")
                .arg("crawler:active")
                .arg(id)
                .query_async::<()>(&mut self.connection)
                .await
                .unwrap();
            for suffix in ["meta", "seen", "frontier", "inflight", "extensions"] {
                redis::cmd("DEL")
                    .arg(format!("crawler:job:{id}:{suffix}"))
                    .query_async::<()>(&mut self.connection)
                    .await
                    .unwrap();
            }
        }
    }
}
