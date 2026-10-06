use redis::aio::MultiplexedConnection;
use url::Url;

#[derive(Clone)]
pub struct Store {
    connection: MultiplexedConnection,
    namespace: String,
}

#[derive(Debug)]
pub struct JobStatus {
    pub base_url: String,
    pub num_files: usize,
    pub processed: usize,
    pub unsuccessful: usize,
    pub frontier: usize,
    pub in_flight: usize,
    pub done: bool,
}

impl Store {
    pub async fn connect(redis_url: &str, namespace: &str) -> redis::RedisResult<Self> {
        let client = redis::Client::open(redis_url)?;
        let connection = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            connection,
            namespace: namespace.to_owned(),
        })
    }

    /// The caller supplies a URL normalized by our URL rules.
    pub async fn submit(&mut self, base: &Url) -> redis::RedisResult<u64> {
        // Reserve an ID before running the script so every key is supplied
        // explicitly. Duplicate submissions may leave harmless gaps in IDs.
        let candidate: u64 = redis::cmd("INCR")
            .arg(self.key("next_job_id"))
            .query_async(&mut self.connection)
            .await?;

        redis::Script::new(include_str!("lua/submit.lua"))
            .key(self.key("submissions"))
            .key(self.job_key(candidate, "meta"))
            .key(self.job_key(candidate, "seen"))
            .key(self.job_key(candidate, "frontier"))
            .key(self.key("active"))
            .arg(candidate)
            .arg(base.as_str())
            .invoke_async(&mut self.connection)
            .await
    }

    pub async fn status(&mut self, job: u64) -> redis::RedisResult<Option<JobStatus>> {
        // Read related fields together, even once workers start changing them.
        type Snapshot = (String, String, usize, usize, usize, usize, usize);
        let snapshot: Option<Snapshot> = redis::Script::new(include_str!("lua/status.lua"))
            .key(self.job_key(job, "meta"))
            .key(self.job_key(job, "frontier"))
            .key(self.job_key(job, "inflight"))
            .invoke_async(&mut self.connection)
            .await?;

        Ok(snapshot.map(
            |(base_url, state, num_files, processed, unsuccessful, frontier, in_flight)| {
                JobStatus {
                    base_url,
                    num_files,
                    processed,
                    unsuccessful,
                    frontier,
                    in_flight,
                    done: state == "done",
                }
            },
        ))
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.namespace)
    }

    fn job_key(&self, job: u64, suffix: &str) -> String {
        self.key(&format!("job:{job}:{suffix}"))
    }
}
