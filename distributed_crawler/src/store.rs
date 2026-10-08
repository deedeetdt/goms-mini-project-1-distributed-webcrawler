use std::collections::HashMap;

use redis::aio::MultiplexedConnection;
use url::Url;

use crate::fetch::FetchOutcome;
use crate::model::WebStats;

#[derive(Clone)]
pub struct Store {
    connection: MultiplexedConnection,
    namespace: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct JobStatus {
    pub base_url: String,
    pub num_files: usize,
    pub processed: usize,
    pub unsuccessful: usize,
    pub frontier: usize,
    pub in_flight: usize,
    pub done: bool,
}

#[derive(Debug)]
pub enum JobStats {
    Running,
    Done(WebStats),
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

    /// Refresh the active jobs each pass so workers can notice new submissions.
    pub async fn active_jobs(&mut self) -> redis::RedisResult<Vec<u64>> {
        let mut jobs: Vec<u64> = redis::cmd("SMEMBERS")
            .arg(self.key("active"))
            .query_async(&mut self.connection)
            .await?;
        // Redis sets have no order; make rotation predictable by job ID.
        jobs.sort_unstable();
        Ok(jobs)
    }

    /// Move the first waiting URL into flight, without a gap between the steps.
    /// None means no work: an empty queue, unknown job, or completed job.
    pub async fn claim(&mut self, job: u64) -> redis::RedisResult<Option<String>> {
        redis::Script::new(include_str!("lua/claim.lua"))
            .key(self.job_key(job, "meta"))
            .key(self.job_key(job, "frontier"))
            .key(self.job_key(job, "inflight"))
            .invoke_async(&mut self.connection)
            .await
    }

    /// Publish a claimed URL's fetch result and check whether the job is done.
    /// None represents a request/body error; the worker should log it first.
    /// Results come from Fetcher, with normalized discoveries already in scope.
    /// Returns true if applied, false if the URL was not in flight (no changes).
    pub async fn finish(
        &mut self,
        job: u64,
        url: &Url,
        outcome: Option<&FetchOutcome>,
    ) -> redis::RedisResult<bool> {
        let script = redis::Script::new(include_str!("lua/finish.lua"));
        let mut invocation = script.prepare_invoke();
        invocation
            .key(self.job_key(job, "meta"))
            .key(self.job_key(job, "frontier"))
            .key(self.job_key(job, "inflight"))
            .key(self.job_key(job, "seen"))
            .key(self.job_key(job, "extensions"))
            .key(self.key("active"))
            .arg(job)
            .arg(url.as_str());

        match outcome {
            Some(FetchOutcome::File {
                extension,
                analysis,
            }) => {
                invocation
                    .arg("file")
                    .arg(extension)
                    .arg(analysis.word_count);
                for link in &analysis.links {
                    invocation.arg(link.as_str());
                }
            }
            Some(FetchOutcome::Redirect { target }) => {
                invocation.arg("redirect").arg("").arg(0);
                if let Some(target) = target {
                    invocation.arg(target.as_str());
                }
            }
            Some(FetchOutcome::Unsuccessful { .. }) | None => {
                invocation.arg("failed").arg("").arg(0);
            }
        }

        invocation.invoke_async(&mut self.connection).await
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

        let Some((base_url, state, num_files, processed, unsuccessful, frontier, in_flight)) =
            snapshot
        else {
            return Ok(None);
        };

        Ok(Some(JobStatus {
            base_url,
            num_files,
            processed,
            unsuccessful,
            frontier,
            in_flight,
            done: state == "done",
        }))
    }

    /// Read final counts only, together with the completion check in one script.
    /// None means unknown; Running carries no partial numbers.
    pub async fn stats(&mut self, job: u64) -> redis::RedisResult<Option<JobStats>> {
        type Snapshot = (bool, usize, u64, HashMap<String, usize>);
        let snapshot: Option<Snapshot> = redis::Script::new(include_str!("lua/stats.lua"))
            .key(self.job_key(job, "meta"))
            .key(self.job_key(job, "extensions"))
            .invoke_async(&mut self.connection)
            .await?;
        Ok(
            snapshot.map(|(done, num_files, total_word_count, ext_counts)| {
                if done {
                    JobStats::Done(WebStats {
                        num_files,
                        num_exts: ext_counts.len(),
                        ext_counts,
                        total_word_count,
                    })
                } else {
                    JobStats::Running
                }
            }),
        )
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.namespace)
    }

    fn job_key(&self, job: u64, suffix: &str) -> String {
        self.key(&format!("job:{job}:{suffix}"))
    }
}
