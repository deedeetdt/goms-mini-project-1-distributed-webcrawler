use std::error::Error;
use std::time::Duration;
use tokio::task::JoinSet;

use crate::fetch::{FetchOutcome, Fetcher};
use crate::store::Store;
use crate::url_rules::normalize_url;

/// Run a fixed set of workers; each handles one request at a time across all jobs.
pub async fn run(store: Store, workers: u8) -> Result<(), Box<dyn Error + Send + Sync>> {
    if !(1..=10).contains(&workers) {
        return Err("Worker count must be 1 to 10".into());
    }
    let fetcher = Fetcher::new()?;
    let mut tasks = JoinSet::new();
    for _ in 0..workers {
        // Cloning shares the Redis connection and HTTP client's connection pool.
        let store = store.clone();
        let fetcher = fetcher.clone();
        tasks.spawn(async move { worker_loop(store, fetcher).await });
    }
    // Workers run indefinitely. Report the first failure or unexpected exit.
    // Dropping the JoinSet on return aborts the remaining workers.
    match tasks.join_next().await {
        Some(Ok(Err(error))) => Err(error),
        Some(Err(error)) => Err(error.into()),
        _ => Err("Crawler worker stopped unexpectedly".into()),
    }
}

/// Take at most one URL per active job on each pass.
async fn worker_loop(
    mut store: Store,
    fetcher: Fetcher,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    loop {
        let mut processed_a_url = false;
        for job in store.active_jobs().await? {
            let Some(status) = store.status(job).await? else {
                continue;
            };
            if status.done {
                continue;
            }
            let base = normalize_url(&status.base_url)?;
            let Some(claimed) = store.claim(job).await? else {
                // Another node may have claimed the last waiting URL.
                continue;
            };
            let url = normalize_url(&claimed)?;
            println!("job {job}: fetching {url}");
            let outcome = match fetcher.fetch(&base, &url).await {
                Ok(outcome) => {
                    if let FetchOutcome::Unsuccessful { status } = &outcome {
                        eprintln!("job {job}: {url} returned HTTP {status}");
                    }
                    Some(outcome)
                }
                Err(error) => {
                    eprintln!("job {job}: could not fetch {url}: {error}");
                    None
                }
            };
            // Finish failures too, so they cannot leave the job in flight.
            store.finish(job, &url, outcome.as_ref()).await?;
            processed_a_url = true;
        }
        if !processed_a_url {
            // Stay alive for later submissions without busy-looping on Redis.
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}
