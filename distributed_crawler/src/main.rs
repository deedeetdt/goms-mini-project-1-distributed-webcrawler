use std::error::Error;
use std::time::Duration;

use clap::Parser;
use distributed_crawler::cli::{Cli, Command};
use distributed_crawler::node;
use distributed_crawler::store::{JobStats, Store};
use distributed_crawler::url_rules::normalize_url;

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn Error + Send + Sync>> {
    match cli.command {
        Command::Node { workers } => {
            let store = Store::connect(&cli.redis_url, "crawler")
                .await
                .map_err(|error| format!("Could not connect to Redis: {error}"))?;
            println!("Node started with {workers} workers; waiting for jobs.");
            node::run(store, workers).await?;
        }
        Command::Submit { urls } => {
            // Validate every input before creating any jobs in this batch.
            let bases = urls
                .iter()
                .map(|url| normalize_url(url))
                .collect::<Result<Vec<_>, _>>()?;
            let mut store = Store::connect(&cli.redis_url, "crawler")
                .await
                .map_err(|error| format!("Could not connect to Redis: {error}"))?;
            for base in bases {
                let job = store.submit(&base).await?;
                println!("{job} {base}");
            }
        }
        Command::Status { job, follow } => {
            let mut store = Store::connect(&cli.redis_url, "crawler")
                .await
                .map_err(|error| format!("Could not connect to Redis: {error}"))?;
            let mut previous = None;
            loop {
                let status = store
                    .status(job)
                    .await?
                    .ok_or_else(|| format!("Job {job} does not exist"))?;
                if previous.as_ref() != Some(&status) {
                    if previous.is_some() {
                        println!();
                    }
                    println!("job: {job}");
                    println!("base: {}", status.base_url);
                    println!("crawled: {}", status.num_files);
                    println!("frontier: {}", status.frontier);
                    println!("in-flight: {}", status.in_flight);
                    println!("processed: {}", status.processed);
                    println!("unsuccessful: {}", status.unsuccessful);
                    println!("done: {}", status.done);
                }
                if !follow || status.done {
                    break;
                }
                previous = Some(status);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Command::Stats { job } => {
            let mut store = Store::connect(&cli.redis_url, "crawler")
                .await
                .map_err(|error| format!("Could not connect to Redis: {error}"))?;
            match store
                .stats(job)
                .await?
                .ok_or_else(|| format!("Job {job} does not exist"))?
            {
                JobStats::Running => {
                    println!("Job {job} is not finished; final statistics are not available yet.");
                }
                JobStats::Done(stats) => {
                    println!("files: {}", stats.num_files);
                    println!("extensions: {}", stats.num_exts);
                    let mut extensions: Vec<_> = stats.ext_counts.iter().collect();
                    extensions.sort_unstable_by_key(|(extension, _)| *extension);
                    for (extension, count) in extensions {
                        println!("  {extension}: {count}");
                    }
                    println!("words: {}", stats.total_word_count);
                }
            }
        }
    }
    Ok(())
}
