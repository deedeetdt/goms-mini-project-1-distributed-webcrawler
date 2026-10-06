use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "crawl", version, about = "Submit and inspect crawler jobs")]
pub struct Cli {
    /// Address of the shared Redis server.
    #[arg(long, global = true, default_value = "redis://127.0.0.1:6379/")]
    pub redis_url: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Store one job per URL and return its ID immediately.
    Submit {
        #[arg(required = true, num_args = 1..)]
        urls: Vec<String>,
    },
    /// Show the job's current progress.
    Status { job: u64 },
}
