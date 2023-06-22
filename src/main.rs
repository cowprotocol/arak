mod config;
mod database;
mod event_visitor;
mod indexer;
mod sqlite;

use self::{config::Config, indexer::Indexer};
use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
struct Arguments {
    #[clap(short, long, env = "ARAKCONFIG", default_value = "arak.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Arguments::parse();
    let config = Config::load(&args.config).context("failed to load configuration")?;

    let eth = ethrpc::http::Client::new(config.ethrpc);
    let database = sqlite::Sqlite::open(
        &config
            .database
            .to_file_path()
            .ok()
            .context("database must be a file:// URL")?,
    )?;

    Indexer::create(eth, database, config.events)?
        .run(indexer::Run {
            page_size: config.indexer.page_size,
            poll_interval: config.indexer.poll_interval,
        })
        .await?;

    Ok(())
}
