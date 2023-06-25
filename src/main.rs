mod config;
mod database;
mod indexer;

use self::{config::Config, indexer::Indexer};
use anyhow::{Context, Result};
use clap::Parser;
use std::{env, path::PathBuf};

#[derive(Parser)]
struct Arguments {
    #[clap(short, long, env = "ARAKCONFIG", default_value = "arak.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Arguments::parse();
    let (config, root) = Config::load(&args.config).context("failed to load configuration")?;
    env::set_current_dir(root)?;

    let eth = ethrpc::http::Client::new(config.ethrpc);
    let database = database::Sqlite::open(&config.database)?;

    Indexer::create(eth, database, config.events)?
        .run(indexer::Run {
            page_size: config.indexer.page_size,
            poll_interval: config.indexer.poll_interval,
        })
        .await?;

    Ok(())
}
