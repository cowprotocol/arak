use database::Database;

mod config;
mod database;
mod indexer;

use {
    self::{config::Config, indexer::Indexer},
    anyhow::{Context, Result},
    clap::Parser,
    std::{env, path::PathBuf},
};

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

    match &config.database {
        config::Database::Sqlite { url } => {
            run_indexer(&config, database::Sqlite::open(url)?).await?;
        }
        config::Database::Postgres { params } => {
            run_indexer(&config, database::Postgres::connect(params).await?).await?;
        }
    }

    Ok(())
}

async fn run_indexer(config: &Config, db: impl Database) -> Result<()> {
    let eth = ethrpc::http::Client::new(config.ethrpc.clone());

    Indexer::create(eth, db, config.events.clone())?
        .run(indexer::Run {
            page_size: config.indexer.page_size,
            poll_interval: config.indexer.poll_interval,
        })
        .await?;

    Ok(())
}
