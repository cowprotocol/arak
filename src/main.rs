mod config;
mod database;
mod decoder;
mod sqlite;

use self::{config::Config, decoder::Decoder};
use anyhow::{Context, Result};
use clap::Parser;
use ethrpc::{
    eth,
    types::{BlockTag, Digest, Hydrated},
};
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

    tracing::info!("{config:#?}");

    let eth = ethrpc::http::Client::new(config.ethrpc);
    let decoders = config
        .events
        .into_iter()
        .map(|event| {
            Ok((
                event.name,
                Decoder::new(&event.signature).context("unsupported event signature")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut last_block = eth
        .execute_empty(eth::BlockNumber)
        .await
        .context("failed to get starting block number")?
        - 1;
    loop {
        let block = eth
            .execute(
                eth::GetBlockByNumber,
                (BlockTag::Latest.into(), Hydrated::Yes),
            )
            .await?
            .context("missing latest block data")?;

        if last_block >= block.number {
            continue;
        }
        tracing::info!(?block.number, "new block");
        last_block = block.number;

        let logs = get_block_logs(block.hash);
        for log in &logs {
            for (name, decoder) in &decoders {
                if let Ok(fields) = decoder.decode(log) {
                    tracing::info!(?name, ?fields, "event");
                }
            }
        }
    }
}

fn get_block_logs(_: Digest) -> Vec<solabi::log::Log<'static>> {
    // todo
    vec![]
}
