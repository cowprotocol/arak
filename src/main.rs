mod config;
mod database;
mod decoder;
mod sqlite;

use self::{config::Config, decoder::Decoder};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
struct Arguments {
    #[clap(short, long, env = "ARAKCONFIG", default_value = "arak.toml")]
    config: PathBuf,
}

fn main() {
    tracing_subscriber::fmt::init();

    let args = Arguments::parse();
    let config = Config::load(&args.config).expect("failed to load configuration");

    tracing::info!("{config:#?}");

    let decoders = config
        .events
        .iter()
        .map(|event| Decoder::new(&event.signature).expect("unsupported event signature"))
        .collect::<Vec<_>>();
    if let Some(decoder) = decoders.first() {
        tracing::debug!("{:?}", decoder.decode(&Default::default()));
    }
}
