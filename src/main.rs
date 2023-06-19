mod config;
mod database;
mod sqlite;

use self::config::Config;
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
}
