use anyhow::Result;
use serde::Deserialize;
use std::{fs, path::Path};

#[derive(Debug, Deserialize)]
pub struct Config {}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let toml = fs::read_to_string(path)?;
        let config = toml::from_str(&toml)?;
        Ok(config)
    }
}
