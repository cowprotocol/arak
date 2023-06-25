use anyhow::Result;
use ethrpc::types::{ArrayVec, LogFilterValue};
use serde::Deserialize;
use solabi::{
    abi::EventDescriptor,
    ethprim::{Address, Digest},
};
use std::{
    fmt::{self, Debug, Formatter},
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use url::Url;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub ethrpc: Url,
    pub database: Url,
    #[serde(default = "indexer::default")]
    pub indexer: Indexer,
    #[serde(default, rename = "event")]
    pub events: Vec<Event>,
    #[serde(default, rename = "hook")]
    pub hooks: Vec<Hook>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Indexer {
    #[serde(default = "indexer::default_page_size")]
    pub page_size: u64,
    #[serde(default = "indexer::default_poll_interval", with = "duration")]
    pub poll_interval: Duration,
}

#[derive(Debug, Deserialize)]
pub struct Event {
    pub name: String,
    #[serde(default)]
    pub start: u64,
    pub contract: Contract,
    #[serde(default)]
    pub topics: ArrayVec<LogFilterValue<Digest>, 3>,
    #[serde(with = "signature")]
    pub signature: EventDescriptor,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Contract {
    #[serde(with = "contract")]
    All,
    Address(Address),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase", tag = "on")]
pub enum Hook {
    Block {
        #[serde(flatten)]
        source: HookSource,
    },
    Finalize {
        #[serde(flatten)]
        source: HookSource,
        #[serde(default = "hook::default_init")]
        init: bool,
    },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum HookSource {
    Sql { sql: String },
    File { file: PathBuf },
}

impl Config {
    /// Reads a configuration from the specified path, returning the parsed
    /// configuration and its root path.
    pub fn load(path: &Path) -> Result<(Self, PathBuf)> {
        let toml = fs::read_to_string(path)?;
        let config = toml::from_str(&toml)?;
        let root = fs::canonicalize(path)?
            .parent()
            .expect("file path without a parent")
            .to_owned();
        Ok((config, root))
    }
}

impl Debug for Config {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Config")
            .field("ethrpc", &self.ethrpc.as_str())
            .field("database", &self.database.as_str())
            .field("indexer", &self.indexer)
            .field("event", &self.events)
            .finish()
    }
}

mod signature {
    use serde::{de, Deserialize, Deserializer};
    use solabi::abi::EventDescriptor;
    use std::borrow::Cow;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<EventDescriptor, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = Cow::<str>::deserialize(deserializer)?;
        EventDescriptor::parse_declaration(s.as_ref()).map_err(de::Error::custom)
    }
}

mod contract {
    use serde::{de, Deserialize, Deserializer};
    use std::borrow::Cow;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = Cow::<str>::deserialize(deserializer)?;
        (s == "*")
            .then_some(())
            .ok_or_else(|| de::Error::custom("expected '*' string"))
    }
}

mod indexer {
    use super::Indexer;
    use std::time::Duration;

    pub fn default() -> Indexer {
        Indexer {
            page_size: default_page_size(),
            poll_interval: default_poll_interval(),
        }
    }

    pub fn default_page_size() -> u64 {
        1000
    }

    pub fn default_poll_interval() -> Duration {
        Duration::from_secs_f64(0.1)
    }
}

mod duration {
    use serde::{Deserialize, Deserializer};
    use std::time::Duration;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = f64::deserialize(deserializer)?;
        Ok(Duration::from_secs_f64(secs))
    }
}

mod hook {
    pub fn default_init() -> bool {
        true
    }
}

impl Event {
    #[cfg(test)]
    pub fn for_signature(signature: &str) -> Self {
        let signature = EventDescriptor::parse_declaration(signature).unwrap();
        Self {
            name: signature.name.clone(),
            start: 0,
            contract: Contract::All,
            topics: ArrayVec::new(),
            signature,
        }
    }
}
