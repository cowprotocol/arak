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
    path::Path,
};
use url::Url;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub ethrpc: Url,
    pub database: Url,
    #[serde(default = "init_page_size::default")]
    pub init_page_size: u64,
    #[serde(rename = "event")]
    pub events: Vec<Event>,
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

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let toml = fs::read_to_string(path)?;
        let config = toml::from_str(&toml)?;
        Ok(config)
    }
}

impl Debug for Config {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Config")
            .field("ethrpc", &self.ethrpc.as_str())
            .field("database", &self.database.as_str())
            .field("init_page_size", &self.init_page_size)
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

mod init_page_size {
    pub fn default() -> u64 {
        1000
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
