use anyhow::Result;
use serde::Deserialize;
use solabi::{abi::EventDescriptor, ethprim::Address};
use std::{
    fmt::{self, Debug, Formatter},
    fs,
    path::Path,
};
use url::Url;

#[derive(Deserialize)]
pub struct Config {
    pub ethrpc: Url,
    #[serde(rename = "event")]
    pub events: Vec<Event>,
}

#[derive(Debug, Deserialize)]
pub struct Event {
    pub name: String,
    pub contract: Contract,
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
