use toml::{Table, Value};
use {
    anyhow::Result,
    ethrpc::types::{ArrayVec, LogFilterValue},
    serde::Deserialize,
    solabi::{
        abi::EventDescriptor,
        ethprim::{Address, Digest},
    },
    std::{
        fmt::{self, Debug, Formatter},
        fs,
        path::{Path, PathBuf},
        time::Duration,
    },
    url::Url,
};

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub ethrpc: Url,
    pub database: Database,
    #[serde(default = "indexer::default")]
    pub indexer: Indexer,
    #[serde(rename = "event")]
    pub events: Vec<Event>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Database {
    Sqlite { connection: String },
    Postgres { connection: String },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Indexer {
    #[serde(default = "indexer::default_page_size")]
    pub page_size: u64,
    #[serde(default = "indexer::default_poll_interval", with = "duration")]
    pub poll_interval: Duration,
}

#[derive(Debug, Deserialize, Clone)]
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

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum Contract {
    #[serde(with = "contract")]
    All,
    Address(Address),
}

impl Config {
    /// Reads a configuration from the specified path, returning the parsed
    /// configuration and its root path.
    pub fn load(
        path: &Path,
        node_url: Option<String>,
        db_url: Option<String>,
    ) -> Result<(Self, PathBuf)> {
        let toml = manual_override(fs::read_to_string(path)?, node_url, db_url)?;
        let config: Config = toml::from_str(&toml.to_string())?;

        let root = fs::canonicalize(path)?
            .parent()
            .expect("file path without a parent")
            .to_owned();
        Ok((config, root))
    }
}

fn manual_override(
    toml_string: String,
    node_url: Option<String>,
    db_url: Option<String>,
) -> Result<Table> {
    let mut toml_values = toml_string.parse::<Table>()?;
    // Manual overrides from env vars.
    if let Some(ethrpc) = node_url {
        tracing::info!("using env NODE_URL");
        toml_values.insert("ethrpc".to_string(), Value::String(ethrpc));
    }
    if let Some(connection) = db_url {
        tracing::info!("using env DB_URL");
        let mut db_data = Table::new();
        db_data.insert("connection".to_string(), Value::String(connection.clone()));
        let mut db_type = Table::new();
        if connection.contains("file:") {
            db_type.insert("sqlite".to_string(), Value::Table(db_data))
        } else {
            db_type.insert("postgres".to_string(), Value::Table(db_data))
        };
        toml_values.insert("database".to_string(), Value::Table(db_type));
    }
    Ok(toml_values)
}

impl Debug for Config {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Config")
            .field("ethrpc", &self.ethrpc.as_str())
            .field("database", &self.database)
            .field("indexer", &self.indexer)
            .field("event", &self.events)
            .finish()
    }
}

mod signature {
    use {
        serde::{de, Deserialize, Deserializer},
        solabi::abi::EventDescriptor,
        std::borrow::Cow,
    };

    pub fn deserialize<'de, D>(deserializer: D) -> Result<EventDescriptor, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = Cow::<str>::deserialize(deserializer)?;
        EventDescriptor::parse_declaration(s.as_ref()).map_err(de::Error::custom)
    }
}

mod contract {
    use {
        serde::{de, Deserialize, Deserializer},
        std::borrow::Cow,
    };

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
    use {super::Indexer, std::time::Duration};

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
    use {
        serde::{Deserialize, Deserializer},
        std::time::Duration,
    };

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = f64::deserialize(deserializer)?;
        Ok(Duration::from_secs_f64(secs))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manual_override() {
        // Sample contains both ethrpc and database.sqlite config
        let sample_toml = r#"
        ethrpc = "old rpc"
        [database.postgres]
        connection = "old db"
        "#
        .to_string();

        // Manual (Env) Values
        let node_url = Some("new rpc".to_string());
        let db_url = Some("new db".to_string());

        // override both
        let new_toml =
            manual_override(sample_toml.clone(), node_url.clone(), db_url.clone()).unwrap();
        assert_eq!(
            new_toml,
            r#"
            ethrpc = "new rpc"
            [database.postgres]
            connection = "new db"
            "#
            .parse::<Table>()
            .unwrap()
        );

        // only node_url
        let new_toml = manual_override(sample_toml.clone(), node_url.clone(), None).unwrap();
        assert_eq!(
            new_toml,
            r#"
            ethrpc = "new rpc"
            [database.postgres]
            connection = "old db"
            "#
            .parse::<Table>()
            .unwrap()
        );

        // only db_url
        let new_toml = manual_override(sample_toml.clone(), None, db_url.clone()).unwrap();
        assert_eq!(
            new_toml,
            r#"
            ethrpc = "old rpc"
            [database.postgres]
            connection = "new db"
            "#
            .parse::<Table>()
            .unwrap()
        );

        // toml without node or db provided
        assert_eq!(
            manual_override("".to_string(), node_url, db_url).unwrap(),
            r#"
            ethrpc = "new rpc"
            [database.postgres]
            connection = "new db"
            "#
            .parse::<Table>()
            .unwrap()
        );
    }
}
