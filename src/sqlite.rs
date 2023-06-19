use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
use solabi::abi::EventDescriptor;

use crate::database::Database;

pub struct Sqlite {
    _connection: Connection,
    events: HashMap<String, EventDescriptor>,
}

impl Sqlite {
    pub fn _new(connection: Connection) -> Self {
        Self {
            _connection: connection,
            events: Default::default(),
        }
    }

    #[cfg(test)]
    /// Create a temporary in memory database for tests.
    pub fn new_for_test() -> Self {
        Self::_new(Connection::open_in_memory().unwrap())
    }

    #[cfg(test)]
    /// Access to the connection. Useful for tests.
    pub fn _connection(&self) -> &Connection {
        &self._connection
    }
}

impl Database for Sqlite {
    fn prepare_event(&mut self, name: &str, event: &EventDescriptor) -> Result<()> {
        if let Some(existing) = self.events.get(name) {
            if event != existing {
                return Err(anyhow!(
                    "event {name} already exists with different signature"
                ));
            }
            return Ok(());
        }
        todo!("check that matching table exists or create it if not")
    }

    fn store_event(
        &mut self,
        name: &str,
        _block_number: u64,
        _log_index: u64,
        _address: &[u8; 20],
        _fields: &[solabi::value::Value],
    ) -> Result<()> {
        let _event = self.events.get(name).context("unprepared event")?;
        todo!("try to parse fields according to event descriptor")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_for_test() {
        Sqlite::new_for_test();
    }
}
