// TODO:
// - Implement this for Postgres in addition to Sqlite. Postgres will likely have a native async backend, so we should have the trait be async and internally `spawn_blocking` for use of non async rusqlite.
// - Think about whether the trait should be Send + Sync and whether methods should take mutable Self.

use anyhow::Result;
use solabi::{abi::EventDescriptor, ethprim::Address, value::Value};
use std::{
    collections::{hash_map, HashMap},
    iter,
};

/// Block indexing information.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Block {
    pub indexed: u64,
    pub finalized: u64,
}

/// Block indexing information attached to an event.
#[derive(Debug)]
pub struct EventBlock<'a> {
    pub event: &'a str,
    pub block: Block,
}

/// An uncled block. All logs for this block or newer are considered invalid.
#[derive(Debug)]
pub struct Uncle<'a> {
    pub event: &'a str,
    pub number: u64,
}

/// An emitted event log.
#[derive(Debug, Default)]
pub struct Log<'a> {
    pub event: &'a str,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
    pub address: Address,
    pub fields: Vec<Value>,
}

/// Abstraction over specific SQL like backends.
///
/// Note that the methods are blocking. If you call them from async code, make sure you handle this correctly. With Tokio you could use `spawn_blocking`.
pub trait Database {
    /// Prepare the database to store this event in the future.
    ///
    /// The database maps the event to tables and columns with native SQL types. For all event tables the primary key is `(block_number, log_index)` plus an array index for values in dynamic arrays.
    ///
    /// `name` identifies this event. Database tables for this event are prefixed with the name.
    ///
    /// Errors:
    ///
    /// - A table for `name` already exists with an incompatible event signature.
    fn prepare_event(&mut self, name: &str, event: &EventDescriptor) -> Result<()>;

    /// Retrieves the block information for the specified event.
    fn event_block(&mut self, name: &str) -> Result<Option<Block>>;

    /// Updates the storage in a single transaction. It updates two things:
    /// - `blocks` specifies updates to the block information for events; this
    ///   will change the value that is read from `event_block`.
    /// - `logs` specified new logs to append to the database.
    ///
    /// Errors:
    ///
    /// - `prepare_event` has not been successfully called with `event` field
    ///   from one or more of the specified `blocks` or `logs`.
    /// - `fields` do not match the event signature specified in the successful
    ///   call to `prepare_event` with this `event` name for one or more `logs`.
    fn update(&mut self, blocks: &[EventBlock], logs: &[Log]) -> Result<()>;

    /// Removes logs from the specified event's uncled blocks.
    ///
    /// Additionally the last indexed block is set to the uncled block's parent;
    /// this changes the `indexed` field of the result from `event_block` for
    /// the specified events.
    fn remove(&mut self, uncles: &[Uncle]) -> Result<()>;
}

#[derive(Default)]
pub struct Dummy {
    events: HashMap<String, Block>,
}

impl Database for Dummy {
    fn prepare_event(&mut self, name: &str, _: &EventDescriptor) -> Result<()> {
        let hash_map::Entry::Vacant(entry) = self.events.entry(name.to_string()) else {
            anyhow::bail!("duplicate event {name}");
        };
        entry.insert(Block::default());
        Ok(())
    }

    fn event_block(&mut self, event: &str) -> Result<Option<Block>> {
        Ok(self.events.get(event).copied())
    }

    fn update(&mut self, blocks: &[EventBlock], logs: &[Log]) -> Result<()> {
        let events = iter::empty()
            .chain(blocks.iter().map(|block| block.event))
            .chain(logs.iter().map(|log| log.event));
        for event in events {
            anyhow::ensure!(self.events.contains_key(event), "missing event {event}");
        }

        for block in blocks {
            *self.events.get_mut(block.event).unwrap() = block.block;
        }
        for log in logs {
            tracing::info!(?log, "added log");
        }

        Ok(())
    }

    fn remove(&mut self, uncles: &[Uncle]) -> Result<()> {
        let events = uncles.iter().map(|uncle| uncle.event);
        for event in events {
            anyhow::ensure!(self.events.contains_key(event), "missing event {event}");
        }

        for uncle in uncles {
            self.events.get_mut(uncle.event).unwrap().indexed = uncle.number - 1;
            tracing::info!(block = %uncle.number, "remove uncled logs");
        }

        Ok(())
    }
}
