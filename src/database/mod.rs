// TODO:
// - Implement this for Postgres in addition to Sqlite. Postgres will likely have a native async backend, so we should have the trait be async and internally `spawn_blocking` for use of non async rusqlite.
// - Think about whether the trait should be Send + Sync and whether methods should take mutable Self.

mod event_visitor;
mod sqlite;

pub use self::sqlite::Sqlite;
use anyhow::Result;
use solabi::{abi::EventDescriptor, ethprim::Address, value::Value};

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
///
/// All methods either succeed in full or error without having applied any changes. This is accomplished by using SQL transactions.
pub trait Database {
    /// Prepare the database to store this event in the future.
    ///
    /// The database maps the event to tables and columns with native SQL types. For all event tables the primary key is `(block_number, log_index)` plus an array index for values in dynamic arrays.
    ///
    /// `name` identifies this event. Database tables for this event are prefixed with the name.
    ///
    /// If this is the first time the event has been prepared on this database (the persistent database file, not this instance of the Database trait), then the event's indexed and finalized blocks' (see `event_block`) are set to 0.
    ///
    /// Errors:
    ///
    /// - A table for `name` already exists with an incompatible event signature.
    fn prepare_event(&mut self, name: &str, event: &EventDescriptor) -> Result<()>;

    /// Retrieves the block information for the specified event.
    fn event_block(&mut self, name: &str) -> Result<Block>;

    /// It updates two things:
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
