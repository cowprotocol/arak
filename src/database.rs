// TODO:
// - Make `store_event` take multiple inputs that are stored together in the same transaction.
// - Implement this for Postgres in addition to Sqlite. Postgres will likely have a native async backend, so we should have the trait be async and internally `spawn_blocking` for use of non async rusqlite.
// - Think about whether the trait should be Send + Sync and whether methods should take mutable Self.

use anyhow::Result;
use solabi::{abi::EventDescriptor, value::Value};

/// Abstraction over specific SQL like backends.
///
/// Note that the methods are blocking. If you call them from async code, make sure you handle this correctly. With Tokio you could use `spawn_blocking`.
pub trait Database {
    /// Prepare the database to store this event in the future.
    ///
    /// This can lead to a new table being created unless a matching table already exists.
    ///
    /// `name` identifies this event. There is one database table per unique name.
    ///
    /// The table has columns for the event's fields mapped to native SQL types. Additionally, every table has the following columns:
    ///
    /// `block_number` and `log_index` form the primary key.
    /// `address` stores the address from which an event was emitted.
    ///
    /// Errors:
    ///
    /// - A table for `name` already exists with an incompatible event signature.
    fn prepare_event(&mut self, name: &str, event: &EventDescriptor) -> Result<()>;

    /// Store an event in the database.
    ///
    /// Errors:
    ///
    /// - `prepare_event` has not been successfully called with this `name`.
    /// - `fields` do not match the event signature specified in the successful call to `prepare_event` with this `name`.
    fn store_event(
        &mut self,
        name: &str,
        block_number: u64,
        log_index: u64,
        address: &[u8; 20],
        fields: &[Value],
    ) -> Result<()>;
}
