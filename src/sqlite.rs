use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use rusqlite::{types::Type, Connection};
use solabi::{
    abi::EventDescriptor,
    value::{Value, ValueKind},
};

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
        // TODO:
        // - Remember to use `STRICT` when creating table.
        // - Maybe have `CHECK` clauses to enforce things like address and integers having expected length.
        // - Unique clause on top level event table but not array tables.
        // - Array tables (all tables except first) have as primary key the event's primary key (block_number, log_index) and their index
        todo!("check that matching table exists or create it if not")
    }

    fn store_event(
        &mut self,
        name: &str,
        _block_number: u64,
        _log_index: u64,
        _address: &[u8; 20],
        _fields: &[Value],
    ) -> Result<()> {
        let _event = self.events.get(name).context("unprepared event")?;
        todo!("try to parse fields according to event descriptor")
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Table(Vec<Column>);

#[derive(Debug, Eq, PartialEq)]
struct Column(Type);

fn _event_to_tables(event: &EventDescriptor) -> Vec<Table> {
    // TODO:
    // - Handle indexed fields.
    // - Make use of field names and potentially tuple names.

    let values: Vec<ValueKind> = event
        .inputs
        .iter()
        .map(|input| input.field.kind.clone())
        .collect();
    map_root_value(&ValueKind::Tuple(values))
}

#[allow(dead_code)]
fn map_root_value(value: &ValueKind) -> Vec<Table> {
    assert!(matches!(value, ValueKind::Tuple(_)));
    let mut tables = vec![Table(vec![])];
    map_value(&mut tables, value);
    tables
}

#[allow(dead_code)]
fn map_value(tables: &mut Vec<Table>, value: &ValueKind) {
    assert!(!tables.is_empty());
    let table_index = tables.len() - 1;
    let mut add_column = |type_: Type| {
        let table = &mut tables[table_index];
        table.0.push(Column(type_));
    };
    match value {
        ValueKind::Int(_) => add_column(Type::Blob),
        ValueKind::Uint(_) => add_column(Type::Blob),
        ValueKind::Address => add_column(Type::Blob),
        ValueKind::Bool => add_column(Type::Integer),
        ValueKind::FixedBytes(_) => add_column(Type::Blob),
        ValueKind::Function => add_column(Type::Blob),
        ValueKind::Bytes => add_column(Type::Blob),
        ValueKind::String => add_column(Type::Blob),
        ValueKind::Tuple(values) => {
            for value in values {
                map_value(tables, value);
            }
        }
        ValueKind::FixedArray(length, value) => {
            for _ in 0..*length {
                map_value(tables, value);
            }
        }
        ValueKind::Array(value) => {
            tables.push(Table(vec![]));
            map_value(tables, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_for_test() {
        Sqlite::new_for_test();
    }

    #[test]
    fn map_value_simple() {
        let value = ValueKind::Tuple(vec![ValueKind::Bytes, ValueKind::Bool]);
        let schema = map_root_value(&value);
        let expected = vec![Table(vec![Column(Type::Blob), Column(Type::Integer)])];
        assert_eq!(schema, expected);
    }

    #[test]
    fn map_value_complex_flat() {
        let value = ValueKind::Tuple(vec![
            ValueKind::Bool,
            ValueKind::Tuple(vec![ValueKind::Bytes, ValueKind::Bool]),
            ValueKind::Bool,
            ValueKind::FixedArray(2, Box::new(ValueKind::Bytes)),
            ValueKind::Bool,
            ValueKind::Tuple(vec![ValueKind::Tuple(vec![ValueKind::FixedArray(
                2,
                Box::new(ValueKind::Bytes),
            )])]),
            ValueKind::Bool,
            ValueKind::FixedArray(
                2,
                Box::new(ValueKind::FixedArray(2, Box::new(ValueKind::Bytes))),
            ),
        ]);
        let schema = map_root_value(&value);
        let expected = vec![Table(vec![
            Column(Type::Integer),
            // first tuple
            Column(Type::Blob),
            Column(Type::Integer),
            //
            Column(Type::Integer),
            // first fixed array
            Column(Type::Blob),
            Column(Type::Blob),
            //
            Column(Type::Integer),
            // second tuple
            Column(Type::Blob),
            Column(Type::Blob),
            //
            Column(Type::Integer),
            // second fixed array
            Column(Type::Blob),
            Column(Type::Blob),
            Column(Type::Blob),
            Column(Type::Blob),
        ])];
        assert_eq!(schema, expected);
    }

    #[test]
    fn map_value_array() {
        let value = ValueKind::Tuple(vec![
            ValueKind::Bool,
            ValueKind::Array(Box::new(ValueKind::Bytes)),
            ValueKind::Array(Box::new(ValueKind::Bool)),
            ValueKind::Array(Box::new(ValueKind::Array(Box::new(ValueKind::Bytes)))),
        ]);
        let schema = map_root_value(&value);
        let expected = vec![
            Table(vec![Column(Type::Integer)]),
            Table(vec![Column(Type::Blob)]),
            Table(vec![Column(Type::Integer)]),
            Table(vec![]),
            Table(vec![Column(Type::Blob)]),
        ];
        assert_eq!(schema, expected);
    }
}
