use crate::{
    database::{self, Database},
    event_visitor::{VisitKind, VisitValue},
};
use anyhow::{anyhow, Context, Result};
use rusqlite::{
    types::{ToSqlOutput, Type as SqlType, Value as SqlValue, ValueRef as SqlValueRef},
    Connection,
};
use solabi::{
    abi::EventDescriptor,
    value::{Value as AbiValue, ValueKind as AbiKind},
};
use std::{collections::HashMap, fmt::Write, path::Path};

pub struct Sqlite {
    connection: Connection,
    /// Invariant: Events in the map have corresponding tables in the database.
    events: HashMap<String, Event_>,
}

/// Columns that every event table has.
/// 1. block number
/// 2. log index
const FIXED_COLUMNS: usize = 2;

struct Event_ {
    descriptor: EventDescriptor,
    insert_statements: Vec<InsertStatement>,
}

/// Prepared statements for inserting into event tables. Tables and columns are ordered by `event_visitor`.
///
/// Parameters:
/// - 1: block number
/// - 2: log index
/// - 3: array index if this is an array table (all tables after the first)
/// - 3 + n: n-th event field/column
struct InsertStatement {
    sql: String,
    /// Number of event fields that map to SQL columns. Does not count FIXED_COLUMNS and array index.
    fields: usize,
}

impl Sqlite {
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            events: Default::default(),
        }
    }

    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self::new(Connection::open(path)?))
    }

    #[cfg(test)]
    /// Create a temporary in memory database for tests.
    pub fn new_for_test() -> Self {
        Self::new(Connection::open_in_memory().unwrap())
    }

    #[cfg(test)]
    /// Access to the connection. Useful for tests.
    pub fn _connection(&self) -> &Connection {
        &self.connection
    }

    /// Sanitize event name so it will work as a SQL table name.
    fn internal_event_name(name: &str) -> String {
        name.chars().filter(|c| c.is_ascii_alphanumeric()).collect()
    }
}

impl Database for Sqlite {
    fn prepare_event(&mut self, name: &str, event: &EventDescriptor) -> Result<()> {
        let name = Self::internal_event_name(name);

        if let Some(existing) = self.events.get(&name) {
            if event != &existing.descriptor {
                return Err(anyhow!(
                    "event {name} already exists with different signature"
                ));
            }
            return Ok(());
        }

        // TODO:
        // - Check whether a matching table already exists. Or as first step whether a table with the expected name exists ignoring the contents.
        // - Maybe have `CHECK` clauses to enforce things like address and integers having expected length.
        // - Array tables (all tables except first) have as primary key the event's primary key (block_number, log_index) and their index

        let tables = event_to_tables(event).context("unsupported event")?;
        let mut sql = String::new();
        writeln!(&mut sql, "BEGIN;").unwrap();
        for (i, table) in tables.iter().enumerate() {
            write!(&mut sql, "CREATE TABLE {name}_{i} (").unwrap();
            write!(
                &mut sql,
                "block_number INTEGER NOT NULL, log_index INTEGER NOT NULL, "
            )
            .unwrap();
            if i != 0 {
                // This is an Array table.
                write!(&mut sql, "index INTEGER NOT NULL, ").unwrap();
            }
            for (i, column) in table.0.iter().enumerate() {
                let type_ = match column.0 {
                    SqlType::Null => unreachable!(),
                    SqlType::Integer => "INTEGER",
                    SqlType::Real => "REAL",
                    SqlType::Text => "TEXT",
                    SqlType::Blob => "BLOB",
                };
                write!(&mut sql, "c{i} {type_}, ").unwrap();
            }
            writeln!(
                &mut sql,
                "PRIMARY KEY(block_number ASC, log_index ASC)) STRICT;"
            )
            .unwrap();
        }
        write!(&mut sql, "COMMIT;").unwrap();
        tracing::debug!("creating table:\n{}", sql);

        let insert_statements: Vec<InsertStatement> = tables
            .iter()
            .enumerate()
            .map(|(i, table)| {
                let is_array = i != 0;
                let mut sql = String::new();
                write!(&mut sql, "INSERT INTO {name}_{i} VALUES(").unwrap();
                for i in 0..table.0.len() + FIXED_COLUMNS + is_array as usize {
                    write!(&mut sql, "?{},", i + 1).unwrap();
                }
                assert_eq!(sql.pop(), Some(','));
                write!(&mut sql, ");").unwrap();
                tracing::debug!("creating insert statement:\n{}", sql);
                InsertStatement {
                    sql,
                    fields: table.0.len(),
                }
            })
            .collect();

        self.connection
            .execute_batch(&sql)
            .context("table creation")?;

        // Check that prepared statements are valid. Unfortunately we can't distinguish the statement being wrong from other Sqlite errors like being unable to access the database file on disk.
        for statement in &insert_statements {
            self.connection
                .prepare_cached(&statement.sql)
                .context("invalid prepared statement")?;
        }

        self.events.insert(
            name,
            Event_ {
                descriptor: event.clone(),
                insert_statements,
            },
        );

        Ok(())
    }

    fn event_block(&mut self, name: &str) -> Result<u64> {
        todo!("fetch the indexed block number for event {name}")
    }

    fn update(&mut self, _: &[database::IndexedBlock], _: &[database::Log]) -> Result<()> {
        // Silence "dead_code" errors.
        let _ = Self::store_event;
        todo!("try to parse fields according to event descriptor")
    }
}

impl Sqlite {
    fn _read_event(_name: &str, _block_number: u64, _log_index: u64) -> Result<Vec<AbiValue>> {
        todo!()
    }

    fn store_event<'a>(
        &mut self,
        name: &str,
        block_number: u64,
        log_index: u64,
        _address: &[u8; 20],
        abi_values: &'a [AbiValue],
    ) -> Result<()> {
        // TODO:
        // - Check that the abi values match the stored EventDescriptor.

        let name = Self::internal_event_name(name);
        let event = self.events.get(&name).context("unknown event")?;

        // Outer vec maps to tables. Inner vec maps to columns.
        let mut sql_values: Vec<Vec<ToSqlOutput<'a>>> = vec![vec![]];
        let mut in_array: bool = false;
        let mut visitor = |value: VisitValue<'a>| {
            let sql_value = match value {
                VisitValue::ArrayStart => {
                    sql_values.push(Vec::new());
                    in_array = true;
                    return;
                }
                VisitValue::ArrayEnd => {
                    in_array = false;
                    return;
                }
                VisitValue::Value(AbiValue::Int(v)) => {
                    ToSqlOutput::Owned(SqlValue::Blob(v.to_be_bytes().to_vec()))
                }
                VisitValue::Value(AbiValue::Uint(v)) => {
                    ToSqlOutput::Owned(SqlValue::Blob(v.to_be_bytes().to_vec()))
                }
                VisitValue::Value(AbiValue::Address(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(&v.0))
                }
                VisitValue::Value(AbiValue::Bool(v)) => {
                    ToSqlOutput::Owned(SqlValue::Integer(*v as i64))
                }
                VisitValue::Value(AbiValue::FixedBytes(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(v))
                }
                VisitValue::Value(AbiValue::Function(v)) => ToSqlOutput::Owned(SqlValue::Blob(
                    v.address
                        .0
                        .iter()
                        .copied()
                        .chain(v.selector.0.iter().copied())
                        .collect(),
                )),
                VisitValue::Value(AbiValue::Bytes(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(v))
                }
                VisitValue::Value(AbiValue::String(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(v.as_bytes()))
                }
                _ => unreachable!(),
            };
            (if in_array {
                <[_]>::last_mut
            } else {
                <[_]>::first_mut
            })(&mut sql_values)
            .unwrap()
            .push(sql_value);
        };
        for value in abi_values {
            crate::event_visitor::visit_value(value, &mut visitor)
        }

        let block_number = ToSqlOutput::Owned(SqlValue::Integer(block_number.try_into().unwrap()));
        let log_index = ToSqlOutput::Owned(SqlValue::Integer(log_index.try_into().unwrap()));

        let transaction = self.connection.transaction().context("transaction")?;
        for (i, (statement, values)) in event.insert_statements.iter().zip(sql_values).enumerate() {
            assert!(values.len() % statement.fields == 0);
            let is_array = i != 0;
            let mut statement_ = transaction
                .prepare_cached(&statement.sql)
                .context("prepare_cached")?;
            for (i, row) in values.chunks_exact(statement.fields).enumerate() {
                let array_index = if is_array {
                    Some(ToSqlOutput::Owned(SqlValue::Integer(i.try_into().unwrap())))
                } else {
                    None
                };
                let params = rusqlite::params_from_iter(
                    std::iter::once(&block_number)
                        .chain(std::iter::once(&log_index))
                        .chain(array_index.as_ref())
                        .chain(row),
                );
                statement_.insert(params).context("insert")?;
            }
        }
        transaction.commit().context("commit")?;

        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Table(Vec<Column>);

#[derive(Debug, Eq, PartialEq)]
struct Column(SqlType);

fn event_to_tables(event: &EventDescriptor) -> Result<Vec<Table>> {
    // TODO:
    // - Handle indexed fields.
    // - Make use of field names and potentially tuple names.

    let values = event.inputs.iter().map(|input| &input.field.kind);

    // Nested dynamic arrays are rare and hard to handle. The recursive visiting code and SQL schema becomes more complicated. Handle this properly later.
    for value in values.clone() {
        if has_nested_dynamic_arrays(value) {
            return Err(anyhow!("nested dynamic arrays"));
        }
    }

    let mut tables = vec![Table(vec![])];
    for value in values {
        map_value(&mut tables, value);
    }

    Ok(tables)
}

fn has_nested_dynamic_arrays(value: &AbiKind) -> bool {
    let mut level: u32 = 0;
    let mut max_level: u32 = 0;
    let mut visitor = |visit: VisitKind| match visit {
        VisitKind::ArrayStart => {
            level += 1;
            max_level = std::cmp::max(max_level, level);
        }
        VisitKind::ArrayEnd => level -= 1,
        VisitKind::Value(_) => (),
    };
    crate::event_visitor::visit_kind(value, &mut visitor);
    max_level > 1
}

fn map_value(tables: &mut Vec<Table>, value: &AbiKind) {
    assert!(!tables.is_empty());
    let mut table_index = 0;
    let mut visitor = move |value: VisitKind| {
        let type_ = match value {
            VisitKind::Value(&AbiKind::Int(_)) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Uint(_)) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Address) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Bool) => SqlType::Integer,
            VisitKind::Value(&AbiKind::FixedBytes(_)) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Function) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Bytes) => SqlType::Blob,
            VisitKind::Value(&AbiKind::String) => SqlType::Blob,
            VisitKind::ArrayStart => {
                table_index = tables.len();
                tables.push(Table(vec![]));
                return;
            }
            VisitKind::ArrayEnd => {
                table_index = 0;
                return;
            }
            _ => unreachable!(),
        };
        tables[table_index].0.push(Column(type_));
    };
    crate::event_visitor::visit_kind(value, &mut visitor);
}

#[cfg(test)]
mod tests {
    use solabi::{
        abi::{EventField, Field},
        ethprim::Address,
        function::{ExternalFunction, Selector},
        value::{BitWidth, ByteLength, FixedBytes, Int, Uint},
    };

    use super::*;

    #[test]
    fn new_for_test() {
        Sqlite::new_for_test();
    }

    fn event_descriptor(values: Vec<AbiKind>) -> EventDescriptor {
        EventDescriptor {
            name: Default::default(),
            inputs: values
                .into_iter()
                .map(|value| EventField {
                    field: Field {
                        name: Default::default(),
                        kind: value,
                        components: Default::default(),
                        internal_type: Default::default(),
                    },
                    indexed: Default::default(),
                })
                .collect(),
            anonymous: Default::default(),
        }
    }

    #[test]
    fn map_value_simple() {
        let values = vec![AbiKind::Bytes, AbiKind::Bool];
        let schema = event_to_tables(&event_descriptor(values)).unwrap();
        let expected = vec![Table(vec![Column(SqlType::Blob), Column(SqlType::Integer)])];
        assert_eq!(schema, expected);
    }

    #[test]
    fn map_value_complex_flat() {
        let values = vec![
            AbiKind::Bool,
            AbiKind::Tuple(vec![AbiKind::Bytes, AbiKind::Bool]),
            AbiKind::Bool,
            AbiKind::FixedArray(2, Box::new(AbiKind::Bytes)),
            AbiKind::Bool,
            AbiKind::Tuple(vec![AbiKind::Tuple(vec![AbiKind::FixedArray(
                2,
                Box::new(AbiKind::Bytes),
            )])]),
            AbiKind::Bool,
            AbiKind::FixedArray(
                2,
                Box::new(AbiKind::FixedArray(2, Box::new(AbiKind::Bytes))),
            ),
        ];
        let schema = event_to_tables(&event_descriptor(values)).unwrap();
        let expected = vec![Table(vec![
            Column(SqlType::Integer),
            // first tuple
            Column(SqlType::Blob),
            Column(SqlType::Integer),
            //
            Column(SqlType::Integer),
            // first fixed array
            Column(SqlType::Blob),
            Column(SqlType::Blob),
            //
            Column(SqlType::Integer),
            // second tuple
            Column(SqlType::Blob),
            Column(SqlType::Blob),
            //
            Column(SqlType::Integer),
            // second fixed array
            Column(SqlType::Blob),
            Column(SqlType::Blob),
            Column(SqlType::Blob),
            Column(SqlType::Blob),
        ])];
        assert_eq!(schema, expected);
    }

    #[test]
    fn map_value_array() {
        let values = vec![
            AbiKind::Bool,
            AbiKind::Array(Box::new(AbiKind::Bytes)),
            AbiKind::Bool,
            AbiKind::Array(Box::new(AbiKind::Bool)),
            AbiKind::Bool,
        ];
        let schema = event_to_tables(&event_descriptor(values)).unwrap();
        let expected = vec![
            Table(vec![
                Column(SqlType::Integer),
                Column(SqlType::Integer),
                Column(SqlType::Integer),
            ]),
            Table(vec![Column(SqlType::Blob)]),
            Table(vec![Column(SqlType::Integer)]),
        ];
        assert_eq!(schema, expected);
    }

    #[test]
    fn full_leaf_types() {
        let mut sqlite = Sqlite::new_for_test();
        let values = vec![
            AbiKind::Int(BitWidth::MIN),
            AbiKind::Uint(BitWidth::MIN),
            AbiKind::Address,
            AbiKind::Bool,
            AbiKind::FixedBytes(ByteLength::MIN),
            AbiKind::Function,
            AbiKind::Bytes,
            AbiKind::String,
        ];
        let event = event_descriptor(values);
        sqlite.prepare_event("event1", &event).unwrap();

        let fields = vec![
            AbiValue::Int(Int::new(8, 1i32.into()).unwrap()),
            AbiValue::Uint(Uint::new(8, 2u32.into()).unwrap()),
            AbiValue::Address(Address([3; 20])),
            AbiValue::Bool(true),
            AbiValue::FixedBytes(FixedBytes::new(&[4, 5]).unwrap()),
            AbiValue::Function(ExternalFunction {
                address: Address([6; 20]),
                selector: Selector([7, 8, 9, 10]),
            }),
            AbiValue::Bytes(vec![11, 12]),
            AbiValue::String("abcd".to_string()),
        ];
        sqlite
            .store_event("event1", 1, 2, &[3; 20], &fields)
            .unwrap();

        let mut statement = sqlite.connection.prepare("SELECT * from event1_0").unwrap();
        let mut rows = statement.query(()).unwrap();
        while let Some(row) = rows.next().unwrap() {
            for i in 0.. {
                let Ok(column) = row.get_ref(i) else { break };
                println!("{:?}", column);
            }
            println!();
        }
    }
}
