use crate::database::{
    self,
    event_to_tables::Table,
    event_visitor::{self, VisitValue},
    Database, Log,
};
use anyhow::{anyhow, Context, Result};
use rusqlite::{
    types::{ToSqlOutput, Type as SqlType, Value as SqlValue, ValueRef as SqlValueRef},
    Connection, OpenFlags, Transaction,
};
use solabi::{
    abi::EventDescriptor,
    value::{Value as AbiValue, ValueKind as AbiKind},
};
use std::{collections::HashMap, env, fmt::Write};
use url::Url;

pub struct Sqlite {
    connection: Connection,
    inner: SqliteInner,
}

impl Sqlite {
    pub fn new(connection: Connection) -> Result<Self> {
        let inner = SqliteInner::new(&connection)?;
        Ok(Self { connection, inner })
    }

    /// Opens a new SQLite database backend for the specified URL. The expected
    /// URL format is `sqlite://[/path[?query]]`. For example:
    ///
    /// - `sqlite://` to open and in-memory connection
    /// - `sqlite:///relative/foo.db` to open the file `relative/foo.db`
    /// - `sqlite:////absolute/foo.db` to open the file `/absolute/foo.db`
    ///
    /// Addionally, query string parameters can be set to configure database
    /// connection options. See <https://www.sqlite.org/uri.html> for supported
    /// query string paramters.
    pub fn open(url: &Url) -> Result<Self> {
        anyhow::ensure!(url.scheme() == "sqlite", "not an sqlite:// URL");
        anyhow::ensure!(
            url.has_authority() && url.authority() == "",
            "sqlite:// URL requires empty authority"
        );
        anyhow::ensure!(
            url.fragment().is_none(),
            "sqlite:// URL does not support fragments"
        );

        if url.path().is_empty() {
            tracing::debug!("opening in-memory database");
            return Self::new(Connection::open_in_memory()?);
        };

        // SQLite 3 supports connection strings as file:// URLs, convert our
        // `sqlite://` to that.
        let path = env::current_dir()?.join(
            url.path()
                .strip_prefix('/')
                .expect("can-be-a-base URL not prefixed with /"),
        );
        let mut file = Url::from_file_path(path)
            .ok()
            .context("invalid sqlite:// URL file path")?;
        if let Some(query) = url.query() {
            file.set_query(Some(query));
        }

        tracing::debug!("opening database {file}");
        let connection = Connection::open_with_flags(
            file.as_str(),
            OpenFlags::default() | OpenFlags::SQLITE_OPEN_URI,
        )?;

        Self::new(connection)
    }

    #[cfg(test)]
    /// Create a temporary in memory database for tests.
    pub fn new_for_test() -> Self {
        Self::new(Connection::open_in_memory().unwrap()).unwrap()
    }
}

impl Database for Sqlite {
    fn prepare_event(&mut self, name: &str, event: &EventDescriptor) -> Result<()> {
        let transaction = self.connection.transaction().context("transaction")?;
        self.inner.prepare_event(&transaction, name, event)?;
        transaction.commit().context("commit")
    }

    fn event_block(&mut self, name: &str) -> Result<database::Block> {
        self.inner.event_block(&self.connection, name)
    }

    fn update(&mut self, blocks: &[database::EventBlock], logs: &[database::Log]) -> Result<()> {
        let transaction = self.connection.transaction().context("transaction")?;
        self.inner.update(&transaction, blocks, logs)?;
        transaction.commit().context("commit")
    }

    fn remove(&mut self, uncles: &[database::Uncle]) -> Result<()> {
        let transaction = self.connection.transaction().context("transaction")?;
        self.inner.remove(&transaction, uncles)?;
        transaction.commit().context("commit")
    }
}

/// Columns that every event table has.
const FIXED_COLUMNS: &str = "block_number INTEGER NOT NULL, log_index INTEGER NOT NULL, transaction_index INTEGER NOT NULL, address BLOB NOT NULL";
const FIXED_COLUMNS_COUNT: usize = 4;
const PRIMARY_KEY: &str = "block_number ASC, log_index ASC";

/// Column for array tables.
const ARRAY_COLUMN: &str = "array_index INTEGER NOT NULL";
const PRIMARY_KEY_ARRAY: &str = "block_number ASC, log_index ASC, array_index ASC";

const CREATE_EVENT_BLOCK_TABLE: &str = "CREATE TABLE IF NOT EXISTS _event_block(event TEXT PRIMARY KEY NOT NULL, indexed INTEGER NOT NULL, finalized INTEGER NOT NULL) STRICT;";
const GET_EVENT_BLOCK: &str = "SELECT indexed, finalized FROM _event_block WHERE event = ?1;";
const NEW_EVENT_BLOCK: &str =
    "INSERT INTO _event_block (event, indexed, finalized) VALUES(?1, 0, 0) ON CONFLICT(event) DO NOTHING;";
const SET_EVENT_BLOCK: &str =
    "UPDATE _event_block SET indexed = ?2, finalized = ?3 WHERE event = ?1;";
const SET_INDEXED_BLOCK: &str = "UPDATE _event_block SET indexed = ?2 WHERE event = ?1";

const TABLE_EXISTS: &str =
    "SELECT COUNT(*) > 0 FROM sqlite_schema WHERE type = 'table' AND name = ?1";

// Separate type because of lifetime issues when creating transactions. Outer struct only stores the connection itself.
struct SqliteInner {
    /// Invariant: Events in the map have corresponding tables in the database.
    ///
    /// The key is the `name` argument when the event was passed into `prepare_event`.
    events: HashMap<String, PreparedEvent>,
}

/// An event is represented in the database in several tables.
///
/// All tables have some columns that are unrelated to the event's fields. See `FIXED_COLUMNS`. The first table contains all fields that exist once per event which means they do not show up in arrays. The other tables contain fields that are part of arrays. Those tables additionally have the column `ARRAY_COLUMN`.
///
/// The order of tables and fields is given by the `event_visitor` module.
struct PreparedEvent {
    descriptor: EventDescriptor,
    insert_statements: Vec<InsertStatement>,
    /// Prepared statements for removing rows starting at some block number.
    /// Every statement takes a block number as parameter.
    remove_statements: Vec<String>,
}

/// Parameters:
/// - 1: block number
/// - 2: log index
/// - 3: array index if this is an array table (all tables after the first)
/// - 3 + n: n-th event field/column
#[derive(Debug)]
struct InsertStatement {
    sql: String,
    /// Number of event fields that map to SQL columns. Does not count FIXED_COLUMNS and array index.
    fields: usize,
}

impl SqliteInner {
    fn new(connection: &Connection) -> Result<Self> {
        connection
            .execute(CREATE_EVENT_BLOCK_TABLE, ())
            .context("create event_block table")?;

        connection
            .prepare_cached(GET_EVENT_BLOCK)
            .context("prepare get_event_block")?;
        connection
            .prepare_cached(SET_EVENT_BLOCK)
            .context("prepare set_event_block")?;
        connection
            .prepare_cached(SET_INDEXED_BLOCK)
            .context("prepare set_indexed_block")?;
        connection
            .prepare_cached(TABLE_EXISTS)
            .context("prepare table_exists")?;

        Ok(Self {
            events: Default::default(),
        })
    }

    /*
        fn read_event(
            &self,
            c: &Connection,
            name: &str,
            block_number: u64,
            log_index: u64,
        ) -> Result<Vec<AbiValue>> {
            let name = Self::internal_event_name(name);
            let event = self.events.get(&name).context("unknown event")?;

            todo!()
        }
    */

    fn event_block(&self, con: &Connection, name: &str) -> Result<database::Block> {
        let mut statement = con
            .prepare_cached(GET_EVENT_BLOCK)
            .context("prepare_cached")?;
        let block: (i64, i64) = statement
            .query_row((name,), |row| Ok((row.get(0)?, row.get(1)?)))
            .context("query_row")?;
        Ok(database::Block {
            indexed: block.0.try_into().context("indexed out of bounds")?,
            finalized: block.1.try_into().context("finalized out of bounds")?,
        })
    }

    fn set_event_blocks(&self, con: &Transaction, blocks: &[database::EventBlock]) -> Result<()> {
        let mut statement = con
            .prepare_cached(SET_EVENT_BLOCK)
            .context("prepare_cached")?;
        for block in blocks {
            if !self.events.contains_key(block.event) {
                return Err(anyhow!("event {} wasn't prepared", block.event));
            }
            let indexed: i64 = block
                .block
                .indexed
                .try_into()
                .context("indexed out of bounds")?;
            let finalized: i64 = block
                .block
                .finalized
                .try_into()
                .context("finalized out of bounds")?;
            let rows = statement
                .execute((block.event, indexed, finalized))
                .context("execute")?;
            if rows != 1 {
                return Err(anyhow!(
                    "query unexpectedly changed {rows} rows instead of 1"
                ));
            }
        }
        Ok(())
    }

    fn prepare_event(
        &mut self,
        con: &Transaction,
        name: &str,
        event: &EventDescriptor,
    ) -> Result<()> {
        // TODO:
        // - Check that either no table exists or all tables exist and with the right types.
        // - Maybe have `CHECK` clauses to enforce things like address and integers having expected length.
        // - Maybe store serialized event descriptor in the database so we can load and check it.

        if let Some(existing) = self.events.get(name) {
            if event != &existing.descriptor {
                return Err(anyhow!(
                    "event {} (database name {name}) already exists with different signature",
                    event.name
                ));
            }
            return Ok(());
        }

        let tables =
            database::event_to_tables::event_to_tables(name, event).context("unsupported event")?;
        let name = &tables.primary.name;

        let create_table = |is_array: bool, table: &Table| {
            let mut sql = String::new();
            write!(&mut sql, "CREATE TABLE IF NOT EXISTS {} (", table.name).unwrap();
            write!(&mut sql, "{FIXED_COLUMNS}, ").unwrap();
            if is_array {
                write!(&mut sql, "{ARRAY_COLUMN}, ").unwrap();
            }
            for column in table.columns.iter() {
                write!(&mut sql, "{}", column.name).unwrap();
                let type_ = match abi_kind_to_sql_type(column.kind).unwrap() {
                    SqlType::Null => unreachable!(),
                    SqlType::Integer => "INTEGER",
                    SqlType::Real => "REAL",
                    SqlType::Text => "TEXT",
                    SqlType::Blob => "BLOB",
                };
                write!(&mut sql, " {type_}, ").unwrap();
            }
            let primary_key = if is_array {
                PRIMARY_KEY_ARRAY
            } else {
                PRIMARY_KEY
            };
            write!(&mut sql, "PRIMARY KEY({primary_key})) STRICT;").unwrap();
            tracing::debug!("creating table:\n{}", sql);
            con.execute(&sql, ()).context("execute create_table")
        };
        create_table(false, &tables.primary)?;
        for table in &tables.dynamic_arrays {
            create_table(true, table)?;
        }

        let mut new_event_block = con
            .prepare_cached(NEW_EVENT_BLOCK)
            .context("prepare new_event_block")?;
        new_event_block
            .execute((&name,))
            .context("execute new_event_block")?;

        let insert_statements: Vec<InsertStatement> = std::iter::once((false, &tables.primary))
            .chain(std::iter::repeat(true).zip(&tables.dynamic_arrays))
            .clone()
            .map(|(is_array, table)| {
                let mut sql = String::new();
                write!(&mut sql, "INSERT INTO {} VALUES(", table.name).unwrap();
                for i in 0..table.columns.len() + FIXED_COLUMNS_COUNT + is_array as usize {
                    write!(&mut sql, "?{},", i + 1).unwrap();
                }
                assert_eq!(sql.pop(), Some(','));
                write!(&mut sql, ");").unwrap();
                tracing::debug!("creating insert statement:\n{}", sql);
                InsertStatement {
                    sql,
                    fields: table.columns.len(),
                }
            })
            .collect();

        let remove_statements: Vec<String> = std::iter::once(&tables.primary)
            .chain(&tables.dynamic_arrays)
            .map(|table| format!("DELETE FROM {} WHERE block_number >= ?1;", table.name))
            .collect();

        // Check that prepared statements are valid. Unfortunately we can't distinguish the statement being wrong from other Sqlite errors like being unable to access the database file on disk.
        for statement in &insert_statements {
            con.prepare_cached(&statement.sql)
                .context("invalid prepared insert statement")?;
        }
        for statement in &remove_statements {
            con.prepare_cached(statement)
                .context("invalid prepared remove statement")?;
        }

        self.events.insert(
            name.clone(),
            PreparedEvent {
                descriptor: event.clone(),
                insert_statements,
                remove_statements,
            },
        );

        Ok(())
    }

    fn store_event<'a>(
        &self,
        con: &Transaction,
        Log {
            event,
            block_number,
            log_index,
            transaction_index,
            address,
            fields,
        }: &'a Log,
    ) -> Result<()> {
        let event = self.events.get(*event).context("unknown event")?;

        let len = fields.len();
        let expected_len = event.descriptor.inputs.len();
        if fields.len() != expected_len {
            return Err(anyhow!(
                "event value has {len} fields but should have {expected_len}"
            ));
        }
        for (i, (value, kind)) in fields.iter().zip(&event.descriptor.inputs).enumerate() {
            if value.kind() != kind.field.kind {
                return Err(anyhow!("event field {i} doesn't match event descriptor"));
            }
        }

        // Outer vec maps to tables. Inner vec maps to (array element count, columns).
        let mut sql_values: Vec<(Option<usize>, Vec<ToSqlOutput<'a>>)> = vec![(None, vec![])];
        let mut in_array: bool = false;
        let mut visitor = |value: VisitValue<'a>| {
            let sql_value = match value {
                VisitValue::ArrayStart(len) => {
                    sql_values.push((Some(len), Vec::new()));
                    in_array = true;
                    return;
                }
                VisitValue::ArrayEnd => {
                    in_array = false;
                    return;
                }
                VisitValue::Value(AbiValue::Int(v)) => {
                    ToSqlOutput::Owned(SqlValue::Blob(v.get().to_be_bytes().to_vec()))
                }
                VisitValue::Value(AbiValue::Uint(v)) => {
                    ToSqlOutput::Owned(SqlValue::Blob(v.get().to_be_bytes().to_vec()))
                }
                VisitValue::Value(AbiValue::Address(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(&v.0))
                }
                VisitValue::Value(AbiValue::Bool(v)) => {
                    ToSqlOutput::Owned(SqlValue::Integer(*v as i64))
                }
                VisitValue::Value(AbiValue::FixedBytes(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(v.as_bytes()))
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
            .1
            .push(sql_value);
        };
        for value in fields {
            event_visitor::visit_value(value, &mut visitor)
        }

        let block_number =
            ToSqlOutput::Owned(SqlValue::Integer((*block_number).try_into().unwrap()));
        let log_index = ToSqlOutput::Owned(SqlValue::Integer((*log_index).try_into().unwrap()));
        let transaction_index =
            ToSqlOutput::Owned(SqlValue::Integer((*transaction_index).try_into().unwrap()));
        let address = ToSqlOutput::Borrowed(SqlValueRef::Blob(&address.0));
        for (statement, (array_element_count, values)) in
            event.insert_statements.iter().zip(sql_values)
        {
            let mut statement_ = con
                .prepare_cached(&statement.sql)
                .context("prepare_cached")?;
            let is_array = array_element_count.is_some();
            let array_element_count = array_element_count.unwrap_or(1);
            assert_eq!(statement.fields * array_element_count, values.len());
            for i in 0..array_element_count {
                let row = &values[i * statement.fields..][..statement.fields];
                let array_index = if is_array {
                    Some(ToSqlOutput::Owned(SqlValue::Integer(i.try_into().unwrap())))
                } else {
                    None
                };
                let params = rusqlite::params_from_iter(
                    [&block_number, &log_index, &transaction_index, &address]
                        .into_iter()
                        .chain(array_index.as_ref())
                        .chain(row),
                );
                statement_.insert(params).context("insert")?;
            }
        }

        Ok(())
    }

    fn update(
        &self,
        con: &Transaction,
        blocks: &[database::EventBlock],
        logs: &[database::Log],
    ) -> Result<()> {
        self.set_event_blocks(con, blocks)
            .context("set_event_blocks")?;
        for log in logs {
            self.store_event(con, log).context("store_event")?;
        }
        Ok(())
    }

    fn remove(&self, connection: &Connection, uncles: &[database::Uncle]) -> Result<()> {
        let mut set_indexed_block = connection
            .prepare_cached(SET_INDEXED_BLOCK)
            .context("prepare_cached set_indexed_block")?;
        for uncle in uncles {
            if uncle.number == 0 {
                return Err(anyhow!("block 0 got uncled"));
            }
            let block = i64::try_from(uncle.number).context("block out of bounds")?;
            let parent_block = block - 1;
            let prepared = self.events.get(uncle.event).context("unprepared event")?;
            for remove_statement in &prepared.remove_statements {
                let mut remove_statement = connection
                    .prepare_cached(remove_statement)
                    .context("prepare_cached remove_statement")?;
                remove_statement
                    .execute((block,))
                    .context("execute remove_statement")?;
                set_indexed_block
                    .execute((uncle.event, parent_block))
                    .context("execute set_indexed_block")?;
            }
        }
        Ok(())
    }
}

fn abi_kind_to_sql_type(value: &AbiKind) -> Option<SqlType> {
    match value {
        AbiKind::Int(_) => Some(SqlType::Blob),
        AbiKind::Uint(_) => Some(SqlType::Blob),
        AbiKind::Address => Some(SqlType::Blob),
        AbiKind::Bool => Some(SqlType::Integer),
        AbiKind::FixedBytes(_) => Some(SqlType::Blob),
        AbiKind::Function => Some(SqlType::Blob),
        AbiKind::Bytes => Some(SqlType::Blob),
        AbiKind::String => Some(SqlType::Blob),
        AbiKind::FixedArray(_, _) | AbiKind::Tuple(_) | AbiKind::Array(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use solabi::{
        ethprim::Address,
        function::{ExternalFunction, Selector},
        value::{Array, FixedBytes, Int, Uint},
    };

    use super::*;

    #[test]
    fn new_for_test() {
        Sqlite::new_for_test();
    }

    fn print_table(con: &Connection, table: &str) {
        let mut statement = con.prepare(&format!("SELECT * FROM {table}")).unwrap();
        let mut rows = statement.query(()).unwrap();
        while let Some(row) = rows.next().unwrap() {
            for i in 0..row.as_ref().column_count() {
                let name = row.as_ref().column_name(i).unwrap();
                let value = row.get_ref(i).unwrap();
                println!("{name}: {value:?}");
            }
            println!();
        }
    }

    #[test]
    fn full_leaf_types() {
        let mut sqlite = Sqlite::new_for_test();
        let event = r#"
event Event (
    int256,
    uint256,
    address,
    bool,
    bytes1,
    function,
    bytes,
    string
)
"#;
        let event = EventDescriptor::parse_declaration(event).unwrap();
        sqlite.prepare_event("event", &event).unwrap();

        let fields = vec![
            AbiValue::Int(Int::new(256, 1i32.into()).unwrap()),
            AbiValue::Uint(Uint::new(256, 2u32.into()).unwrap()),
            AbiValue::Address(Address([3; 20])),
            AbiValue::Bool(true),
            AbiValue::FixedBytes(FixedBytes::new(&[4]).unwrap()),
            AbiValue::Function(ExternalFunction {
                address: Address([6; 20]),
                selector: Selector([7, 8, 9, 10]),
            }),
            AbiValue::Bytes(vec![11, 12]),
            AbiValue::String("abcd".to_string()),
        ];
        sqlite
            .update(
                &[],
                &[Log {
                    event: "event",
                    block_number: 1,
                    log_index: 2,
                    transaction_index: 3,
                    address: Address([4; 20]),
                    fields,
                }],
            )
            .unwrap();

        print_table(&sqlite.connection, "event");
    }

    #[test]
    fn with_array() {
        let mut sqlite = Sqlite::new_for_test();
        let event = r#"
event Event (
    (bool, string)[]
)
"#;
        let event = EventDescriptor::parse_declaration(event).unwrap();
        sqlite.prepare_event("event", &event).unwrap();

        let log = Log {
            event: "event",
            block_number: 0,
            fields: vec![AbiValue::Array(
                Array::from_values(vec![
                    AbiValue::Tuple(vec![
                        AbiValue::Bool(false),
                        AbiValue::String("hello".to_string()),
                    ]),
                    AbiValue::Tuple(vec![
                        AbiValue::Bool(true),
                        AbiValue::String("world".to_string()),
                    ]),
                ])
                .unwrap(),
            )],
            ..Default::default()
        };
        sqlite.update(&[], &[log]).unwrap();

        let log = Log {
            event: "event",
            block_number: 1,
            fields: vec![AbiValue::Array(
                Array::new(AbiKind::Tuple(vec![AbiKind::Bool, AbiKind::String]), vec![]).unwrap(),
            )],
            ..Default::default()
        };
        sqlite.update(&[], &[log]).unwrap();

        print_table(&sqlite.connection, "event");
        print_table(&sqlite.connection, "event_array_0");
    }

    #[test]
    fn event_blocks() {
        let mut sqlite = Sqlite::new_for_test();
        let event = EventDescriptor::parse_declaration("event Event()").unwrap();
        sqlite.prepare_event("event", &event).unwrap();
        let result = sqlite.event_block("event").unwrap();
        assert_eq!(result.indexed, 0);
        assert_eq!(result.finalized, 0);
        let blocks = database::EventBlock {
            event: "event",
            block: database::Block {
                indexed: 2,
                finalized: 3,
            },
        };
        sqlite.update(&[blocks], &[]).unwrap();
        let result = sqlite.event_block("event").unwrap();
        assert_eq!(result.indexed, 2);
        assert_eq!(result.finalized, 3);
    }

    #[test]
    fn remove() {
        let mut sqlite = Sqlite::new_for_test();

        let event = EventDescriptor::parse_declaration("event Event()").unwrap();
        sqlite.prepare_event("event", &event).unwrap();
        sqlite.prepare_event("eventAAA", &event).unwrap();
        sqlite
            .update(
                &[],
                &[
                    Log {
                        event: "event",
                        block_number: 1,
                        ..Default::default()
                    },
                    Log {
                        event: "event",
                        block_number: 2,
                        ..Default::default()
                    },
                    Log {
                        event: "event",
                        block_number: 5,
                        ..Default::default()
                    },
                    Log {
                        event: "event",
                        block_number: 6,
                        ..Default::default()
                    },
                ],
            )
            .unwrap();

        let rows = |sqlite: &Sqlite| {
            let count: i64 = sqlite
                .connection
                .query_row("SELECT COUNT(*) FROM event", (), |row| row.get(0))
                .unwrap();
            count
        };
        assert_eq!(rows(&sqlite), 4);

        sqlite
            .remove(&[database::Uncle {
                event: "event",
                number: 6,
            }])
            .unwrap();
        assert_eq!(rows(&sqlite), 3);

        sqlite
            .remove(&[database::Uncle {
                event: "eventAAA",
                number: 1,
            }])
            .unwrap();
        assert_eq!(rows(&sqlite), 3);

        sqlite
            .remove(&[database::Uncle {
                event: "event",
                number: 1,
            }])
            .unwrap();
        assert_eq!(rows(&sqlite), 0);
    }
}
