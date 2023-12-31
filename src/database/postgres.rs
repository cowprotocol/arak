use {
    crate::database::{
        self,
        event_to_tables::Table,
        event_visitor::{self, VisitValue},
        Database, Log,
    },
    anyhow::{anyhow, Context, Result},
    futures::{future::BoxFuture, FutureExt},
    pg_bigdecimal::{BigDecimal, PgNumeric},
    solabi::{
        abi::EventDescriptor,
        value::{Value as AbiValue, ValueKind as AbiKind},
    },
    std::{collections::HashMap, fmt::Write, str::FromStr},
};

pub struct Postgres {
    client: tokio_postgres::Client,
    /// Invariant: Events in the map have corresponding tables in the database.
    ///
    /// The key is the `name` argument when the event was passed into
    /// `prepare_event`.
    events: HashMap<String, PreparedEvent>,

    get_event_block: tokio_postgres::Statement,
    set_event_block: tokio_postgres::Statement,
    set_indexed_block: tokio_postgres::Statement,
    new_event_block: tokio_postgres::Statement,
}

/// An event is represented in the database in several tables.
///
/// All tables have some columns that are unrelated to the event's fields. See
/// `FIXED_COLUMNS`. The first table contains all fields that exist once per
/// event which means they do not show up in arrays. The other tables contain
/// fields that are part of arrays. Those tables additionally have the column
/// `ARRAY_COLUMN`.
///
/// The order of tables and fields is given by the `event_visitor` module.
struct PreparedEvent {
    descriptor: EventDescriptor,
    insert_statements: Vec<InsertStatement>,
    /// Prepared statements for removing rows starting at some block number.
    /// Every statement takes a block number as parameter.
    remove_statements: Vec<tokio_postgres::Statement>,
}

async fn connect(params: &str) -> Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(params, tokio_postgres::NoTls)
        .await
        .context("connect client")?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

impl Postgres {
    pub async fn connect(params: &str) -> Result<Self> {
        tracing::debug!("opening postgres database");
        let client = connect(params).await.context("connect")?;

        client
            .execute(CREATE_EVENT_BLOCK_TABLE, &[])
            .await
            .context("create event_block table")?;

        let get_event_block = client
            .prepare(GET_EVENT_BLOCK)
            .await
            .context("prepare GET_EVENT_BLOCK")?;
        let set_event_block = client
            .prepare(SET_EVENT_BLOCK)
            .await
            .context("prepare SET_EVENT_BLOCK")?;
        let set_indexed_block = client
            .prepare(SET_INDEXED_BLOCK)
            .await
            .context("prepare SET_INDEXED_BLOCK")?;
        let new_event_block = client
            .prepare(NEW_EVENT_BLOCK)
            .await
            .context("prepare new_event_block")?;

        Ok(Self {
            client,
            events: Default::default(),
            get_event_block,
            set_event_block,
            set_indexed_block,
            new_event_block,
        })
    }
}

impl Database for Postgres {
    fn prepare_event<'a>(
        &'a mut self,
        name: &'a str,
        event: &'a EventDescriptor,
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            let transaction = self.client.transaction().await.context("transaction")?;
            // TODO:
            // - Check that either no table exists or all tables exist and with the right
            //   types.
            // - Maybe have `CHECK` clauses to enforce things like address and integers
            //   having expected length.
            // - Maybe store serialized event descriptor in the database so we can load and
            //   check it.

            if let Some(existing) = self.events.get(name) {
                if event != &existing.descriptor {
                    return Err(anyhow!(
                        "event {} (database name {name}) already exists with different signature",
                        event.name
                    ));
                }
                return Ok(());
            }

            let tables = database::event_to_tables::event_to_tables(name, event)
                .context("unsupported event")?;
            let name = &tables.primary.name;
            Self::create_table(&transaction, false, &tables.primary).await?;
            for table in &tables.dynamic_arrays {
                Self::create_table(&transaction, true, table).await?;
            }

            transaction
                .execute(&self.new_event_block, &[name])
                .await
                .context("execute new_event_block")?;

            let mut insert_statements = Vec::new();
            for (is_array, table) in std::iter::once((false, &tables.primary))
                .chain(std::iter::repeat(true).zip(&tables.dynamic_arrays))
            {
                let mut sql = String::new();
                write!(&mut sql, "INSERT INTO {} VALUES(", table.name).unwrap();
                for i in 0..table.columns.len() + FIXED_COLUMNS_COUNT + is_array as usize {
                    write!(&mut sql, "${},", i + 1).unwrap();
                }
                assert_eq!(sql.pop(), Some(','));
                write!(&mut sql, ");").unwrap();
                tracing::debug!("creating insert statement:\n{}", sql);
                insert_statements.push(InsertStatement {
                    sql: transaction
                        .prepare(&sql)
                        .await
                        .context(format!("prepare {}", sql))?,
                    fields: table.columns.len(),
                });
            }

            let mut remove_statements = Vec::new();
            for table in std::iter::once(&tables.primary).chain(&tables.dynamic_arrays) {
                let sql = format!("DELETE FROM {} WHERE block_number >= $1;", table.name);
                remove_statements.push(
                    transaction
                        .prepare(&sql)
                        .await
                        .context(format!("prepare {}", sql))?,
                );
            }

            self.events.insert(
                name.clone(),
                PreparedEvent {
                    descriptor: event.clone(),
                    insert_statements,
                    remove_statements,
                },
            );

            transaction.commit().await.context("commit")
        }
        .boxed()
    }

    fn event_block<'a>(&'a mut self, name: &'a str) -> BoxFuture<'a, Result<database::Block>> {
        async move {
            let row = self
                .client
                .query_one(&self.get_event_block, &[&name])
                .await
                .context("query GET_EVENT_BLOCK")?;
            let block: (i64, i64) = (row.try_get(0)?, row.try_get(1)?);
            Ok(database::Block {
                indexed: block.0.try_into().context("indexed out of bounds")?,
                finalized: block.1.try_into().context("finalized out of bounds")?,
            })
        }
        .boxed()
    }

    fn update<'a>(
        &'a mut self,
        blocks: &'a [database::EventBlock],
        logs: &'a [database::Log],
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            let mut transaction = self.client.transaction().await.context("transaction")?;

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
                let rows = transaction
                    .execute(&self.set_event_block, &[&block.event, &indexed, &finalized])
                    .await
                    .context("execute SET_EVENT_BLOCK")?;
                if rows != 1 {
                    return Err(anyhow!(
                        "query unexpectedly changed {rows} rows instead of 1"
                    ));
                }
            }

            for log in logs {
                Self::store_event(&mut transaction, &self.events, log)
                    .await
                    .context("store_event")?;
            }

            transaction.commit().await.context("commit")
        }
        .boxed()
    }

    fn remove<'a>(&'a mut self, uncles: &'a [database::Uncle]) -> BoxFuture<'a, Result<()>> {
        async move {
            let transaction = self.client.transaction().await.context("transaction")?;

            for uncle in uncles {
                if uncle.number == 0 {
                    return Err(anyhow!("block 0 got uncled"));
                }
                let block = i64::try_from(uncle.number).context("block out of bounds")?;
                let parent_block = block - 1;
                let prepared = self.events.get(uncle.event).context("unprepared event")?;
                for remove_statement in &prepared.remove_statements {
                    transaction
                        .execute(remove_statement, &[&block])
                        .await
                        .context("execute remove_statement")?;
                    transaction
                        .execute(&self.set_indexed_block, &[&uncle.event, &parent_block])
                        .await
                        .context("execute set_indexed_block")?;
                }
            }

            transaction.commit().await.context("commit")
        }
        .boxed()
    }
}

impl Postgres {
    async fn store_event<'a>(
        transaction: &mut tokio_postgres::Transaction<'a>,
        events: &HashMap<String, PreparedEvent>,
        Log {
            event,
            block_number,
            log_index,
            transaction_index,
            address,
            fields,
        }: &'a Log<'a>,
    ) -> Result<()> {
        let event = events.get(*event).context("unknown event")?;

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
        type ToSqlBox = Box<dyn tokio_postgres::types::ToSql + Send + Sync>;
        let mut sql_values: Vec<(Option<usize>, Vec<ToSqlBox>)> = vec![(None, vec![])];
        let mut in_array: bool = false;
        let mut visitor = |value: VisitValue<'a>| {
            let sql_value: Box<dyn tokio_postgres::types::ToSql + Send + Sync> = match value {
                VisitValue::ArrayStart(len) => {
                    sql_values.push((Some(len), Vec::new()));
                    in_array = true;
                    return;
                }
                VisitValue::ArrayEnd => {
                    in_array = false;
                    return;
                }
                VisitValue::Value(AbiValue::Int(v)) => Box::new(PgNumeric::new(Some(
                    BigDecimal::from_str(&v.get().to_string()).unwrap(),
                ))),
                VisitValue::Value(AbiValue::Uint(v)) => Box::new(PgNumeric::new(Some(
                    BigDecimal::from_str(&v.get().to_string()).unwrap(),
                ))),
                VisitValue::Value(AbiValue::Address(v)) => {
                    Box::new(v.0.into_iter().collect::<Vec<_>>())
                }
                VisitValue::Value(AbiValue::Bool(v)) => Box::new(*v as i64),
                VisitValue::Value(AbiValue::FixedBytes(v)) => Box::new(v.as_bytes().to_vec()),
                VisitValue::Value(AbiValue::Function(v)) => Box::new(
                    v.address
                        .0
                        .iter()
                        .copied()
                        .chain(v.selector.0.iter().copied())
                        .collect::<Vec<_>>(),
                ),
                VisitValue::Value(AbiValue::Bytes(v)) => Box::new(v.to_owned()),
                VisitValue::Value(AbiValue::String(v)) => Box::new(v.as_bytes().to_vec()),
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

        let block_number = i64::try_from(*block_number).unwrap();
        let log_index = i64::try_from(*log_index).unwrap();
        let transaction_index = i64::try_from(*transaction_index).unwrap();
        let address = address.0.as_slice();
        for (statement, (array_element_count, values)) in
            event.insert_statements.iter().zip(sql_values)
        {
            let is_array = array_element_count.is_some();
            let array_element_count = array_element_count.unwrap_or(1);
            assert_eq!(statement.fields * array_element_count, values.len());
            for i in 0..array_element_count {
                let row = &values[i * statement.fields..][..statement.fields];
                let array_index = if is_array {
                    Some(i64::try_from(i).unwrap())
                } else {
                    None
                };
                let params: Vec<_> = [
                    &block_number as &(dyn tokio_postgres::types::ToSql + Sync),
                    &log_index,
                    &transaction_index,
                    &address,
                ]
                .into_iter()
                .chain(
                    array_index
                        .as_ref()
                        .map(|i| i as &(dyn tokio_postgres::types::ToSql + Sync)),
                )
                .chain(
                    row.iter()
                        .map(|v| v.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)),
                )
                .collect();
                transaction
                    .execute(&statement.sql, params.as_slice())
                    .await
                    .context("execute insert")?;
            }
        }

        Ok(())
    }

    async fn create_table<'a>(
        transaction: &tokio_postgres::Transaction<'a>,
        is_array: bool,
        table: &Table<'a>,
    ) -> Result<u64> {
        let mut sql = String::new();
        write!(&mut sql, "CREATE TABLE IF NOT EXISTS {} (", table.name).unwrap();
        write!(&mut sql, "{FIXED_COLUMNS}, ").unwrap();
        if is_array {
            write!(&mut sql, "{ARRAY_COLUMN}, ").unwrap();
        }
        for column in table.columns.iter() {
            write!(&mut sql, "{}", column.name).unwrap();
            let type_ = match abi_kind_to_sql_type(column.kind).unwrap() {
                tokio_postgres::types::Type::INT8 => "INT8",
                tokio_postgres::types::Type::BYTEA => "BYTEA",
                tokio_postgres::types::Type::NUMERIC => "NUMERIC",
                _ => unreachable!(),
            };
            write!(&mut sql, " {type_}, ").unwrap();
        }
        let primary_key = if is_array {
            PRIMARY_KEY_ARRAY
        } else {
            PRIMARY_KEY
        };
        write!(&mut sql, "PRIMARY KEY({primary_key}));").unwrap();
        tracing::debug!("creating table:\n{}", sql);
        transaction
            .execute(&sql, &[])
            .await
            .context("execute CREATE TABLE")
    }
}

/// Columns that every event table has.
const FIXED_COLUMNS: &str = "block_number BIGINT NOT NULL, log_index BIGINT NOT NULL, \
                             transaction_index BIGINT NOT NULL, address BYTEA NOT NULL";
const FIXED_COLUMNS_COUNT: usize = 4;
const PRIMARY_KEY: &str = "block_number, log_index";

/// Column for array tables.
const ARRAY_COLUMN: &str = "array_index BIGINT NOT NULL";
const PRIMARY_KEY_ARRAY: &str = "block_number, log_index, array_index";

const CREATE_EVENT_BLOCK_TABLE: &str = "CREATE TABLE IF NOT EXISTS _event_block(event TEXT \
                                        PRIMARY KEY NOT NULL, indexed BIGINT NOT NULL, finalized \
                                        BIGINT NOT NULL);";
const GET_EVENT_BLOCK: &str = "SELECT indexed, finalized FROM _event_block WHERE event = $1;";
const NEW_EVENT_BLOCK: &str = "INSERT INTO _event_block (event, indexed, finalized) VALUES($1, 0, \
                               0) ON CONFLICT(event) DO NOTHING;";
const SET_EVENT_BLOCK: &str =
    "UPDATE _event_block SET indexed = $2, finalized = $3 WHERE event = $1;";
const SET_INDEXED_BLOCK: &str = "UPDATE _event_block SET indexed = $2 WHERE event = $1";

/// Parameters:
/// - 1: block number
/// - 2: log index
/// - 3: array index if this is an array table (all tables after the first)
/// - 3 + n: n-th event field/column
struct InsertStatement {
    sql: tokio_postgres::Statement,
    /// Number of event fields that map to SQL columns. Does not count
    /// FIXED_COLUMNS and array index.
    fields: usize,
}

impl std::fmt::Debug for InsertStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertStatement")
            .field("fields", &self.fields)
            .finish()
    }
}

fn abi_kind_to_sql_type(value: &AbiKind) -> Option<tokio_postgres::types::Type> {
    match value {
        AbiKind::Int(_) => Some(tokio_postgres::types::Type::NUMERIC),
        AbiKind::Uint(_) => Some(tokio_postgres::types::Type::NUMERIC),
        AbiKind::Address => Some(tokio_postgres::types::Type::BYTEA),
        AbiKind::Bool => Some(tokio_postgres::types::Type::BOOL),
        AbiKind::FixedBytes(_) => Some(tokio_postgres::types::Type::BYTEA),
        AbiKind::Function => Some(tokio_postgres::types::Type::BYTEA),
        AbiKind::Bytes => Some(tokio_postgres::types::Type::BYTEA),
        AbiKind::String => Some(tokio_postgres::types::Type::TEXT),
        AbiKind::FixedArray(_, _) | AbiKind::Tuple(_) | AbiKind::Array(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use solabi::{
        value::{Int, Uint},
        I256, U256,
    };

    use super::*;

    fn local_postgres_url() -> String {
        format!("postgresql://{}@localhost", whoami::username())
    }

    async fn clear_database() {
        let client = connect(&local_postgres_url()).await.unwrap();
        // https://stackoverflow.com/a/36023359
        let query = r#"
DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;
        "#;
        client.batch_execute(query).await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn large_number() {
        clear_database().await;
        let mut db = Postgres::connect(&local_postgres_url()).await.unwrap();
        let event = r#"
event Event (
    uint256,
    int256
)
"#;
        let event = EventDescriptor::parse_declaration(event).unwrap();
        db.prepare_event("event", &event).await.unwrap();
        let log = Log {
            event: "event",
            block_number: 0,
            fields: vec![
                AbiValue::Uint(Uint::new(256, U256::MAX).unwrap()),
                AbiValue::Int(Int::new(256, I256::MIN).unwrap()),
            ],
            ..Default::default()
        };
        db.update(&[], &[log]).await.unwrap();
    }
}
