//! Ethereum event indexer for a collection of events.

mod adapter;

use self::adapter::Adapter;
use crate::{
    config,
    database::{self, Database},
};
use anyhow::{Context, Result};
use ethrpc::{
    eth,
    types::{BlockTag, Hydrated, LogBlocks},
};
use std::cmp;

/// An Ethereum event indexer.
pub struct Indexer<D> {
    eth: ethrpc::http::Client,
    database: D,
    adapters: Vec<Adapter>,
}

impl<D> Indexer<D>
where
    D: Database,
{
    /// Creates a new event indexer.
    pub fn create(
        eth: ethrpc::http::Client,
        database: D,
        events: Vec<config::Event>,
    ) -> Result<Uninit<D>> {
        Ok(Uninit(Self {
            eth,
            database,
            adapters: events
                .into_iter()
                .map(Adapter::new)
                .collect::<Result<_>>()?,
        }))
    }

    /// Initializes an event indexer. This syncs historical event data and
    /// ensures that all events are indexed up until the `safe` block.
    async fn init(mut self, page_size: u64) -> Result<Self> {
        for adapter in &self.adapters {
            self.database
                .prepare_event(adapter.name(), adapter.signature())?;
        }

        loop {
            let safe = self
                .eth
                .execute(eth::GetBlockByNumber, (BlockTag::Safe.into(), Hydrated::No))
                .await?
                .context("missing safe block")?
                .number
                .as_u64();

            // Compute the next block to initialize from per adapter and the
            // earliest initialization block.
            let init = self.init_blocks()?;
            let earliest = init.iter().copied().min().unwrap_or(safe);
            if earliest >= safe {
                tracing::info!(block = %safe, "indexer initialized");
                return Ok(self);
            }

            let to = cmp::min(safe, earliest + page_size - 1);
            tracing::debug!(from =% earliest, %to, "indexing blocks");

            // Prepare `eth_getLogs` queries, noting the indices of their
            // corresponding adapters for decoding responses.
            let (adapters, queries) = self
                .adapters
                .iter()
                .zip(init.iter().copied())
                .filter(|(_, from)| *from <= to)
                .map(|(adapter, from)| {
                    (
                        adapter,
                        (
                            eth::GetLogs,
                            (adapter.filter(LogBlocks::Range {
                                from: from.into(),
                                to: to.into(),
                            }),),
                        ),
                    )
                })
                .unzip::<_, _, Vec<_>, Vec<_>>();
            let results = self.eth.batch(queries).await?;

            // Compute the database updates required:
            // - Update latest indexed blocks for the events that were queried
            // - Add the logs to the DB.
            let blocks = adapters
                .iter()
                .copied()
                .map(|adapter| database::IndexedBlock {
                    event: adapter.name(),
                    number: to,
                })
                .collect::<Vec<_>>();
            let logs = adapters
                .into_iter()
                .zip(results)
                .flat_map(|(adapter, logs)| logs.into_iter().map(move |log| (adapter, log)))
                .filter_map(|(adapter, log)| {
                    let fields = match adapter.decode(&log.topics, &log.data) {
                        Ok(fields) => fields,
                        Err(err) => {
                            tracing::warn!(?err, ?log, "failed to decode log");
                            return None;
                        }
                    };

                    Some(database::Log {
                        event: adapter.name(),
                        block_number: log.block_number.as_u64(),
                        log_index: log.log_index.as_u64(),
                        transaction_index: log.transaction_index.as_u64(),
                        address: log.address,
                        fields,
                    })
                })
                .collect::<Vec<_>>();

            self.database.update(&blocks, &logs)?;
        }
    }

    /// Computes the blocks to start initialising from for each adapter.
    fn init_blocks(&mut self) -> Result<Vec<u64>> {
        self.adapters
            .iter()
            .map(|adapter| {
                Ok(cmp::max(
                    adapter.start(),
                    self.database.event_block(adapter.name())? + 1,
                ))
            })
            .collect()
    }
}

/// An uninitialized event indexer.
pub struct Uninit<D>(Indexer<D>);

impl<D> Uninit<D>
where
    D: Database,
{
    /// Initializes an event indexer. This syncs historical event data and
    /// ensures that all events are indexed up until the `safe` block.
    pub async fn init(self, page_size: u64) -> Result<Indexer<D>> {
        self.0.init(page_size).await
    }
}
