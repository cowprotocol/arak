//! Ethereum event indexer for a collection of events.

mod adapter;
mod chain;

use self::{adapter::Adapter, chain::Chain};
use crate::{
    config,
    database::{self, Database},
};
use anyhow::{Context, Result};
use ethrpc::{
    eth,
    types::{Block, BlockTag, Hydrated, LogBlocks},
};
use std::{cmp, time::Duration};
use tokio::time;

/// An Ethereum event indexer.
pub struct Indexer<D> {
    eth: ethrpc::http::Client,
    database: D,
    adapters: Vec<Adapter>,
}

/// The indexer run configuration.
#[derive(Clone, Copy, Debug)]
pub struct Run {
    /// The block page size to use when fetching historic event data. Using
    /// larger values will speed up initialization, but may cause issues if too
    /// many events are fetched per page.
    pub page_size: u64,
    /// The poll interval to use when checking for new blocks.
    pub poll_interval: Duration,
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
    ) -> Result<Self> {
        Ok(Self {
            eth,
            database,
            adapters: events
                .into_iter()
                .map(Adapter::new)
                .collect::<Result<_>>()?,
        })
    }

    /// Runs the indexer, continuously fetching updates from the blockchain and
    /// storing them into the database.
    pub async fn run(mut self, config: Run) -> Result<()> {
        let finalized = self.init(config).await?;
        let mut chain = Chain::new(finalized.number, finalized.hash);
        loop {
            if !self.sync(&mut chain).await? {
                time::sleep(config.poll_interval).await;
            };
        }
    }

    /// Initializes an event indexer. This syncs historical event data and
    /// ensures that all events are indexed up until the `finalized` block.
    /// Returns the `finalized` block that it finished indexing until.
    async fn init(&mut self, config: Run) -> Result<Block> {
        for adapter in &self.adapters {
            self.database
                .prepare_event(adapter.name(), adapter.signature())?;
        }

        loop {
            let finalized = self
                .eth
                .execute(
                    eth::GetBlockByNumber,
                    (BlockTag::Finalized.into(), Hydrated::No),
                )
                .await?
                .context("missing finalized block")?;

            // Compute the next block to initialize from per adapter and the
            // earliest initialization block.
            let init = self.init_blocks()?;
            let earliest = init
                .iter()
                .copied()
                .min()
                .unwrap_or(finalized.number.as_u64());
            if finalized.number.as_u64() <= earliest {
                return Ok(finalized);
            }

            let to = cmp::min(finalized.number.as_u64(), earliest + config.page_size - 1);
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
                .filter_map(|(adapter, log)| database_log(adapter, log))
                .collect::<Vec<_>>();

            self.database.update(&blocks, &logs)?;
        }
    }

    /// Synchronises more events. Returns `true` if new blockchain state was
    /// processed.
    async fn sync(&mut self, chain: &mut Chain) -> Result<bool> {
        let mut next = match self
            .eth
            .execute(eth::GetBlockByNumber, (chain.next().into(), Hydrated::No))
            .await?
        {
            Some(value) => value,
            None => return Ok(false),
        };

        tracing::debug!(
            block = %next.number, hash = %next.hash,
            "found new block"
        );

        let mut pending = Vec::new();
        while match chain.append(next.hash, next.parent_hash)? {
            chain::Append::Reorg => true,
            chain::Append::Ok => false,
        } {
            pending.push((next.hash, next.parent_hash));
            next = self
                .eth
                .execute(eth::GetBlockByNumber, (chain.next().into(), Hydrated::No))
                .await?
                .context("missing block data for past block")?;
        }

        if !pending.is_empty() {
            let (block, hash) = (next.number - 1, next.parent_hash);
            tracing::debug!(%block, %hash, "reorg");
        }

        // TODO(nlordell): Remove reorged events in a single transaction!
        // TODO(nlordell): Check database event indexed block matches!
        // TODO(nlordell): Update finalized block in the database!

        Ok(true)
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

fn database_log<'a>(adapter: &'a Adapter, log: ethrpc::types::Log) -> Option<database::Log<'a>> {
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
}
