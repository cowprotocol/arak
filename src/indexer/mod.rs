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

        let unfinalized = self
            .adapters
            .iter()
            .filter_map(|adapter| {
                let block = match self.database.event_block(adapter.name()) {
                    Ok(value) => value,
                    Err(err) => return Some(Err(err)),
                };

                (block.indexed > block.finalized).then(|| {
                    Ok(database::Uncle {
                        event: adapter.name(),
                        number: block.finalized + 1,
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for unfinalized in &unfinalized {
            tracing::info!(
                event = %unfinalized.event, finalized = %unfinalized.number,
                "removing logs for unfinalized blocks"
            );
        }
        if !unfinalized.is_empty() {
            self.database.remove(&unfinalized)?;
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
                .map(|adapter| database::EventBlock {
                    event: adapter.name(),
                    block: database::Block {
                        indexed: to,
                        finalized: finalized.number.as_u64(),
                    },
                })
                .collect::<Vec<_>>();
            let logs = adapters
                .into_iter()
                .zip(results)
                .flat_map(|(adapter, logs)| database_logs(adapter, logs))
                .collect::<Vec<_>>();

            self.database.update(&blocks, &logs)?;
        }
    }

    /// Synchronises more events. Returns `true` if new blockchain state was
    /// processed.
    async fn sync(&mut self, chain: &mut Chain) -> Result<bool> {
        // TODO(nlordell): Remove reorged blocks and update with new data in a
        // single database transaction.

        let next = match self
            .eth
            .execute(eth::GetBlockByNumber, (chain.next().into(), Hydrated::No))
            .await?
        {
            Some(value) => value,
            None => return Ok(false),
        };

        match chain.append(next.hash, next.parent_hash)? {
            chain::Append::Ok => {
                tracing::debug!(
                    block = %next.number, hash = %next.hash,
                    "found new block"
                );
            }
            chain::Append::Reorg => {
                let block = next.number - 1;
                tracing::debug!(%block, hash = %next.parent_hash, "reorg");

                let uncles = self
                    .adapters
                    .iter()
                    .map(|adapter| database::Uncle {
                        event: adapter.name(),
                        number: block.as_u64(),
                    })
                    .collect::<Vec<_>>();
                self.database.remove(&uncles)?;
                return Ok(true);
            }
        }

        let (finalized, results) = tokio::try_join!(
            async {
                self.eth
                    .execute(
                        eth::GetBlockByNumber,
                        (BlockTag::Finalized.into(), Hydrated::No),
                    )
                    .await?
                    .context("missing finalized block")
            },
            async {
                self.eth
                    .batch(
                        self.adapters
                            .iter()
                            .map(|adapter| {
                                (eth::GetLogs, (adapter.filter(LogBlocks::Hash(next.hash)),))
                            })
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .map_err(anyhow::Error::from)
            },
        )?;

        if chain.finalize(finalized.number)? != finalized.number {
            tracing::debug!(
                block = %finalized.number,
                "updated finalized block"
            );
        }

        let blocks = self
            .adapters
            .iter()
            .map(|adapter| database::EventBlock {
                event: adapter.name(),
                block: database::Block {
                    indexed: next.number.as_u64(),
                    finalized: finalized.number.as_u64(),
                },
            })
            .collect::<Vec<_>>();
        let logs = self
            .adapters
            .iter()
            .zip(results)
            .flat_map(|(adapter, logs)| database_logs(adapter, logs))
            .collect::<Vec<_>>();

        self.database.update(&blocks, &logs)?;
        Ok(true)
    }

    /// Computes the blocks to start initialising from for each adapter.
    fn init_blocks(&mut self) -> Result<Vec<u64>> {
        self.adapters
            .iter()
            .map(|adapter| {
                Ok(cmp::max(
                    adapter.start(),
                    self.database.event_block(adapter.name())?.indexed + 1,
                ))
            })
            .collect()
    }
}

fn database_logs(
    adapter: &Adapter,
    logs: Vec<ethrpc::types::Log>,
) -> impl Iterator<Item = database::Log> {
    if !logs.is_empty() {
        tracing::debug!(
            event = %adapter.name(), logs = %logs.len(),
            "fetched logs"
        );
    }

    logs.into_iter().filter_map(move |log| {
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
}
