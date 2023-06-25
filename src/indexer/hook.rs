//! Indexing hooks.

use crate::config;
use anyhow::Result;
use std::fs;

pub struct Hook {
    /// The raw source of the SQL query to run for this hook.
    source: String,
    /// Bit flags for tracking which [`Event`]s this hook should run for.
    events: u8,
}

/// An event where a hook can be run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Event {
    Init = 0x1,
    Block = 0x2,
    Finalize = 0x4,
}

impl Hook {
    /// Creates a new [`Hook`] from its configuration.
    pub fn new(config: config::Hook) -> Result<Self> {
        let (source, events) = match config {
            config::Hook::Block { source } => (
                source,
                Event::Init as u8 | Event::Block as u8 | Event::Finalize as u8,
            ),
            config::Hook::Finalize { source, init } => (
                source,
                if init { Event::Init as u8 } else { 0 } | Event::Finalize as u8,
            ),
        };
        let source = match source {
            config::HookSource::Sql { sql } => sql,
            config::HookSource::File { file } => fs::read_to_string(file)?,
        };

        Ok(Self { source, events })
    }

    /// Returns
    pub fn get(&self, event: Event) -> Option<&str> {
        (self.events & event as u8 != 0).then_some(&self.source)
    }
}
