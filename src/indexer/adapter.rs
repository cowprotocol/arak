//! An adapter for a single event. This module is responsible for taking an
//! Arak event configuration and creating an adapter that can:
//! - Generate Ethereum RPC log filters for the specified configuration
//! - Decode Ethereum log topics and data into Solidity values

use {
    crate::config,
    anyhow::{Context, Result},
    ethrpc::types::{ArrayVec, Digest, LogBlocks, LogFilter, LogFilterValue},
    solabi::{
        abi::EventDescriptor,
        value::{EventEncoder, Value},
    },
    std::borrow::Cow,
};

/// An [`Adapter`] is an adapter for a single event. Here's an example image of
/// an [`Adapter`]. Its purpose is to adapt the single event.
/// https://www.bhphotovideo.com/images/images2500x2500/hp_as615at_displayport_to_vga_adapter_1024540.jpg
pub struct Adapter {
    name: String,
    signature: EventDescriptor,
    start: u64,
    filter: LogFilter,
    encoder: EventEncoder,
}

impl Adapter {
    /// Creates a new adapter for a single event.
    pub fn new(config: config::Event) -> Result<Self> {
        let filter = LogFilter {
            address: match config.contract {
                config::Contract::All => LogFilterValue::Any,
                config::Contract::Address(address) => LogFilterValue::Exact(address),
            },
            topics: {
                let mut topics = ArrayVec::<_, 4>::new();
                topics.try_push(LogFilterValue::Exact(Digest(
                    config
                        .signature
                        .selector()
                        .context("anonymous events are not supported")?,
                )))?;
                topics.extend(config.topics);
                topics
            },
            blocks: LogBlocks::default(),
        };
        let encoder = EventEncoder::new(&config.signature)?;

        Ok(Self {
            name: config.name,
            signature: config.signature,
            start: config.start,
            filter,
            encoder,
        })
    }

    #[cfg(test)]
    pub fn for_signature(signature: &str) -> Self {
        Adapter::new(config::Event::for_signature(signature)).unwrap()
    }

    /// Returns the name of the event indexer.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the signature of the adapter.
    pub fn signature(&self) -> &EventDescriptor {
        &self.signature
    }

    /// Returns the first block to index events for.
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Returns a log filter for the specified blocks.
    pub fn filter(&self, blocks: LogBlocks) -> LogFilter {
        LogFilter {
            blocks,
            ..self.filter.clone()
        }
    }

    /// Decodes Ethereum log topics and data into a database event for storing.
    pub fn decode(&self, topics: &[Digest], data: &[u8]) -> Result<Vec<Value>> {
        let fields = self.encoder.decode(&solabi::log::Log {
            topics: {
                let mut converted = solabi::log::Topics::default();
                for topic in topics {
                    converted.push(topic);
                }
                converted
            },
            data: Cow::Borrowed(data),
        })?;
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        hex_literal::hex,
        solabi::{
            ethprim::{address, digest, keccak, uint},
            value::Uint,
        },
    };

    #[test]
    fn no_anonymous_events() {
        assert!(Adapter::new(config::Event::for_signature("event Foo() anonymous;")).is_err());
    }

    #[test]
    fn decode_event() {
        let indexer = Adapter::for_signature(
            "event Transfer(address indexed to, address indexed from, uint256 value)",
        );

        let topics = [
            keccak!("Transfer(address,address,uint256)"),
            digest!("0x0000000000000000000000000101010101010101010101010101010101010101"),
            digest!("0x0000000000000000000000000202020202020202020202020202020202020202"),
        ];
        let data = hex!("0000000000000000000000000000000000000000000000003a4965bf58a40000");

        assert_eq!(
            indexer.decode(&topics, &data).unwrap(),
            [
                Value::Address(address!("0x0101010101010101010101010101010101010101")),
                Value::Address(address!("0x0202020202020202020202020202020202020202")),
                Value::Uint(Uint::new(256, uint!("4_200_000_000_000_000_000")).unwrap()),
            ]
        );
    }

    #[test]
    fn non_primitive_indexed_field() {
        let decoder = Adapter::for_signature("event Foo(string indexed note, bool indexed flag);");

        let topics = [
            keccak!("Foo(string,bool)"),
            keccak!("hello"),
            digest!("0x0000000000000000000000000000000000000000000000000000000000000001"),
        ];
        let data = [];

        assert_eq!(
            decoder.decode(&topics, &data).unwrap(),
            [
                Value::FixedBytes(keccak!("hello").0.into()),
                Value::Bool(true),
            ]
        );
    }
}
