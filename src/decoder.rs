//! Module implementing event decoding logic. This module is responsible for
//! taking an Ethereum log and decoding it into event fields.

use anyhow::Result;
use solabi::{
    abi::EventDescriptor,
    log::Log,
    value::{EventEncoder, Value},
};

pub struct Decoder {
    encoder: EventEncoder,
}

impl Decoder {
    pub fn new(event: &EventDescriptor) -> Result<Self> {
        anyhow::ensure!(!event.anonymous, "anonymous events are not supported");
        let encoder = EventEncoder::new(event)?;

        Ok(Self { encoder })
    }

    pub fn decode(&self, log: &Log) -> Result<Vec<Value>> {
        let fields = self.encoder.decode(log)?;
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use solabi::{
        ethprim::{address, keccak, uint},
        log::Topics,
        value::Uint,
    };

    #[test]
    fn no_anonymous_events() {
        assert!(Decoder::new(
            &EventDescriptor::parse_declaration("event Foo() anonymous;").unwrap()
        )
        .is_err());
    }

    #[test]
    fn decode_event() {
        let decoder = Decoder::new(
            &EventDescriptor::parse_declaration(
                "event Transfer(address indexed to, address indexed from, uint256 value)",
            )
            .unwrap(),
        )
        .unwrap();

        let log = Log {
            topics: Topics::from([
                keccak!("Transfer(address,address,uint256)").0,
                hex!("0000000000000000000000000101010101010101010101010101010101010101"),
                hex!("0000000000000000000000000202020202020202020202020202020202020202"),
            ]),
            data: hex!("0000000000000000000000000000000000000000000000003a4965bf58a40000")[..]
                .into(),
        };

        assert_eq!(
            decoder.decode(&log).unwrap(),
            [
                Value::Address(address!("0x0101010101010101010101010101010101010101")),
                Value::Address(address!("0x0202020202020202020202020202020202020202")),
                Value::Uint(Uint::new(256, uint!("4_200_000_000_000_000_000")).unwrap()),
            ]
        );
    }

    #[test]
    fn non_primitive_indexed_field() {
        let decoder = Decoder::new(
            &EventDescriptor::parse_declaration(
                "event Foo(string indexed note, bool indexed flag);",
            )
            .unwrap(),
        )
        .unwrap();

        let log = Log {
            topics: Topics::from([
                keccak!("Foo(string,bool)").0,
                keccak!("hello").0,
                hex!("0000000000000000000000000000000000000000000000000000000000000001"),
            ]),
            data: vec![].into(),
        };

        assert_eq!(
            decoder.decode(&log).unwrap(),
            [
                Value::FixedBytes(keccak!("hello").0.into()),
                Value::Bool(true),
            ]
        );
    }
}
