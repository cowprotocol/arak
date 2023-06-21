//! A local representation of the blockchain. This keeps track of block hashes
//! past the finalized block and detects reorgs.

use anyhow::Result;
use solabi::ethprim::{AsU256, Digest, U256};
use std::collections::VecDeque;

/// Local blockchain state.
#[derive(Clone, Debug)]
pub struct Chain {
    hashes: VecDeque<Digest>,
    finalized: U256,
}

impl Chain {
    /// Initializes a new local blockchain state from the finalized block's
    /// number and hash.
    pub fn new(finalized: U256, hash: Digest) -> Self {
        let mut hashes = VecDeque::new();
        hashes.push_front(hash);

        Self { hashes, finalized }
    }

    /// Returns the next block number in the chain.
    pub fn next(&self) -> U256 {
        self.finalized + self.hashes.len().as_u256()
    }

    /// Appends the next block in the chain to the local state.
    pub fn append(&mut self, hash: Digest, parent: Digest) -> Result<Append> {
        if parent != self.hashes[0] {
            anyhow::ensure!(self.hashes.len() > 1, "reorg past finalized block");

            self.hashes.pop_front();
            return Ok(Append::Reorg);
        }

        self.hashes.push_front(hash);
        Ok(Append::Ok)
    }

    /// Updates the finalized block. Returns the previous finalized block.
    pub fn finalize(&mut self, finalized: U256) -> Result<U256> {
        anyhow::ensure!(
            (self.finalized..self.next()).contains(&finalized),
            "invalid finalized block"
        );

        let keep = self.next() - finalized;
        let old = self.finalized;

        self.finalized = finalized;
        self.hashes.truncate(keep.as_usize());

        Ok(old)
    }
}

/// The result of appending a new block to the local chain state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Append {
    Ok,
    Reorg,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_blocks() {
        let d = |b: u8| Digest([b; 32]);

        let mut chain = Chain::new(U256::new(1), d(0x10));
        assert_eq!(chain.next(), 2);

        // Error reorgs past finalized block.
        assert!(chain.append(d(1), d(0)).is_err());

        assert_eq!(chain.append(d(0x20), d(0x10)).unwrap(), Append::Ok);
        assert_eq!(chain.next(), 3);

        assert_eq!(chain.append(d(0x30), d(0x20)).unwrap(), Append::Ok);
        assert_eq!(chain.next(), 4);

        assert_eq!(chain.append(d(0x40), d(0x31)).unwrap(), Append::Reorg);
        assert_eq!(chain.next(), 3);

        assert_eq!(chain.append(d(0x31), d(0x20)).unwrap(), Append::Ok);
        assert_eq!(chain.next(), 4);

        assert_eq!(chain.append(d(0x40), d(0x31)).unwrap(), Append::Ok);
        assert_eq!(chain.next(), 5);
    }

    #[test]
    fn finalizes_blocks() {
        let d = |b: u8| Digest([b; 32]);

        let mut chain = Chain::new(U256::new(1), d(1));
        for i in 2..100 {
            chain.append(d(i), d(i - 1)).unwrap();
        }

        assert_eq!(chain.next(), 100);

        // Before currently finalized block.
        assert!(chain.finalize(U256::new(0)).is_err());

        // After last known block.
        assert!(chain.finalize(U256::new(100)).is_err());

        // Proper block - note that `next()` doesn't change!
        chain.finalize(U256::new(42)).unwrap();
        assert_eq!(chain.next(), 100);
        assert_eq!(chain.append(d(100), d(99)).unwrap(), Append::Ok);
    }
}
