//! Shared primitives for the rbitcoin workspace.
//!
//! Keep consensus-heavy types in rust-bitcoin once wired; this crate holds
//! store and node newtypes that must stay stable across crates.

mod hex;

pub use hex::{decode as hex_decode, encode as hex_encode, HexError};

use std::fmt;

/// Workspace schema / API version string for diagnostics.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// File magic for store tables: ASCII `RBT1`.
pub const STORE_MAGIC: [u8; 4] = *b"RBT1";

/// Current on-disk schema version (see workspace `SCHEMA.md`).
///
/// v8: `tx.head` is a fixed keyless address table (`2^31` × 8 B entries, double-hash
/// probe, HAS_NEXT bit; body verify). Header / scripthash heads still use v7-style
/// 16 B key prefixes + optional `.mlt` multi-lists.
///
/// v7: hash heads (tx/header/…) use 16 B key prefixes + optional multi-fk list
/// (`.mlt`) for prefix collisions and BIP30 duplicate txids; body verify on lookup.
///
/// v6: scripthash head 16 B key prefix + 16 B value; body entry = create_tx_fk only.
///
/// v5: spend annotation on each output (`spender_field` + rare multi `spenders.body`);
/// no `point.head` open-hash multimap.
///
/// v4: hybrid scripthash (2-inline head or geometric body slab + size-class freelist).
///
/// v3: thin point/scripthash linked lists; strong_tx bitset; denser hash heads;
/// Class A inputs always external `prev_txid`.
pub const SCHEMA_VERSION: u16 = 8;

/// 1-based foreign key into a store table body. Zero means null / absent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Fk(pub u64);

impl Fk {
    pub const NULL: Fk = Fk(0);

    pub fn is_null(self) -> bool {
        self.0 == 0
    }

    pub fn new(id: u64) -> Option<Fk> {
        if id == 0 {
            None
        } else {
            Some(Fk(id))
        }
    }

    pub fn get(self) -> Option<u64> {
        if self.is_null() {
            None
        } else {
            Some(self.0)
        }
    }
}

impl fmt::Display for Fk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            write!(f, "Fk(null)")
        } else {
            write!(f, "Fk({})", self.0)
        }
    }
}

/// Block height (genesis = 0).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Height(pub u32);

impl Height {
    pub const GENESIS: Height = Height(0);

    pub fn next(self) -> Option<Height> {
        self.0.checked_add(1).map(Height)
    }
}

impl fmt::Display for Height {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Bitcoin network selection for the node process.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Network {
    #[default]
    Mainnet,
    Testnet,
    Signet,
    Regtest,
}

impl Network {
    pub fn as_str(self) -> &'static str {
        match self {
            Network::Mainnet => "mainnet",
            Network::Testnet => "testnet",
            Network::Signet => "signet",
            Network::Regtest => "regtest",
        }
    }

    pub fn parse(s: &str) -> Result<Network, ParseNetworkError> {
        match s.to_ascii_lowercase().as_str() {
            "main" | "mainnet" => Ok(Network::Mainnet),
            "test" | "testnet" | "testnet3" => Ok(Network::Testnet),
            "signet" => Ok(Network::Signet),
            "regtest" => Ok(Network::Regtest),
            other => Err(ParseNetworkError {
                input: other.to_string(),
            }),
        }
    }
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Unknown network name from CLI / config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseNetworkError {
    pub input: String,
}

impl fmt::Display for ParseNetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown network `{}`", self.input)
    }
}

impl std::error::Error for ParseNetworkError {}

/// Table kind identifiers stored in file headers ([`SCHEMA.md`](../../SCHEMA.md)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum TableKind {
    Meta = 1,
    Header = 2,
    Tx = 3,
    Input = 4,
    Output = 5,
    /// Legacy id (v4 point multimap); new stores use [`TableKind::Spender`].
    Point = 6,
    StrongTx = 7,
    Confirmed = 8,
    ArrayLink = 9,
    HashHead = 10,
    /// Electrum scripthash multimap (SHA256(scriptPubKey)).
    ScriptHash = 11,
    /// Class C: tx_fk-1 → create height+1 (0 = unset). Maturity without UTXO.
    TxHeight = 12,
    /// Multi-spender list nodes (16 B: spending_tx_fk | next).
    Spender = 13,
}

impl TableKind {
    pub fn from_u16(v: u16) -> Option<TableKind> {
        match v {
            1 => Some(TableKind::Meta),
            2 => Some(TableKind::Header),
            3 => Some(TableKind::Tx),
            4 => Some(TableKind::Input),
            5 => Some(TableKind::Output),
            6 => Some(TableKind::Point),
            7 => Some(TableKind::StrongTx),
            8 => Some(TableKind::Confirmed),
            9 => Some(TableKind::ArrayLink),
            10 => Some(TableKind::HashHead),
            11 => Some(TableKind::ScriptHash),
            12 => Some(TableKind::TxHeight),
            13 => Some(TableKind::Spender),
            _ => None,
        }
    }

    pub fn as_u16(self) -> u16 {
        self as u16
    }
}
