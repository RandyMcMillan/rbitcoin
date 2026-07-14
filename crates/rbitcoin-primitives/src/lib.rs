//! Shared primitives for the rbitcoin workspace.
//!
//! Keep consensus-heavy types in rust-bitcoin once wired; this crate holds
//! store and node newtypes that must stay stable across crates.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Workspace schema / API version string for diagnostics.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// File magic for store tables: ASCII `RBT1`.
pub const STORE_MAGIC: [u8; 4] = *b"RBT1";

/// Current on-disk schema version ([`SCHEMA.md`](../../SCHEMA.md)).
pub const SCHEMA_VERSION: u16 = 0;

/// 1-based foreign key into a store table body. Zero means null / absent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown network `{input}`")]
pub struct ParseNetworkError {
    pub input: String,
}

/// Table kind identifiers stored in file headers ([`SCHEMA.md`](../../SCHEMA.md)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum TableKind {
    Meta = 1,
    Header = 2,
    Tx = 3,
    Input = 4,
    Output = 5,
    Point = 6,
    StrongTx = 7,
    Confirmed = 8,
    ArrayLink = 9,
    HashHead = 10,
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
            _ => None,
        }
    }

    pub fn as_u16(self) -> u16 {
        self as u16
    }
}
