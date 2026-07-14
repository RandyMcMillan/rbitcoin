//! Descriptor wallets (Phase 8). Legacy Core wallets are intentionally unsupported.

pub fn crate_name() -> &'static str {
    "rbitcoin-wallet"
}

/// Only descriptor wallets are supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletKind {
    Descriptor,
}

impl WalletKind {
    pub fn is_supported(self) -> bool {
        matches!(self, WalletKind::Descriptor)
    }

    /// Parse createwallet-style kind flags. Non-descriptor is rejected.
    pub fn from_descriptors_flag(descriptors: bool) -> Result<Self, WalletError> {
        if descriptors {
            Ok(WalletKind::Descriptor)
        } else {
            Err(WalletError::LegacyNotSupported)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalletError {
    LegacyNotSupported,
}

impl std::fmt::Display for WalletError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalletError::LegacyNotSupported => {
                write!(f, "legacy (non-descriptor) wallets are not supported")
            }
        }
    }
}

impl std::error::Error for WalletError {}
