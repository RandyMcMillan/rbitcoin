use rbitcoin_store::StoreError;
use std::fmt;

#[derive(Debug)]
pub enum ConsensusError {
    Store(StoreError),
    BadHeader(&'static str),
    BadBlock(&'static str),
    BadTx(&'static str),
    Script(String),
    MissingPrevout,
    PrevoutSpent,
    InvalidPow,
    BadPrev,
}

impl fmt::Display for ConsensusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsensusError::Store(e) => write!(f, "store: {e}"),
            ConsensusError::BadHeader(s) => write!(f, "bad header: {s}"),
            ConsensusError::BadBlock(s) => write!(f, "bad block: {s}"),
            ConsensusError::BadTx(s) => write!(f, "bad transaction: {s}"),
            ConsensusError::Script(s) => write!(f, "script verification failed: {s}"),
            ConsensusError::MissingPrevout => f.write_str("missing prevout"),
            ConsensusError::PrevoutSpent => f.write_str("prevout already spent on best chain"),
            ConsensusError::InvalidPow => f.write_str("pow invalid"),
            ConsensusError::BadPrev => f.write_str("unexpected previous header"),
        }
    }
}

impl std::error::Error for ConsensusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConsensusError::Store(e) => Some(e),
            _ => None,
        }
    }
}

impl From<StoreError> for ConsensusError {
    fn from(e: StoreError) -> Self {
        // Peer / untrusted blocks can hit archive stamp when a parent is simply
        // missing (invalid block). That is consensus MissingPrevout, not store
        // corruption — see docs/external_findings/002-store-corrupt-record-on-invalid-block.md.
        match &e {
            StoreError::Corrupt(m) if m.contains("parent create_fk unresolved") => {
                ConsensusError::MissingPrevout
            }
            _ => ConsensusError::Store(e),
        }
    }
}

/// Core `BlockValidationState` / `debug.log` reject needle for a confirm error.
///
/// P2P `assert_debug_log` and `submitblock` share this. Script messages keep
/// our internal tokens (`CSV negative`); only this helper speaks Core English.
pub fn block_reject_reason(err: &ConsensusError) -> String {
    match err {
        ConsensusError::BadTx("not final" | "bad-txns-nonfinal") => "bad-txns-nonfinal".into(),
        ConsensusError::BadTx(s) => (*s).into(),
        ConsensusError::BadBlock(s) => (*s).into(),
        ConsensusError::BadHeader(s) => (*s).into(),
        ConsensusError::Script(s) => {
            let inner = s.split(" txid=").next().unwrap_or(s.as_str());
            let paren = match inner {
                "CSV negative" | "CLTV negative" => "Negative locktime",
                "CSV" | "CSV version" | "CLTV" | "CLTV type" | "CLTV final sequence" => {
                    "Locktime requirement not satisfied"
                }
                "stack empty" | "stack size" => "Operation not valid with the current stack size",
                "NULLDUMMY" => "Dummy CHECKMULTISIG argument must be zero",
                other => other,
            };
            format!("block-script-verify-flag-failed ({paren})")
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn display_and_source_cover_all_variants() {
        let store = ConsensusError::Store(StoreError::NotFound);
        assert!(store.to_string().contains("store:"));
        assert!(store.source().is_some());

        let cases: &[(ConsensusError, &str)] = &[
            (ConsensusError::BadHeader("bits"), "bad header: bits"),
            (ConsensusError::BadBlock("empty"), "bad block: empty"),
            (ConsensusError::BadTx("fee"), "bad transaction: fee"),
            (
                ConsensusError::Script("sig".into()),
                "script verification failed: sig",
            ),
            (ConsensusError::MissingPrevout, "missing prevout"),
            (
                ConsensusError::PrevoutSpent,
                "prevout already spent on best chain",
            ),
            (ConsensusError::InvalidPow, "pow invalid"),
            (ConsensusError::BadPrev, "unexpected previous header"),
        ];
        for (err, needle) in cases {
            assert_eq!(err.to_string(), *needle);
            assert!(err.source().is_none());
        }
    }

    #[test]
    fn archive_unresolved_parent_is_missing_prevout_not_corrupt() {
        let e = ConsensusError::from(StoreError::Corrupt(
            "archive: parent create_fk unresolved (contiguous batch required)",
        ));
        assert!(matches!(e, ConsensusError::MissingPrevout), "got {e:?}");
        assert!(!e.to_string().contains("corrupt"));
    }

    #[test]
    fn from_store_error() {
        let e: ConsensusError = StoreError::NotFound.into();
        assert!(matches!(e, ConsensusError::Store(StoreError::NotFound)));
    }

    #[test]
    fn block_reject_reason_bip113_and_bip112_needles() {
        assert_eq!(
            block_reject_reason(&ConsensusError::BadTx("not final")),
            "bad-txns-nonfinal"
        );
        assert_eq!(
            block_reject_reason(&ConsensusError::BadTx("bad-txns-nonfinal")),
            "bad-txns-nonfinal"
        );
        assert_eq!(
            block_reject_reason(&ConsensusError::Script("CSV negative".into())),
            "block-script-verify-flag-failed (Negative locktime)"
        );
        assert_eq!(
            block_reject_reason(&ConsensusError::Script("CSV negative txid=ab vin=0".into())),
            "block-script-verify-flag-failed (Negative locktime)"
        );
        assert_eq!(
            block_reject_reason(&ConsensusError::Script("stack empty".into())),
            "block-script-verify-flag-failed (Operation not valid with the current stack size)"
        );
        assert_eq!(
            block_reject_reason(&ConsensusError::Script("CSV".into())),
            "block-script-verify-flag-failed (Locktime requirement not satisfied)"
        );
    }
}
