use rbitcoin_store::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("bad header: {0}")]
    BadHeader(&'static str),
    #[error("bad block: {0}")]
    BadBlock(&'static str),
    #[error("bad transaction: {0}")]
    BadTx(&'static str),
    #[error("script verification failed: {0}")]
    Script(String),
    #[error("missing prevout")]
    MissingPrevout,
    #[error("prevout already spent on best chain")]
    PrevoutSpent,
    #[error("pow invalid")]
    InvalidPow,
    #[error("unexpected previous header")]
    BadPrev,
}
