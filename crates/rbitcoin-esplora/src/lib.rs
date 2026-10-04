//! Esplora-compatible REST HTTP (plain HTTP; TLS via reverse proxy).
//!
//! Wallet clients (exact address/scripthash, tx/block by id, broadcast) and
//! **mempool/electrs HTTP drop-in** (`/internal/*` on unix listen) except
//! address-prefix. Surface: [`COMPAT.md`](../../../COMPAT.md).
//! `GET …/txs/summary` is a mempool.space-shaped compact dialect (not
//! Blockstream Esplora `API.md`). Opt-in `GET /block-template` is GBT.

mod handlers;
mod internal;
mod script_fields;
mod server;
mod tx_json;

pub use script_fields::{esplora_script_fields, EsploraScriptFields};
pub use server::{
    perf_totals, run_esplora, sample_reset_perf, BlockTemplateFn, EsploraConfig, EsploraHandle,
    EsploraListen,
};
