//! Bitcoin P2P: BIP324 v2 transport, headers/blocks, tip follow, tip-mode **tx relay**.

mod asmap;
mod cache;
mod chain;
mod codec;
mod compact;
mod error;
mod eviction;
mod ibd;
mod most_work;
mod msg_decode;
mod netgroup;
mod peer;
mod peer_dos;
mod peers;
mod reactor;
mod seeds;
mod serve_perf;
mod service;
mod tip_accept;
mod tx_relay;
mod v2;
mod versionbits_warn;

pub use asmap::{interpret, ip16_for_lookup, sanity_check, AsMap, TWO_PREFIX_ASMAP};
pub use cache::BlockCache;
pub use chain::{headers_download_timeout_secs, AcceptOutcome, ChainHub, ChainTipInfo, TipEvent};
pub use compact::{
    classify_v2_cmpct_peer, prefilled_indexes_ok, shortid_map_from_txs, try_reconstruct,
    CmpctPeerFrame,
};
pub use error::NetError;
pub use eviction::{select_inbound_eviction, InboundEvictCandidate};
pub use ibd::{
    format_tip_perf_sizes, is_bad_prev_err, read_proc_rss, rehydrate_block_queue_residue,
    IbdConfig, ProcRss, TipPerfSizes, DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER, DEFAULT_IBD_WINDOW,
};
pub use most_work::{sum_work, work_better};
pub use netgroup::netgroup;
pub use peer::{
    addrv2_message_size_log, advertising_address_log, connected_to_self_log,
    desirable_service_flags, drain_pending_now, expected_services_disconnect_log,
    feeler_connection_completed_log, flush_tx_invs, force_announce_txid,
    has_all_desirable_service_flags, local_service_flags, non_version_before_handshake_log,
    obsolete_version_log, ping_prior_to_verack_log, run_feeler_timed, sendaddrv2_after_verack_log,
    unsupported_before_verack_log, version_handshake_timeout_log, PendingBlocks, V2PlainSession,
    BAN_SCORE_THRESHOLD, HANDSHAKE_TIMEOUT, MAX_ADDR_TO_SEND, MAX_PCT_ADDR_TO_SEND,
    MAX_SERVE_BLOCKS, MIN_PEER_PROTO_VERSION,
};
pub use peer_dos::{
    DEFAULT_MAX_BYTES_PER_SEC, DEFAULT_MAX_INBOUND, DEFAULT_MAX_MSGS_PER_SEC, OVERSIZE_BAN_SCORE,
    RATE_LIMIT_BAN_SCORE,
};
pub use peers::{
    parse_peer_addr, pick_stale_follow_evict, DialRequest, LivePeer, PeerConnType, PeerHub,
    PeerInfo, PeerOut, PingAction,
};
pub use rbitcoin_mempool::AcceptError;
pub(crate) use rbitcoin_mempool::MempoolGraphStats;
pub use reactor::BlockingRegion;
pub use seeds::{
    default_port, dns_seed_query_host, dns_seeds, fixed_seed_hosts, required_seed_services,
    resolve_all_seeds, resolve_dns_seeds, resolve_fixed_seeds, seed_lookup_names, AddrMan,
    PeerEntry, PeerFlags, MAX_ADDR_MAN,
};
pub use serve_perf::{format_serve_perf, sample_reset_serve_perf, ServePerfSample};
pub use service::P2PNode;
pub use tx_relay::{ElectrumMempoolItem, MempoolAnnounce, MempoolHub, MempoolPerfSample};
pub use v2::{encode_v2_contents, parse_v2_regtest, parse_v2_regtest_named, WireBytes};
pub use versionbits_warn::{
    active_unknown_bits, unknown_rules_warning, warn_period_threshold, warning_strings,
};

/// Default number of **live download peers** during IBD (`IbdConfig::target_peers`
/// and node `--max-outbound` default).
///
/// This is **not** the seed candidate pool size. The node dials a larger sample
/// of seed addresses (typically `2 × target`, clamped) so failed connects still
/// leave enough live peers.
pub const DEFAULT_IBD_TARGET_PEERS: u32 = 16;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_defaults() {
        assert_eq!(DEFAULT_IBD_TARGET_PEERS, 16);
    }
}
