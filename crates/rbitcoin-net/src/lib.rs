//! Bitcoin P2P: BIP324 v2 transport, headers/blocks, tip follow, tip-mode **tx relay**.

mod asmap;
mod cache;
mod chain;
mod codec;
mod compact;
mod ephemeral;
mod error;
mod eviction;
mod fee_history;
mod fee_history_file;
mod i2p_sam;
mod ibd;
mod most_work;
mod msg_decode;
mod net_permissions;
mod netaddr;
mod netgroup;
mod peer;
mod peer_dos;
mod peers;
mod perf_meter;
mod reactor;
mod seeds;
mod serve_perf;
mod service;
mod socks;
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
pub use ephemeral::spawn_isolated_broadcast_loop;
pub use error::NetError;
pub use eviction::{select_inbound_eviction, InboundEvictCandidate};
pub use i2p_sam::I2Psam;
pub use ibd::{
    connect_timeout_for, format_tip_perf_sizes, read_platform_rss, rehydrate_block_queue_residue,
    IbdConfig, ProcessRss, TipPerfSizes, DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER, DEFAULT_IBD_WINDOW,
    is_bad_prev_err, read_platform_rss as read_proc_rss, ProcessRss as ProcRss,
};
pub use most_work::{sum_work, work_better, WorkOverflow};
pub use net_permissions::{
    apply_implicit, parse_whitebind, parse_whitelist, NetPermTable, NetPermissionFlags,
    WhitebindGrant, WhitelistGrant, DEFAULT_WHITELISTFORCERELAY, DEFAULT_WHITELISTRELAY,
};
pub use netaddr::{is_cjdns_ip, NetAddr, OnlyNet};
pub use netgroup::netgroup;
pub use peer::{
    addrv2_message_size_log, advertising_address_log, connected_to_self_log,
    desirable_service_flags, drain_pending_now, expected_services_disconnect_log,
    feeler_connection_completed_log, flush_tx_invs, force_announce_txid,
    has_all_desirable_service_flags, local_service_flags, local_service_flags_pruned,
    non_version_before_handshake_log, obsolete_version_log, ping_prior_to_verack_log,
    run_feeler_timed, sendaddrv2_after_verack_log, set_compact_filters_service,
    unsupported_before_verack_log, version_handshake_timeout_log, PendingBlocks, V2PlainSession,
    BAN_SCORE_THRESHOLD, HANDSHAKE_TIMEOUT, MAX_ADDR_TO_SEND, MAX_PCT_ADDR_TO_SEND,
    MAX_SERVE_BLOCKS, MIN_PEER_PROTO_VERSION,
};
pub use peer_dos::{
    DEFAULT_MAX_BYTES_PER_SEC, DEFAULT_MAX_INBOUND, DEFAULT_MAX_MSGS_PER_SEC, OVERSIZE_BAN_SCORE,
    RATE_LIMIT_BAN_SCORE,
};
pub use peers::{
    connection_counts, outbound_time_offset, parse_peer_addr, parse_peer_addr_with_port,
    parse_peer_net, pick_stale_follow_evict, DialRequest, DialTarget, LivePeer, PeerConnType,
    PeerHub, PeerInfo, PeerOut, PingAction,
};
pub use perf_meter::RequestMeter;
pub(crate) use rbitcoin_mempool::MempoolGraphStats;
pub use rbitcoin_mempool::{AcceptError, Selected};
pub use reactor::BlockingRegion;
pub use seeds::{
    default_port, default_rpc_port, dns_seed_query_host, dns_seeds, fixed_seed_hosts,
    required_seed_services, resolve_all_seeds, resolve_dns_seeds, resolve_fixed_seeds,
    seed_lookup_names, socks_dns_seed_dests, AddrMan, PeerEntry, PeerFlags, MAX_ADDR_MAN,
};
pub use serve_perf::{
    format_serve_perf, sample_reset_serve_perf, serve_perf_totals, ServePerfSample,
};
pub use service::P2PNode;
pub use socks::{install_i2p_dialer, Dialer};
pub use tx_relay::{
    ElectrumMempoolItem, FeeHistoryBackfillStats, MempoolAnnounce, MempoolHub, MempoolPerfSample,
    MempoolTxSnapEntry, MempoolTxSnapshot,
};
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
