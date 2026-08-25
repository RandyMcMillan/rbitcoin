//! Pin-time spend edges for confirm assemble / write.

/// One spend at pin time: wire prevout + spend/create fks (no second thin rebuild).
#[derive(Clone, Copy, Debug)]
pub struct SpendEdge {
    pub prev_txid: [u8; 32],
    pub vout: u32,
    pub spend_fk: rbitcoin_primitives::Fk,
    pub create_fk: rbitcoin_primitives::Fk,
}
