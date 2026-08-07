//! Transaction **policy** (relay / mempool admission), separate from block consensus.
//!
//! Policy checks may reject mempool acceptance. They must **never** be invoked
//! from block connect / confirm paths.
//!
//! # Libre-relay-class admission (plan §3.1)
//!
//! Reject only consensus failure, DoS resource limits, and reserved upgrade hooks.
//! Defaults: **0.1 sat/vB** min relay, **no dust limit**, full RBF, Libre annex.

use bitcoin::script::Script;
use bitcoin::Transaction;

/// Minimum relay feerate: **0.1 sat/vB** = 100 sat/kvB.
pub const MIN_RELAY_FEE_RATE_SAT_PER_KVB: u64 = 100;

/// Absolute weight cap for a single transaction (4_000_000 = block weight).
pub const MAX_STANDARD_TX_WEIGHT: u64 = 400_000;

/// Result of a policy check (not consensus).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyResult {
    Standard,
    NonStandard(&'static str),
}

impl PolicyResult {
    pub fn is_standard(&self) -> bool {
        matches!(self, PolicyResult::Standard)
    }

    pub fn is_ok(&self) -> bool {
        self.is_standard()
    }
}

/// True if `script` is push-only (BIP62 / IsPushOnly).
pub fn is_push_only(script: &Script) -> bool {
    use bitcoin::script::Instruction;
    for ins in script.instructions() {
        match ins {
            Ok(Instruction::PushBytes(_)) => {}
            Ok(Instruction::Op(op)) => {
                let n = op.to_u8();
                // OP_0 and OP_1..OP_16 are push-like.
                if n == 0x00 || (0x4f..=0x60).contains(&n) {
                    continue;
                }
                return false;
            }
            Err(_) => return false,
        }
    }
    true
}

/// Coarse **Core-style** standardness of a scriptPubKey (not used for Libre admit).
pub fn is_standard_script_pubkey(script: &Script) -> PolicyResult {
    let b = script.as_bytes();
    if b.is_empty() {
        return PolicyResult::NonStandard("empty scriptPubKey");
    }
    if b == [0x51] {
        return PolicyResult::NonStandard("op_true");
    }
    if (b.len() == 22 && b[0] == 0x00 && b[1] == 0x14)
        || (b.len() == 34 && b[0] == 0x00 && b[1] == 0x20)
        || (b.len() == 34 && b[0] == 0x51 && b[1] == 0x20)
        || (b.len() == 25 && b[0] == 0x76 && b[1] == 0xa9)
        || (b.len() == 23 && b[0] == 0xa9 && b[1] == 0x14)
    {
        return PolicyResult::Standard;
    }
    PolicyResult::NonStandard("nonstandard scriptPubKey")
}

/// Lightweight whole-tx **Core-style** standardness stub.
pub fn check_tx_standard(tx: &Transaction) -> PolicyResult {
    for inp in &tx.input {
        if !inp.previous_output.is_null() && !is_push_only(inp.script_sig.as_script()) {
            return PolicyResult::NonStandard("scriptSig not push-only");
        }
    }
    for out in &tx.output {
        let r = is_standard_script_pubkey(out.script_pubkey.as_script());
        if !r.is_standard() {
            let b = out.script_pubkey.as_bytes();
            if !b.is_empty() && b[0] == 0x6a {
                continue;
            }
            return r;
        }
    }
    PolicyResult::Standard
}

// ── Libre-relay-class admission ─────────────────────────────────────────────

/// Virtual size in vbytes: `(weight + 3) / 4`.
#[inline]
pub fn get_virtual_size(weight: u64) -> u64 {
    weight.saturating_add(3) / 4
}

/// True if `fee_sat` meets the minimum relay feerate for `weight` (WU).
///
/// Uses integer compare: `fee * 1000 >= vsize * MIN_RELAY_FEE_RATE_SAT_PER_KVB`.
pub fn meets_min_relay_fee(fee_sat: u64, weight: u64) -> bool {
    let vsize = get_virtual_size(weight);
    if vsize == 0 {
        return false;
    }
    fee_sat.saturating_mul(1000) >= vsize.saturating_mul(MIN_RELAY_FEE_RATE_SAT_PER_KVB)
}

/// Feerate in sat/kvB for diagnostics (floors).
pub fn fee_rate_sat_per_kvb(fee_sat: u64, weight: u64) -> u64 {
    let vsize = get_virtual_size(weight);
    if vsize == 0 {
        return 0;
    }
    fee_sat.saturating_mul(1000) / vsize
}

/// Libre annex rule ([`IsAnnexStandard`](https://github.com/bitcoin/bitcoin) Libre Relay):
///
/// - No annex / empty payload after `0x50` tag → OK  
/// - Non-empty annex only if the **first data byte after the tag is `0x00`**  
///
/// `annex` is the full witness stack element (including leading `0x50` when present).
pub fn is_annex_standard(annex: &[u8]) -> bool {
    if annex.is_empty() {
        return true;
    }
    // Not tagged as annex — not our concern here.
    if annex[0] != 0x50 {
        return true;
    }
    // Tag only, or first data byte is 0x00.
    annex.len() == 1 || annex[1] == 0x00
}

/// Scan all inputs' witnesses for a BIP341 annex and apply Libre annex rule.
pub fn check_libre_annex(tx: &Transaction) -> PolicyResult {
    for inp in &tx.input {
        let stack = &inp.witness.to_vec();
        if let Some(last) = stack.last() {
            if !last.is_empty() && last[0] == 0x50 && !is_annex_standard(last) {
                return PolicyResult::NonStandard("libre annex");
            }
        }
    }
    PolicyResult::Standard
}

/// Libre admission for a single tx given fee and weight (no dust, no template ban).
///
/// Callers still enforce consensus, cluster limits, and DoS caps separately.
pub fn check_libre_admission(tx: &Transaction, fee_sat: u64, weight: u64) -> PolicyResult {
    if tx.is_coinbase() {
        return PolicyResult::NonStandard("coinbase");
    }
    if tx.input.is_empty() {
        return PolicyResult::NonStandard("no inputs");
    }
    if tx.output.is_empty() {
        return PolicyResult::NonStandard("no outputs");
    }
    if weight > MAX_STANDARD_TX_WEIGHT {
        return PolicyResult::NonStandard("tx weight");
    }
    if !meets_min_relay_fee(fee_sat, weight) {
        return PolicyResult::NonStandard("min relay fee");
    }
    check_libre_annex(tx)
    // Dust: intentionally not enforced (Libre).
    // Script templates / bare multisig / large OP_RETURN: allowed.
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version;
    use bitcoin::{OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Witness};

    #[test]
    fn push_only_and_templates() {
        let mut push = vec![0x00, 0x14];
        push.extend_from_slice(&[0u8; 20]);
        assert!(is_push_only(ScriptBuf::from_bytes(push).as_script()));
        assert!(!is_push_only(ScriptBuf::from_bytes(vec![0x76]).as_script()));
        let p2wpkh = {
            let mut v = vec![0x00, 0x14];
            v.extend_from_slice(&[0u8; 20]);
            ScriptBuf::from_bytes(v)
        };
        assert!(is_standard_script_pubkey(p2wpkh.as_script()).is_standard());
        assert!(
            !is_standard_script_pubkey(ScriptBuf::from_bytes(vec![0x51]).as_script()).is_standard()
        );
    }

    #[test]
    fn min_relay_fee_point_one_sat_vb() {
        // 1000 vB → weight 4000; 0.1 sat/vB → 100 sat min.
        assert!(meets_min_relay_fee(100, 4000));
        assert!(!meets_min_relay_fee(99, 4000));
        assert_eq!(fee_rate_sat_per_kvb(100, 4000), 100);
    }

    #[test]
    fn annex_libre_rules() {
        assert!(is_annex_standard(&[]));
        assert!(is_annex_standard(&[0x50]));
        assert!(is_annex_standard(&[0x50, 0x00]));
        assert!(is_annex_standard(&[0x50, 0x00, 0xab]));
        assert!(!is_annex_standard(&[0x50, 0x01]));
        assert!(!is_annex_standard(&[0x50, 0xff, 0x00]));
    }

    fn bare_tx(fee_out: u64) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([1u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::from_sat(fee_out),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]), // OP_TRUE — dust-ish / nonstd OK under Libre
            }],
        }
    }

    #[test]
    fn libre_allows_op_true_and_dust_outputs() {
        // weight of bare_tx is small; fee = 50_000 - 1 = large enough.
        let tx = bare_tx(1);
        let weight = tx.weight().to_wu();
        // pretend input value 50_000
        let fee = 50_000u64.saturating_sub(1);
        assert!(check_libre_admission(&tx, fee, weight).is_ok());
    }

    #[test]
    fn libre_rejects_low_feerate() {
        let tx = bare_tx(50_000);
        let weight = tx.weight().to_wu();
        assert_eq!(
            check_libre_admission(&tx, 0, weight),
            PolicyResult::NonStandard("min relay fee")
        );
    }

    #[test]
    fn libre_rejects_bad_annex() {
        let mut tx = bare_tx(1);
        tx.input[0].witness = Witness::from_slice(&[vec![0x01], vec![0x50, 0x01]]);
        let weight = tx.weight().to_wu();
        assert_eq!(
            check_libre_admission(&tx, 50_000, weight),
            PolicyResult::NonStandard("libre annex")
        );
    }

    #[test]
    fn policy_result_is_ok_alias() {
        assert!(PolicyResult::Standard.is_ok());
        assert!(!PolicyResult::NonStandard("x").is_ok());
    }

    #[test]
    fn push_only_allows_op_n_and_rejects_decode_error() {
        // OP_1NEGATE (0x4f) and OP_1 (0x51) are push-like.
        assert!(is_push_only(
            ScriptBuf::from_bytes(vec![0x4f, 0x51]).as_script()
        ));
        // Truncated push: instruction decode fails → not push-only.
        assert!(!is_push_only(
            ScriptBuf::from_bytes(vec![0x02, 0xaa]).as_script()
        ));
        // OP_CHECKSIG is not push-only.
        assert!(!is_push_only(ScriptBuf::from_bytes(vec![0xac]).as_script()));
    }

    #[test]
    fn standard_script_pubkey_templates_and_rejects() {
        assert_eq!(
            is_standard_script_pubkey(ScriptBuf::new().as_script()),
            PolicyResult::NonStandard("empty scriptPubKey")
        );
        // P2WSH
        let mut p2wsh = vec![0x00, 0x20];
        p2wsh.extend([0u8; 32]);
        assert!(is_standard_script_pubkey(ScriptBuf::from_bytes(p2wsh).as_script()).is_standard());
        // P2TR
        let mut p2tr = vec![0x51, 0x20];
        p2tr.extend([0u8; 32]);
        assert!(is_standard_script_pubkey(ScriptBuf::from_bytes(p2tr).as_script()).is_standard());
        // P2PKH
        let mut p2pkh = vec![0x76, 0xa9, 0x14];
        p2pkh.extend([0u8; 20]);
        p2pkh.extend([0x88, 0xac]);
        assert!(is_standard_script_pubkey(ScriptBuf::from_bytes(p2pkh).as_script()).is_standard());
        // P2SH
        let mut p2sh = vec![0xa9, 0x14];
        p2sh.extend([0u8; 20]);
        p2sh.push(0x87);
        assert!(is_standard_script_pubkey(ScriptBuf::from_bytes(p2sh).as_script()).is_standard());
        // Bare OP_NOP
        assert_eq!(
            is_standard_script_pubkey(ScriptBuf::from_bytes(vec![0x61]).as_script()),
            PolicyResult::NonStandard("nonstandard scriptPubKey")
        );
    }

    #[test]
    fn check_tx_standard_scriptsig_and_op_return() {
        // Non-push scriptSig on non-coinbase → NonStandard.
        let mut tx = bare_tx(1);
        tx.input[0].script_sig = ScriptBuf::from_bytes(vec![0xac]);
        assert_eq!(
            check_tx_standard(&tx),
            PolicyResult::NonStandard("scriptSig not push-only")
        );
        // OP_RETURN outputs allowed even when nonstandard template.
        let mut ok = bare_tx(1);
        ok.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0x6a, 0x01, 0xff]);
        assert!(check_tx_standard(&ok).is_standard());
        // Empty OP_TRUE was already nonstandard under Core-style; OP_TRUE alone rejected.
        let bad = bare_tx(1);
        assert!(!check_tx_standard(&bad).is_standard());
    }

    #[test]
    fn vsize_fee_zero_and_libre_gates() {
        assert!(!meets_min_relay_fee(1, 0));
        assert_eq!(fee_rate_sat_per_kvb(100, 0), 0);
        assert_eq!(get_virtual_size(1), 1);

        // Coinbase rejected.
        let cb = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![0x00, 0x01]),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::from_sat(50),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        assert_eq!(
            check_libre_admission(&cb, 0, 100),
            PolicyResult::NonStandard("coinbase")
        );

        // No inputs / no outputs.
        let mut no_in = bare_tx(1);
        no_in.input.clear();
        assert_eq!(
            check_libre_admission(&no_in, 1000, 100),
            PolicyResult::NonStandard("no inputs")
        );
        let mut no_out = bare_tx(1);
        no_out.output.clear();
        assert_eq!(
            check_libre_admission(&no_out, 1000, 100),
            PolicyResult::NonStandard("no outputs")
        );

        // Weight cap.
        let tx = bare_tx(1);
        assert_eq!(
            check_libre_admission(&tx, 1_000_000, MAX_STANDARD_TX_WEIGHT + 1),
            PolicyResult::NonStandard("tx weight")
        );

        // Annex not tagged 0x50 is ignored by is_annex_standard.
        assert!(is_annex_standard(&[0x01, 0x02]));
        // Libre annex scan: non-annex last item is fine.
        let mut ok = bare_tx(1);
        ok.input[0].witness = Witness::from_slice(&[vec![0x01], vec![0x02]]);
        assert!(check_libre_annex(&ok).is_standard());
    }
}
