//! Pure-Rust script / signature verification (no libbitcoinconsensus).
//!
//! Verification is a pure function of `(tx, input_index, prevout TxOut)`.
//! Prevouts are resolved by connect (Class A / same-block / spent_local) — **not** a UTXO set.

mod classify;
pub(crate) mod interpreter;
mod nested;
mod p2pkh;
mod p2tr;
mod p2wpkh;
mod p2wsh;

#[cfg(test)]
mod core_vectors;
#[cfg(test)]
mod tests_verify;

use bitcoin::{Transaction, TxOut};

use crate::block::ScriptCheckJob;
use crate::error::ConsensusError;
use classify::ScriptKind;

pub(crate) use classify::is_anyone_can_spend;

/// Verify every non-anyone-can-spend input of a script job.
///
/// One shared [`bitcoin::sighash::SighashCache`] per tx for typed paths (P2WPKH /
/// P2TR key-path / nested). Interpreter paths own a cache for the script eval so
/// multi-CHECKSIG (multisig) reuses midstate. Signet-heavy path is 1-input txs —
/// that case avoids the multi-input loop overhead.
pub(crate) fn verify_job_all_inputs(job: &ScriptCheckJob) -> Result<(), ConsensusError> {
    use bitcoin::sighash::SighashCache;
    let tx = &job.tx;
    let n = job.prevouts.len();
    if n == 0 {
        return Ok(());
    }
    let mut cache = SighashCache::new(tx);
    if n == 1 {
        return verify_input(job, 0, tx, &mut cache);
    }
    for ii in 0..n {
        verify_input(job, ii, tx, &mut cache)?;
    }
    Ok(())
}

/// Verify one input: classify `scriptPubKey`, then typed path or interpreter.
#[inline]
pub(crate) fn verify_input(
    job: &ScriptCheckJob,
    input_index: usize,
    tx: &Transaction,
    cache: &mut bitcoin::sighash::SighashCache<&Transaction>,
) -> Result<(), ConsensusError> {
    if input_index >= job.prevouts.len() || input_index >= tx.input.len() {
        return Err(ConsensusError::Script("input index".into()));
    }
    let prevout = &job.prevouts[input_index];
    let spk = prevout.script_pubkey.as_script();
    if is_anyone_can_spend(spk) {
        return Ok(());
    }

    match classify::classify(spk) {
        ScriptKind::P2wpkh => p2wpkh::verify(job, input_index, tx, cache),
        ScriptKind::P2pkh => {
            // Fast path: exact `<sig> <pubkey>` scriptSig. Historical mainnet has
            // non-standard P2PKH scriptSigs that still leave a valid stack for
            // scriptPubKey (e.g. height 218596: "p2pkh scriptSig len"). Core always
            // EvalScript(scriptSig)+EvalScript(scriptPubKey) — fall back only for
            // scriptSig *shape* errors (not DER/ECDSA), so bip66 failure codes stay.
            match p2pkh::verify(job, input_index, tx, cache) {
                Ok(()) => Ok(()),
                Err(e) if p2pkh_scriptsig_shape_error(&e) => {
                    verify_bare(job, input_index, tx, prevout)
                }
                Err(e) => Err(e),
            }
        }
        ScriptKind::P2sh => {
            // Pre-BIP16: HASH160/EQUAL is a bare script (push data, hash, equal) —
            // do **not** treat the last push as a redeemScript. Mainnet 170060.
            if !job.bip16_active {
                return verify_bare(job, input_index, tx, prevout);
            }
            // Nested P2SH-P2WPKH / P2SH-P2WSH, or bare redeem via interpreter.
            if let Some(res) = nested::try_p2sh_p2wpkh(job, input_index, tx, cache) {
                return res;
            }
            if let Some(res) = nested::try_p2sh_p2wsh(job, input_index, tx) {
                return res;
            }
            nested::verify_p2sh_legacy(job, input_index, tx)
        }
        ScriptKind::P2wsh => p2wsh::verify(job, input_index, tx),
        ScriptKind::P2tr => p2tr::verify(job, input_index, tx, cache),
        ScriptKind::Bare => verify_bare(job, input_index, tx, prevout),
        ScriptKind::Unknown => Err(ConsensusError::Script("unsupported scriptPubKey".into())),
    }
}

/// True when the P2PKH fast path failed because scriptSig is not exactly two
/// data pushes (still may be valid under full EvalScript like Core).
fn p2pkh_scriptsig_shape_error(err: &ConsensusError) -> bool {
    match err {
        ConsensusError::Script(msg) => {
            msg == "p2pkh scriptSig len"
                || msg == "p2pkh scriptSig"
                || msg == "p2pkh scriptSig op"
                || msg == "p2pkh scriptSig unexpected op"
        }
        _ => false,
    }
}

fn verify_bare(
    job: &ScriptCheckJob,
    input_index: usize,
    tx: &Transaction,
    prevout: &TxOut,
) -> Result<(), ConsensusError> {
    // Core `VerifyScript`: fully **EvalScript(scriptSig)** then **EvalScript(scriptPubKey)**
    // with a shared stack. scriptSig is **not** push-only in consensus for bare spends
    // (SIGPUSHONLY is policy / BIP16-P2SH only). Mainnet block 163685 has bare spends
    // whose scriptSig runs `OP_CODESEPARATOR` + `OP_CHECKMULTISIG` (sig left of codesep;
    // pubkey script after), then a pre-BIP65 `OP_NOP2`/`CLTV`+`DROP` scriptPubKey.
    let input = &tx.input[input_index];
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let ss = input.script_sig.as_script();
    if !ss.as_bytes().is_empty() {
        let ctx_sig = interpreter::EvalContext::new_with_flags(
            tx,
            input_index,
            prevout.value,
            &job.prevouts,
            ss,
            interpreter::SigVersion::Base,
            job.bip65_active,
            job.bip112_active,
            job.bip66_active,
        );
        let _ = interpreter::eval_script(ss, &mut stack, &ctx_sig)?;
    }
    let ctx = interpreter::EvalContext::new_with_flags(
        tx,
        input_index,
        prevout.value,
        &job.prevouts,
        prevout.script_pubkey.as_script(),
        interpreter::SigVersion::Base,
        job.bip65_active,
        job.bip112_active,
        job.bip66_active,
    );
    if interpreter::eval_script(prevout.script_pubkey.as_script(), &mut stack, &ctx)? {
        // Legacy bare: true top (not witness cleanstack).
        interpreter::require_true_top(&stack)?;
    }
    Ok(())
}

/// Shared ECDSA / secp helpers for typed paths.
pub(crate) mod crypto {
    use bitcoin::hashes::Hash;
    use bitcoin::secp256k1::{ecdsa, Message, PublicKey, Secp256k1, VerifyOnly};
    use bitcoin::sighash::EcdsaSighashType;

    use crate::error::ConsensusError;

    thread_local! {
        pub static SECP: Secp256k1<VerifyOnly> = Secp256k1::verification_only();
    }

    /// Parse DER signature + **raw** sighash type byte (as `u32`).
    ///
    /// Important: do **not** run the type through [`EcdsaSighashType::from_consensus`]
    /// before legacy `SignatureHash`. That maps `0 → SIGHASH_ALL(1)`, but mainnet
    /// has historical spends signed with hashtype **0** (block 110300 and others).
    /// Core hashes with the raw byte; we must too.
    ///
    /// Matches Bitcoin Core:
    /// - Always parse with **lax** DER (`ecdsa_signature_parse_der_lax`). Never prefer
    ///   strict `from_der` first: for some pre-BIP66 encodings (e.g. high-bit S without
    ///   `0x00` pad, mainnet block 140493) libsecp `from_der` returns `Ok` with a
    ///   **wrong** (R,S) while `from_der_lax` recovers the OpenSSL-era values that
    ///   actually verify.
    /// - When `strict_der` (BIP66 / `SCRIPT_VERIFY_DERSIG`), enforce
    ///   [`is_valid_signature_encoding`] on the full push (DER + hashtype) *before*
    ///   the lax parse — same split as Core's `CheckSignatureEncoding` + lax verify.
    pub fn parse_der_sig(
        sig_raw: &[u8],
        strict_der: bool,
    ) -> Result<(ecdsa::Signature, u32), ConsensusError> {
        if sig_raw.is_empty() {
            return Err(ConsensusError::Script("empty sig".into()));
        }
        if strict_der && !is_valid_signature_encoding(sig_raw) {
            return Err(ConsensusError::Script("der sig".into()));
        }
        let sighash_ty = sig_raw[sig_raw.len() - 1] as u32;
        let der = &sig_raw[..sig_raw.len() - 1];
        let sig = ecdsa::Signature::from_der_lax(der)
            .map_err(|_| ConsensusError::Script("der sig".into()))?;
        Ok((sig, sighash_ty))
    }

    /// BIP66 / Bitcoin Core `IsValidSignatureEncoding`.
    ///
    /// `sig` is the full scriptSig push including the trailing hashtype byte.
    /// Rejects non-minimal integer encodings (high-bit without `0x00` pad, excess
    /// leading zeros). Used only when BIP66 is active; pre-BIP66 relies on lax parse.
    pub fn is_valid_signature_encoding(sig: &[u8]) -> bool {
        // Format: 0x30 [total-length] 0x02 [R-length] [R] 0x02 [S-length] [S] [sighash]
        // Minimum: 0x30 0x06 0x02 0x01 0x00 0x02 0x01 0x00 [ht] → 9 bytes
        // Maximum: 73 bytes (with 33-byte R/S and hashtype)
        if sig.len() < 9 || sig.len() > 73 {
            return false;
        }
        if sig[0] != 0x30 {
            return false;
        }
        // Length byte covers everything after it except hashtype.
        if sig[1] as usize != sig.len().wrapping_sub(3) {
            return false;
        }
        if sig[2] != 0x02 {
            return false;
        }
        let len_r = sig[3] as usize;
        if len_r == 0 {
            return false;
        }
        if 5 + len_r >= sig.len() {
            return false;
        }
        if sig[4 + len_r] != 0x02 {
            return false;
        }
        let len_s = sig[5 + len_r] as usize;
        if len_s == 0 {
            return false;
        }
        if len_r + len_s + 7 != sig.len() {
            return false;
        }
        // R: not negative; no excessive padding.
        if sig[4] & 0x80 != 0 {
            return false;
        }
        if len_r > 1 && sig[4] == 0x00 && (sig[5] & 0x80) == 0 {
            return false;
        }
        // S: not negative; no excessive padding.
        let s0 = 6 + len_r;
        if sig[s0] & 0x80 != 0 {
            return false;
        }
        if len_s > 1 && sig[s0] == 0x00 && (sig[s0 + 1] & 0x80) == 0 {
            return false;
        }
        true
    }

    /// BIP143 APIs want [`EcdsaSighashType`]; preserve raw `0` via a private path.
    ///
    /// rust-bitcoin's `from_consensus(0)` becomes `All` (`to_u32()==1`). For BIP143
    /// we currently only see standard types; still prefer encoding the raw byte
    /// when the API allows a `u32`.
    #[inline]
    pub fn ecdsa_sighash_type(raw: u32) -> EcdsaSighashType {
        EcdsaSighashType::from_consensus(raw)
    }

    pub fn parse_pubkey(raw: &[u8]) -> Result<PublicKey, ConsensusError> {
        PublicKey::from_slice(raw).map_err(|_| ConsensusError::Script("pubkey".into()))
    }

    /// Verify ECDSA under **Bitcoin consensus** rules.
    ///
    /// libsecp256k1 rejects high-S signatures, but high-S has never been a
    /// consensus failure on Bitcoin (BIP146 unactivated). Bitcoin Core normalizes
    /// S before verify (`ecdsa_signature_normalize`) — we do the same so early
    /// mainnet P2PK spends (e.g. block 183) accept.
    pub fn verify_ecdsa(msg_bytes: [u8; 32], sig: &ecdsa::Signature, pubkey: &PublicKey) -> bool {
        let msg = Message::from_digest(msg_bytes);
        let mut normalized = *sig;
        normalized.normalize_s();
        SECP.with(|secp| secp.verify_ecdsa(&msg, &normalized, pubkey).is_ok())
    }

    pub fn hash160(data: &[u8]) -> [u8; 20] {
        use bitcoin::hashes::hash160;
        *hash160::Hash::hash(data).as_byte_array()
    }

    pub fn sha256(data: &[u8]) -> [u8; 32] {
        use bitcoin::hashes::sha256;
        *sha256::Hash::hash(data).as_byte_array()
    }

    pub fn sha1(data: &[u8]) -> [u8; 20] {
        use bitcoin_hashes::Hash as _;
        *bitcoin_hashes::sha1::Hash::hash(data).as_byte_array()
    }
}
