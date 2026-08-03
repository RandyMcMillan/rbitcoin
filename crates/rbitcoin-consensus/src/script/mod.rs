//! Pure-Rust script / signature verification (no libbitcoinconsensus).
//!
//! Verification is a pure function of `(tx, input_index, prevout TxOut)`.
//! Prevouts are resolved by connect (wave / light UTXO create_fk /
//! same-block) — not a full coins cache.

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
///
/// On failure, [`ConsensusError::Script`] messages are annotated with `txid=` and
/// `vin=` so IBD logs name the failing spend (batch-first height alone is not enough).
pub(crate) fn verify_job_all_inputs(job: &ScriptCheckJob) -> Result<(), ConsensusError> {
    use bitcoin::sighash::SighashCache;
    let tx = &job.tx;
    let n = job.prevouts.len();
    if n == 0 {
        return Ok(());
    }
    let mut cache = SighashCache::new(tx);
    if n == 1 {
        return verify_input(job, 0, tx, &mut cache)
            .map_err(|e| annotate_script_err(e, tx, 0));
    }
    for ii in 0..n {
        verify_input(job, ii, tx, &mut cache).map_err(|e| annotate_script_err(e, tx, ii))?;
    }
    Ok(())
}

/// Append `txid=… vin=…` to script errors for operator diagnosis.
fn annotate_script_err(err: ConsensusError, tx: &Transaction, input_index: usize) -> ConsensusError {
    match err {
        ConsensusError::Script(msg) if !msg.contains("txid=") => {
            let txid = tx.compute_txid();
            ConsensusError::Script(format!("{msg} txid={txid} vin={input_index}"))
        }
        other => other,
    }
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
        ScriptKind::P2tr => {
            // Pre-activation: witness v1 is anyone-can-spend (BIP141 unknown version).
            if !job.taproot_active {
                return Ok(());
            }
            p2tr::verify(job, input_index, tx, cache)
        }
        ScriptKind::Bare => verify_bare(job, input_index, tx, prevout),
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

    pub fn parse_pubkey(raw: &[u8]) -> Result<PublicKey, ConsensusError> {
        PublicKey::from_slice(raw).map_err(|_| ConsensusError::Script("pubkey".into()))
    }

    /// BIP143 signature hash with **raw** `nHashType` (last byte of the sig push).
    ///
    /// `script_code` is the BIP143 scriptCode (for P2WPKH: the
    /// `OP_DUP OP_HASH160 <keyhash> OP_EQUALVERIFY OP_CHECKSIG` template; for
    /// P2WSH: the witness script).
    pub fn bip143_signature_hash(
        tx: &bitcoin::Transaction,
        input_index: usize,
        script_code: &bitcoin::Script,
        amount: bitcoin::Amount,
        raw_ty: u32,
    ) -> Result<[u8; 32], ConsensusError> {
        use bitcoin::consensus::Encodable;
        use bitcoin::hashes::{sha256d, Hash, HashEngine};
        use bitcoin::sighash::EcdsaSighashType;

        if input_index >= tx.input.len() {
            return Err(ConsensusError::Script("bip143 input index".into()));
        }
        use EcdsaSighashType::*;
        let mapped = EcdsaSighashType::from_consensus(raw_ty);
        // `split_anyonecanpay_flag` is crate-private in rust-bitcoin 0.32.
        let anyone_can_pay = matches!(
            mapped,
            AllPlusAnyoneCanPay | NonePlusAnyoneCanPay | SinglePlusAnyoneCanPay
        );
        let base = match mapped {
            None | NonePlusAnyoneCanPay => None,
            Single | SinglePlusAnyoneCanPay => Single,
            _ => All,
        };
        let zero = [0u8; 32];

        let hash_prevouts: [u8; 32] = if !anyone_can_pay {
            let mut eng = sha256d::Hash::engine();
            for i in &tx.input {
                i.previous_output
                    .consensus_encode(&mut eng)
                    .map_err(|_| ConsensusError::Script("bip143 prevouts".into()))?;
            }
            sha256d::Hash::from_engine(eng).to_byte_array()
        } else {
            zero
        };

        let hash_sequence: [u8; 32] = if !anyone_can_pay && base != Single && base != None {
            let mut eng = sha256d::Hash::engine();
            for i in &tx.input {
                i.sequence
                    .consensus_encode(&mut eng)
                    .map_err(|_| ConsensusError::Script("bip143 sequences".into()))?;
            }
            sha256d::Hash::from_engine(eng).to_byte_array()
        } else {
            zero
        };

        let hash_outputs: [u8; 32] = if base != Single && base != None {
            let mut eng = sha256d::Hash::engine();
            for o in &tx.output {
                o.consensus_encode(&mut eng)
                    .map_err(|_| ConsensusError::Script("bip143 outputs".into()))?;
            }
            sha256d::Hash::from_engine(eng).to_byte_array()
        } else if base == Single && input_index < tx.output.len() {
            let mut eng = sha256d::Hash::engine();
            tx.output[input_index]
                .consensus_encode(&mut eng)
                .map_err(|_| ConsensusError::Script("bip143 single output".into()))?;
            sha256d::Hash::from_engine(eng).to_byte_array()
        } else {
            zero
        };

        let mut eng = sha256d::Hash::engine();
        tx.version
            .consensus_encode(&mut eng)
            .map_err(|_| ConsensusError::Script("bip143 version".into()))?;
        eng.input(&hash_prevouts);
        eng.input(&hash_sequence);
        {
            let txin = &tx.input[input_index];
            txin.previous_output
                .consensus_encode(&mut eng)
                .map_err(|_| ConsensusError::Script("bip143 outpoint".into()))?;
            script_code
                .consensus_encode(&mut eng)
                .map_err(|_| ConsensusError::Script("bip143 scriptCode".into()))?;
            amount
                .consensus_encode(&mut eng)
                .map_err(|_| ConsensusError::Script("bip143 amount".into()))?;
            txin.sequence
                .consensus_encode(&mut eng)
                .map_err(|_| ConsensusError::Script("bip143 nSequence".into()))?;
        }
        eng.input(&hash_outputs);
        tx.lock_time
            .consensus_encode(&mut eng)
            .map_err(|_| ConsensusError::Script("bip143 locktime".into()))?;
        // Core: raw nHashType as uint32 LE — not the normalized enum value.
        eng.input(&raw_ty.to_le_bytes());
        Ok(sha256d::Hash::from_engine(eng).to_byte_array())
    }

    /// P2WPKH BIP143 hash: `script_pubkey` is native spk **or** nested redeem (`00 14 <20>`).
    ///
    /// Fast path: when `raw_ty` round-trips through [`EcdsaSighashType`] (standard
    /// 0x01/02/03/81/82/83), use rust-bitcoin's [`SighashCache`] midstate
    /// (hashPrevouts/hashSequence/hashOutputs once per tx). Slow path: non-standard
    /// raw types (e.g. mainnet `0x65`) need our raw-uint32 encoder.
    pub fn bip143_p2wpkh_signature_hash(
        tx: &bitcoin::Transaction,
        input_index: usize,
        script_pubkey: &bitcoin::Script,
        amount: bitcoin::Amount,
        raw_ty: u32,
        cache: &mut bitcoin::sighash::SighashCache<&bitcoin::Transaction>,
    ) -> Result<[u8; 32], ConsensusError> {
        let mapped = EcdsaSighashType::from_consensus(raw_ty);
        if mapped.to_u32() == raw_ty {
            return cache
                .p2wpkh_signature_hash(input_index, script_pubkey, amount, mapped)
                .map(|h| h.to_byte_array())
                .map_err(|_| ConsensusError::Script("p2wpkh sighash".into()));
        }
        let script_code = script_pubkey
            .p2wpkh_script_code()
            .ok_or_else(|| ConsensusError::Script("bip143 not p2wpkh".into()))?;
        bip143_signature_hash(tx, input_index, script_code.as_script(), amount, raw_ty)
    }

    /// P2WSH / tapscript-free WitnessV0 BIP143 with the same fast/slow split.
    pub fn bip143_p2wsh_signature_hash(
        tx: &bitcoin::Transaction,
        input_index: usize,
        witness_script: &bitcoin::Script,
        amount: bitcoin::Amount,
        raw_ty: u32,
        cache: &mut bitcoin::sighash::SighashCache<&bitcoin::Transaction>,
    ) -> Result<[u8; 32], ConsensusError> {
        let mapped = EcdsaSighashType::from_consensus(raw_ty);
        if mapped.to_u32() == raw_ty {
            return cache
                .p2wsh_signature_hash(input_index, witness_script, amount, mapped)
                .map(|h| h.to_byte_array())
                .map_err(|_| ConsensusError::Script("p2wsh sighash".into()));
        }
        bip143_signature_hash(tx, input_index, witness_script, amount, raw_ty)
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

    #[cfg(test)]
    mod der_tests {
        use super::*;

        fn valid_der_sig() -> Vec<u8> {
            // Minimal valid-shaped DER + SIGHASH_ALL
            // 30 06 02 01 01 02 01 01 01
            vec![0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01, 0x01]
        }

        #[test]
        fn bip66_encoding_edge_cases() {
            assert!(!is_valid_signature_encoding(&[]));
            assert!(!is_valid_signature_encoding(&[0x30; 8])); // too short
            assert!(!is_valid_signature_encoding(&vec![0x30; 74])); // too long
            let mut bad = valid_der_sig();
            bad[0] = 0x31;
            assert!(!is_valid_signature_encoding(&bad));
            // wrong total length byte
            let mut bad = valid_der_sig();
            bad[1] = 0x05;
            assert!(!is_valid_signature_encoding(&bad));
            // R not INT
            let mut bad = valid_der_sig();
            bad[2] = 0x03;
            assert!(!is_valid_signature_encoding(&bad));
            // zero-length R
            assert!(!is_valid_signature_encoding(&[
                0x30, 0x06, 0x02, 0x00, 0x02, 0x01, 0x01, 0x01
            ]));
            // R high bit set (negative)
            let neg = vec![0x30, 0x07, 0x02, 0x01, 0x80, 0x02, 0x01, 0x01, 0x01];
            assert!(!is_valid_signature_encoding(&neg));
            // excess leading zero on R
            let pad = vec![
                0x30, 0x08, 0x02, 0x02, 0x00, 0x01, 0x02, 0x01, 0x01, 0x01,
            ];
            assert!(!is_valid_signature_encoding(&pad));
            // S high bit
            let sneg = vec![0x30, 0x07, 0x02, 0x01, 0x01, 0x02, 0x01, 0x80, 0x01];
            assert!(!is_valid_signature_encoding(&sneg));
            // excess leading zero on S
            let spad = vec![
                0x30, 0x08, 0x02, 0x01, 0x01, 0x02, 0x02, 0x00, 0x01, 0x01,
            ];
            assert!(!is_valid_signature_encoding(&spad));
            // zero-length S
            assert!(!is_valid_signature_encoding(&[
                0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x00, 0x01
            ]));
            // S marker wrong
            let mut sm = valid_der_sig();
            sm[4] = 0x03; // after R len=1 at [3]=1, S tag at index 4
            // structure: [0]=30 [1]=06 [2]=02 [3]=01 [4]=R [5]=02 [6]=01 [7]=S [8]=ht
            // Actually valid_der is 30 06 02 01 01 02 01 01 01 so S tag at [5]
            let mut sm = valid_der_sig();
            sm[5] = 0x03;
            assert!(!is_valid_signature_encoding(&sm));

            assert!(is_valid_signature_encoding(&valid_der_sig()));
            let _ = (neg, pad, sneg, spad);
        }

        #[test]
        fn parse_der_empty_and_strict() {
            assert!(parse_der_sig(&[], true).is_err());
            // non-BIP66 but lax may still fail parse — just exercise strict reject
            let bad = vec![0x30, 0x01, 0x00];
            assert!(parse_der_sig(&bad, true).is_err());
        }
    }
}

#[cfg(test)]
mod verify_routing_tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
    };
    use crate::block::ScriptCheckJob;

    #[test]
    fn empty_prevouts_and_index_errors() {
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let job = ScriptCheckJob {
            txid: [0u8; 32],
            prevouts: vec![],
            tx: tx.clone(),
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        assert!(verify_job_all_inputs(&job).is_ok());

        let job2 = ScriptCheckJob {
            txid: [0u8; 32],
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
            tx: Transaction {
                version: bitcoin::transaction::Version::TWO,
                lock_time: LockTime::ZERO,
                input: vec![],
                output: vec![],
            },
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        let mut cache = bitcoin::sighash::SighashCache::new(&job2.tx);
        assert!(verify_input(&job2, 0, &job2.tx, &mut cache).is_err());
    }

    #[test]
    fn annotate_preserves_non_script_and_existing_txid() {
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([2; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let e = annotate_script_err(ConsensusError::MissingPrevout, &tx, 0);
        assert!(matches!(e, ConsensusError::MissingPrevout));
        let e2 = annotate_script_err(
            ConsensusError::Script("already txid=abc vin=0".into()),
            &tx,
            3,
        );
        match e2 {
            ConsensusError::Script(m) => assert!(m.contains("already txid=")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn taproot_inactive_is_anyone_can_spend() {
        let mut spk = vec![0x51, 0x20];
        spk.extend([0u8; 32]);
        let job = ScriptCheckJob {
            txid: [0u8; 32],
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(spk),
            }],
            tx: Transaction {
                version: bitcoin::transaction::Version::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint {
                        txid: bitcoin::Txid::from_byte_array([3; 32]),
                        vout: 0,
                    },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(1),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            },
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: false,
        };
        verify_job_all_inputs(&job).expect("pre-taproot v1 ACS");
    }

    #[test]
    fn p2pkh_shape_error_detection() {
        assert!(p2pkh_scriptsig_shape_error(&ConsensusError::Script(
            "p2pkh scriptSig len".into()
        )));
        assert!(p2pkh_scriptsig_shape_error(&ConsensusError::Script(
            "p2pkh scriptSig".into()
        )));
        assert!(p2pkh_scriptsig_shape_error(&ConsensusError::Script(
            "p2pkh scriptSig op".into()
        )));
        assert!(p2pkh_scriptsig_shape_error(&ConsensusError::Script(
            "p2pkh scriptSig unexpected op".into()
        )));
        assert!(!p2pkh_scriptsig_shape_error(&ConsensusError::Script(
            "p2pkh ecdsa".into()
        )));
        assert!(!p2pkh_scriptsig_shape_error(&ConsensusError::MissingPrevout));
    }

    #[test]
    fn bip143_sighash_arms_single_acp_and_p2wsh() {
        use bitcoin::script::Script;
        use bitcoin::Amount;

        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![
                TxIn {
                    previous_output: OutPoint {
                        txid: bitcoin::Txid::from_byte_array([1; 32]),
                        vout: 0,
                    },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                },
                TxIn {
                    previous_output: OutPoint {
                        txid: bitcoin::Txid::from_byte_array([2; 32]),
                        vout: 1,
                    },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::from_consensus(1),
                    witness: Witness::new(),
                },
            ],
            output: vec![
                TxOut {
                    value: Amount::from_sat(1000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                },
                TxOut {
                    value: Amount::from_sat(2000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x52]),
                },
            ],
        };
        let sc = Script::from_bytes(&[0x51]);
        let amt = Amount::from_sat(50_000);

        // Bad index.
        assert!(crypto::bip143_signature_hash(&tx, 9, sc, amt, 0x01).is_err());

        // All / Single / None + AnyoneCanPay variants.
        for ty in [0x01u32, 0x02, 0x03, 0x81, 0x82, 0x83] {
            let h = crypto::bip143_signature_hash(&tx, 0, sc, amt, ty).expect("sighash");
            assert_ne!(h, [0u8; 32]);
        }
        // Single at index with matching output (input 1 → output 1).
        let h_single = crypto::bip143_signature_hash(&tx, 1, sc, amt, 0x03).unwrap();
        // Single with no matching output (only 2 outs; index 1 ok; use 1-input for zero outputs arm).
        let tx1 = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![tx.input[0].clone()],
            output: vec![],
        };
        let h_none_out = crypto::bip143_signature_hash(&tx1, 0, sc, amt, 0x03).unwrap();
        assert_ne!(h_single, h_none_out);

        // Non-standard raw type 0x65 → slow path for p2wsh.
        let mut cache = bitcoin::sighash::SighashCache::new(&tx);
        let wscript = Script::from_bytes(&[0x51]);
        let h_fast = crypto::bip143_p2wsh_signature_hash(
            &tx, 0, wscript, amt, 0x01, &mut cache,
        )
        .unwrap();
        let h_slow = crypto::bip143_p2wsh_signature_hash(
            &tx, 0, wscript, amt, 0x65, &mut cache,
        )
        .unwrap();
        assert_ne!(h_fast, [0u8; 32]);
        assert_ne!(h_slow, [0u8; 32]);
        // Standard and non-standard types must differ (raw_ty in digest).
        assert_ne!(h_fast, h_slow);
    }

    #[test]
    fn bip16_inactive_treats_p2sh_as_bare() {
        // P2SH template spent as bare HASH160/EQUAL when bip16 off.
        let mut p2sh = vec![0xa9, 0x14];
        p2sh.extend([0u8; 20]);
        p2sh.push(0x87);
        // scriptSig that just pushes 20 zero bytes (equal fails — but exercises bare path)
        let mut ss = vec![0x14];
        ss.extend([0u8; 20]);
        let job = ScriptCheckJob {
            txid: [0u8; 32],
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(p2sh),
            }],
            tx: Transaction {
                version: bitcoin::transaction::Version::ONE,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint {
                        txid: bitcoin::Txid::from_byte_array([4; 32]),
                        vout: 0,
                    },
                    script_sig: ScriptBuf::from_bytes(ss),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(1),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            },
            bip65_active: false,
            bip112_active: false,
            bip66_active: false,
            bip16_active: false,
            taproot_active: false,
        };
        // Bare HASH160 equal of zeros vs hash160([]) — should fail script, not p2sh redeem.
        let err = verify_job_all_inputs(&job).unwrap_err();
        assert!(matches!(err, ConsensusError::Script(_)));
    }
}
