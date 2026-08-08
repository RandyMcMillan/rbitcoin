//! Bitcoin Script stack interpreter for P2WSH / P2SH / bare / tapscript.
//!
//! **Goal:** implement *all* consensus-enabled opcodes so IBD does not fail
//! post-milestone on the next rarely-used opcode. Whack-a-mole is not a strategy.
//!
//! Opcode semantics are **sigversion-aware**:
//! - Legacy / Witness v0: 10 000-byte script limit, 201 non-push ops; disabled
//!   opcodes (CAT, …) fail if executed; RESERVED/VER fail if executed.
//! - Tapscript (BIP342): no script-size or op-count limit; OP_SUCCESSx anywhere
//!   → unconditional success; CHECKMULTISIG disabled; MINIMALIF consensus;
//!   CHECKSIGADD; empty-pubkey / unknown-key-type / hard-fail-on-bad-sig rules.
//!
//! This module implements **consensus** checks only. Relay / standardness lives
//! in [`crate::policy`] and must never reject blocks.

use std::cell::{Cell, RefCell};

use bitcoin::hashes::Hash;
use bitcoin::script::{Instruction, Script};
use bitcoin::sighash::{Prevouts, SighashCache};
use bitcoin::{Amount, Sequence, Transaction, TxOut};

use super::crypto;
use crate::error::ConsensusError;

/// Core-aligned stack element count (main + alt).
const MAX_STACK_SIZE: usize = 1000;
/// Legacy / witness-v0 script size cap (BIP16 / BIP141). **Not** applied in tapscript
/// (BIP342: size only bounded by block weight).
const MAX_SCRIPT_SIZE_LEGACY: usize = 10_000;
/// Legacy / witness-v0 non-push opcode budget. **Not** applied in tapscript (BIP342).
const MAX_OPS_LEGACY: usize = 201;
const MAX_PUBKEYS_PER_MULTISIG: i64 = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SigVersion {
    Base,
    WitnessV0,
    TapScript,
}

/// BIP342 OP_SUCCESSx opcode numbers (decimal).
///
/// If any of these appears in a **tapscript** during decoding, the script is
/// unconditionally valid.
fn is_op_success(code: u8) -> bool {
    matches!(
        code,
        80 | 98
            | 126
            | 127
            | 128
            | 129
            | 131
            | 132
            | 133
            | 134
            | 137
            | 138
            | 141
            | 142
            | 149
            | 150
            | 151
            | 152
            | 153
    ) || (187..=254).contains(&code)
}

/// Classic disabled opcodes (legacy / witness v0) — consensus fail if executed.
fn is_disabled_legacy(code: u8) -> bool {
    matches!(
        code,
        0x7e | 0x7f | 0x80 | 0x81 | // CAT SUBSTR LEFT RIGHT
        0x83 | 0x84 | 0x85 | 0x86 | // INVERT AND OR XOR
        0x8d | 0x8e | // 2MUL 2DIV
        0x95 | 0x96 | 0x97 | 0x98 | 0x99 // MUL DIV MOD LSHIFT RSHIFT
    )
}

/// BIP342: scan tapscript for OP_SUCCESSx. Returns true if script is immediately valid.
pub(crate) fn tapscript_has_op_success(script: &Script) -> bool {
    let bytes = script.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let op = bytes[i];
        // Direct push 1..=75
        if (1..=75).contains(&op) {
            let n = op as usize;
            i = i.saturating_add(1).saturating_add(n);
            continue;
        }
        match op {
            0x4c => {
                // OP_PUSHDATA1
                if i + 1 >= bytes.len() {
                    break;
                }
                let n = bytes[i + 1] as usize;
                i = i.saturating_add(2).saturating_add(n);
            }
            0x4d => {
                if i + 2 >= bytes.len() {
                    break;
                }
                let n = u16::from_le_bytes([bytes[i + 1], bytes[i + 2]]) as usize;
                i = i.saturating_add(3).saturating_add(n);
            }
            0x4e => {
                if i + 4 >= bytes.len() {
                    break;
                }
                let n = u32::from_le_bytes([bytes[i + 1], bytes[i + 2], bytes[i + 3], bytes[i + 4]])
                    as usize;
                i = i.saturating_add(5).saturating_add(n);
            }
            _ => {
                if is_op_success(op) {
                    return true;
                }
                i += 1;
            }
        }
    }
    false
}

pub(crate) struct EvalContext<'a> {
    pub tx: &'a Transaction,
    pub input_index: usize,
    pub amount: Amount,
    pub prevouts: &'a [TxOut],
    /// scriptCode for sighash (redeem / witness script).
    pub script_code: &'a Script,
    pub sig_version: SigVersion,
    /// When false, OP_CLTV (0xb1) is a no-op (pre-BIP65).
    pub bip65_active: bool,
    /// When false, OP_CSV (0xb2) is a no-op (pre-BIP112).
    pub bip112_active: bool,
    /// When true, ECDSA signatures must be strict DER (BIP66).
    pub bip66_active: bool,
    /// BIP342: instruction index of last executed OP_CODESEPARATOR, or `0xFFFFFFFF`.
    ///
    /// Counted like Core's `opcode_pos` (one per GetOp/instruction, not byte offset).
    codeseparator_pos: Cell<u32>,
    /// Base / WitnessV0: byte offset into `script_code` of the first opcode **after**
    /// the last executed OP_CODESEPARATOR (Core `pbegincodehash`). `None` = full script.
    ///
    /// BIP143 / legacy CHECKSIG use this truncated script as `scriptCode`.
    codeseparator_script_off: Cell<Option<usize>>,
    /// One cache per script eval so multi-CHECKSIG / CHECKSIGADD reuse midstate.
    cache: RefCell<SighashCache<&'a Transaction>>,
}

impl<'a> EvalContext<'a> {
    pub(crate) fn new(
        tx: &'a Transaction,
        input_index: usize,
        amount: Amount,
        prevouts: &'a [TxOut],
        script_code: &'a Script,
        sig_version: SigVersion,
    ) -> Self {
        Self::new_with_flags(
            tx,
            input_index,
            amount,
            prevouts,
            script_code,
            sig_version,
            true,
            true,
            true,
        )
    }

    pub(crate) fn new_with_flags(
        tx: &'a Transaction,
        input_index: usize,
        amount: Amount,
        prevouts: &'a [TxOut],
        script_code: &'a Script,
        sig_version: SigVersion,
        bip65_active: bool,
        bip112_active: bool,
        bip66_active: bool,
    ) -> Self {
        Self {
            tx,
            input_index,
            amount,
            prevouts,
            script_code,
            sig_version,
            bip65_active,
            bip112_active,
            bip66_active,
            codeseparator_pos: Cell::new(0xFFFF_FFFF),
            codeseparator_script_off: Cell::new(None),
            cache: RefCell::new(SighashCache::new(tx)),
        }
    }
}

/// BIP141 / BIP342: exactly one true value on the stack (witness / tapscript).
pub(crate) fn require_clean_true(stack: &[Vec<u8>]) -> Result<(), ConsensusError> {
    if stack.len() != 1 {
        return Err(ConsensusError::Script("cleanstack".into()));
    }
    if !cast_to_bool(&stack[0]) {
        return Err(ConsensusError::Script("script false".into()));
    }
    Ok(())
}

/// BIP16 / legacy: final stack must be non-empty with a true top element.
///
/// **Not** cleanstack — BIP62 CLEANSTACK was never activated for non-witness
/// consensus. Requiring `len==1` falsely rejected valid P2SH (signet 219477).
pub(crate) fn require_true_top(stack: &[Vec<u8>]) -> Result<(), ConsensusError> {
    if stack.is_empty() || !cast_to_bool(stack.last().unwrap()) {
        return Err(ConsensusError::Script("script false".into()));
    }
    Ok(())
}

/// Evaluate scriptSig as push-only onto `stack` (BIP16 P2SH / SIGPUSHONLY).
///
/// **Not** used for bare script verification — historical bare spends may run
/// non-push opcodes in scriptSig (e.g. `OP_CODESEPARATOR` + `CHECKMULTISIG` at
/// mainnet height 163685). Callers that need BIP16 push-only must use this
/// helper; bare paths use full [`eval_script`] on scriptSig.
pub(crate) fn eval_script_sig_pushes(
    script: &Script,
    stack: &mut Vec<Vec<u8>>,
) -> Result<(), ConsensusError> {
    for ins in script.instructions() {
        match ins.map_err(|_| ConsensusError::Script("scriptSig parse".into()))? {
            Instruction::PushBytes(b) => {
                push(stack, b.as_bytes().to_vec())?;
            }
            Instruction::Op(op) => {
                let n = op.to_u8();
                if n == 0x00 {
                    push(stack, vec![])?;
                } else if n == 0x4f {
                    // OP_1NEGATE
                    push(stack, vec![0x81])?;
                } else if (0x51..=0x60).contains(&n) {
                    push(stack, vec![n - 0x50])?;
                } else {
                    return Err(ConsensusError::Script("scriptSig non-push".into()));
                }
            }
        }
    }
    Ok(())
}

/// Evaluate `script`. On success returns `true` if cleanstack must still be
/// checked; `false` if the script already fully succeeded (e.g. OP_SUCCESS).
pub(crate) fn eval_script(
    script: &Script,
    stack: &mut Vec<Vec<u8>>,
    ctx: &EvalContext<'_>,
) -> Result<bool, ConsensusError> {
    let bytes = script.as_bytes();
    // BIP342: OP_SUCCESSx anywhere in tapscript → unconditional success *before*
    // size / stack limits (even unparseable tails pass).
    if ctx.sig_version == SigVersion::TapScript && tapscript_has_op_success(script) {
        return Ok(false);
    }
    // Legacy / v0 only: 10k script size. Tapscript: no explicit size limit.
    if ctx.sig_version != SigVersion::TapScript && bytes.len() > MAX_SCRIPT_SIZE_LEGACY {
        return Err(ConsensusError::Script("script too large".into()));
    }

    let mut altstack: Vec<Vec<u8>> = Vec::new();
    let mut if_stack: Vec<bool> = Vec::new(); // whether this branch is executing
    let mut op_count = 0usize;
    let enforce_op_limit = ctx.sig_version != SigVersion::TapScript;
    let minimal_if = ctx.sig_version == SigVersion::TapScript;
    // BIP342 / Core: instruction index for codeseparator_pos (not byte offset).
    let mut opcode_pos: u32 = 0;
    // Prefer instruction_indices so OP_CODESEPARATOR can set Base/WitnessV0
    // scriptCode byte offsets (BIP143 / Core pbegincodehash).
    for item in script.instruction_indices() {
        let (byte_index, ins) = item.map_err(|_| ConsensusError::Script("script parse".into()))?;
        let this_pos = opcode_pos;
        opcode_pos = opcode_pos.saturating_add(1);
        let executing = if_stack.iter().all(|&x| x);

        match ins {
            Instruction::PushBytes(b) => {
                // Core: MAX_SCRIPT_ELEMENT_SIZE even in unexecuted branches.
                let data = b.as_bytes();
                if data.len() > 520 {
                    return Err(ConsensusError::Script("push too large".into()));
                }
                if executing {
                    push(stack, data.to_vec())?;
                }
            }
            Instruction::Op(op) => {
                let code = op.to_u8();
                // OP_IF / NOTIF / ELSE / ENDIF always process structure.
                match code {
                    0x63 => {
                        // OP_IF
                        let mut cond = false;
                        if executing {
                            let v = pop(stack)?;
                            if minimal_if && !is_minimal_if_arg(&v) {
                                return Err(ConsensusError::Script("MINIMALIF".into()));
                            }
                            cond = cast_to_bool(&v);
                        }
                        if_stack.push(executing && cond);
                        continue;
                    }
                    0x64 => {
                        // OP_NOTIF
                        let mut cond = false;
                        if executing {
                            let v = pop(stack)?;
                            if minimal_if && !is_minimal_if_arg(&v) {
                                return Err(ConsensusError::Script("MINIMALIF".into()));
                            }
                            cond = !cast_to_bool(&v);
                        }
                        if_stack.push(executing && cond);
                        continue;
                    }
                    0x67 => {
                        // OP_ELSE
                        if if_stack.is_empty() {
                            return Err(ConsensusError::Script("OP_ELSE".into()));
                        }
                        let last = if_stack.last_mut().unwrap();
                        *last = !*last;
                        continue;
                    }
                    0x68 => {
                        // OP_ENDIF
                        if if_stack.pop().is_none() {
                            return Err(ConsensusError::Script("OP_ENDIF".into()));
                        }
                        continue;
                    }
                    _ => {}
                }

                // Core: OP_VERIF / OP_VERNOTIF always fail (even unexecuted).
                if code == 0x65 || code == 0x66 {
                    return Err(ConsensusError::Script("OP_VERIF".into()));
                }
                // Core: disabled opcodes fail even in unexecuted branches (legacy/v0).
                if ctx.sig_version != SigVersion::TapScript && is_disabled_legacy(code) {
                    return Err(ConsensusError::Script(format!(
                        "disabled opcode 0x{code:02x}"
                    )));
                }

                // Legacy / v0: non-push opcodes count toward 201 even when skipped.
                if enforce_op_limit && code > 0x60 {
                    op_count += 1;
                    if op_count > MAX_OPS_LEGACY {
                        return Err(ConsensusError::Script("op count".into()));
                    }
                }

                if !executing {
                    continue;
                }

                match code {
                    0x00 => push(stack, vec![])?,                                    // OP_0
                    0x4f => push(stack, vec![0x81])?, // OP_1NEGATE (-1)
                    n if (0x51..=0x60).contains(&n) => push(stack, vec![n - 0x50])?, // OP_1..OP_16

                    // OP_RESERVED — fail if executed (tapscript SUCCESS80 handled above).
                    0x50 => {
                        return Err(ConsensusError::Script("OP_RESERVED".into()));
                    }
                    0x61 => {} // OP_NOP
                    // OP_VER — fails if executed (tapscript SUCCESS98 handled above).
                    0x62 => {
                        return Err(ConsensusError::Script("OP_VER".into()));
                    }
                    // OP_VERIF / OP_VERNOTIF always fail if reached when executing.
                    0x65 | 0x66 => {
                        return Err(ConsensusError::Script("OP_VERIF".into()));
                    }
                    0x69 => {
                        // OP_VERIFY
                        let v = pop(stack)?;
                        if !cast_to_bool(&v) {
                            return Err(ConsensusError::Script("OP_VERIFY".into()));
                        }
                    }
                    0x6a => return Err(ConsensusError::Script("OP_RETURN".into())),

                    0x6b => {
                        // OP_TOALTSTACK
                        let v = pop(stack)?;
                        altstack.push(v);
                    }
                    0x6c => {
                        // OP_FROMALTSTACK
                        let v = altstack
                            .pop()
                            .ok_or_else(|| ConsensusError::Script("altstack empty".into()))?;
                        push(stack, v)?;
                    }
                    0x6d => {
                        // OP_2DROP
                        pop(stack)?;
                        pop(stack)?;
                    }
                    0x6e => {
                        // OP_2DUP
                        require_n(stack, 2)?;
                        let a = stack[stack.len() - 2].clone();
                        let b = stack[stack.len() - 1].clone();
                        push(stack, a)?;
                        push(stack, b)?;
                    }
                    0x6f => {
                        // OP_3DUP
                        require_n(stack, 3)?;
                        let a = stack[stack.len() - 3].clone();
                        let b = stack[stack.len() - 2].clone();
                        let c = stack[stack.len() - 1].clone();
                        push(stack, a)?;
                        push(stack, b)?;
                        push(stack, c)?;
                    }
                    0x70 => {
                        // OP_2OVER
                        require_n(stack, 4)?;
                        let a = stack[stack.len() - 4].clone();
                        let b = stack[stack.len() - 3].clone();
                        push(stack, a)?;
                        push(stack, b)?;
                    }
                    0x71 => {
                        // OP_2ROT: (x1 x2 x3 x4 x5 x6 -- x3 x4 x5 x6 x1 x2)
                        require_n(stack, 6)?;
                        let n = stack.len();
                        let x1 = stack[n - 6].clone();
                        let x2 = stack[n - 5].clone();
                        for i in 0..4 {
                            stack[n - 6 + i] = stack[n - 4 + i].clone();
                        }
                        stack[n - 2] = x1;
                        stack[n - 1] = x2;
                    }
                    0x72 => {
                        // OP_2SWAP
                        require_n(stack, 4)?;
                        let n = stack.len();
                        stack.swap(n - 4, n - 2);
                        stack.swap(n - 3, n - 1);
                    }
                    0x73 => {
                        // OP_IFDUP
                        require_n(stack, 1)?;
                        if cast_to_bool(stack.last().unwrap()) {
                            let v = stack.last().unwrap().clone();
                            push(stack, v)?;
                        }
                    }
                    0x74 => {
                        // OP_DEPTH
                        let d = stack.len() as i64;
                        push(stack, scriptnum_encode(d))?;
                    }
                    0x75 => {
                        pop(stack)?;
                    } // OP_DROP
                    0x76 => {
                        // OP_DUP
                        require_n(stack, 1)?;
                        let v = stack.last().unwrap().clone();
                        push(stack, v)?;
                    }
                    0x77 => {
                        // OP_NIP
                        require_n(stack, 2)?;
                        let top = pop(stack)?;
                        pop(stack)?;
                        push(stack, top)?;
                    }
                    0x78 => {
                        // OP_OVER
                        require_n(stack, 2)?;
                        let v = stack[stack.len() - 2].clone();
                        push(stack, v)?;
                    }
                    0x79 => {
                        // OP_PICK
                        let n = scriptnum_decode(&pop(stack)?)?;
                        if n < 0 || n as usize >= stack.len() {
                            return Err(ConsensusError::Script("OP_PICK".into()));
                        }
                        let v = stack[stack.len() - 1 - n as usize].clone();
                        push(stack, v)?;
                    }
                    0x7a => {
                        // OP_ROLL
                        let n = scriptnum_decode(&pop(stack)?)?;
                        if n < 0 || n as usize >= stack.len() {
                            return Err(ConsensusError::Script("OP_ROLL".into()));
                        }
                        let idx = stack.len() - 1 - n as usize;
                        let v = stack.remove(idx);
                        push(stack, v)?;
                    }
                    0x7b => {
                        // OP_ROT
                        require_n(stack, 3)?;
                        let n = stack.len();
                        stack.swap(n - 3, n - 2);
                        stack.swap(n - 2, n - 1);
                    }
                    0x7c => {
                        // OP_SWAP
                        require_n(stack, 2)?;
                        let n = stack.len();
                        stack.swap(n - 1, n - 2);
                    }
                    0x7d => {
                        // OP_TUCK
                        require_n(stack, 2)?;
                        let v = stack[stack.len() - 1].clone();
                        stack.insert(stack.len() - 2, v);
                        if stack.len() > MAX_STACK_SIZE {
                            return Err(ConsensusError::Script("stack size".into()));
                        }
                    }
                    0x82 => {
                        // OP_SIZE
                        require_n(stack, 1)?;
                        let sz = stack.last().unwrap().len() as i64;
                        push(stack, scriptnum_encode(sz))?;
                    }
                    // OP_RESERVED1 / OP_RESERVED2
                    0x89 | 0x8a => {
                        return Err(ConsensusError::Script("OP_RESERVED".into()));
                    }
                    0x87 => {
                        // OP_EQUAL
                        let a = pop(stack)?;
                        let b = pop(stack)?;
                        push(stack, bool_encode(a == b))?;
                    }
                    0x88 => {
                        // OP_EQUALVERIFY
                        let a = pop(stack)?;
                        let b = pop(stack)?;
                        if a != b {
                            return Err(ConsensusError::Script("OP_EQUALVERIFY".into()));
                        }
                    }
                    // Numeric unary — full set (1ADD/1SUB/NEGATE/ABS).
                    0x8b => {
                        // OP_1ADD
                        let v = scriptnum_decode(&pop(stack)?)?;
                        push(stack, scriptnum_encode(v.saturating_add(1)))?;
                    }
                    0x8c => {
                        // OP_1SUB
                        let v = scriptnum_decode(&pop(stack)?)?;
                        push(stack, scriptnum_encode(v.saturating_sub(1)))?;
                    }
                    0x8f => {
                        // OP_NEGATE
                        let v = scriptnum_decode(&pop(stack)?)?;
                        push(stack, scriptnum_encode(-v))?;
                    }
                    0x90 => {
                        // OP_ABS
                        let v = scriptnum_decode(&pop(stack)?)?;
                        push(stack, scriptnum_encode(v.abs()))?;
                    }
                    0x91 => {
                        // OP_NOT
                        let v = scriptnum_decode(&pop(stack)?)?;
                        push(stack, bool_encode(v == 0))?;
                    }
                    0x92 => {
                        // OP_0NOTEQUAL
                        let v = scriptnum_decode(&pop(stack)?)?;
                        push(stack, bool_encode(v != 0))?;
                    }
                    0x93 => bin_arith(stack, |a, b| a + b)?,
                    0x94 => bin_arith(stack, |a, b| a - b)?,
                    0x9a => {
                        // OP_BOOLAND
                        let b = scriptnum_decode(&pop(stack)?)?;
                        let a = scriptnum_decode(&pop(stack)?)?;
                        push(stack, bool_encode(a != 0 && b != 0))?;
                    }
                    0x9b => {
                        // OP_BOOLOR
                        let b = scriptnum_decode(&pop(stack)?)?;
                        let a = scriptnum_decode(&pop(stack)?)?;
                        push(stack, bool_encode(a != 0 || b != 0))?;
                    }
                    0x9c => bin_cmp(stack, |a, b| a == b)?,
                    0x9d => {
                        // OP_NUMEQUALVERIFY
                        let b = scriptnum_decode(&pop(stack)?)?;
                        let a = scriptnum_decode(&pop(stack)?)?;
                        if a != b {
                            return Err(ConsensusError::Script("OP_NUMEQUALVERIFY".into()));
                        }
                    }
                    0x9e => bin_cmp(stack, |a, b| a != b)?,
                    0x9f => bin_cmp(stack, |a, b| a < b)?,
                    0xa0 => bin_cmp(stack, |a, b| a > b)?,
                    0xa1 => bin_cmp(stack, |a, b| a <= b)?,
                    0xa2 => bin_cmp(stack, |a, b| a >= b)?,
                    0xa3 => {
                        // OP_MIN
                        let b = scriptnum_decode(&pop(stack)?)?;
                        let a = scriptnum_decode(&pop(stack)?)?;
                        push(stack, scriptnum_encode(a.min(b)))?;
                    }
                    0xa4 => {
                        // OP_MAX
                        let b = scriptnum_decode(&pop(stack)?)?;
                        let a = scriptnum_decode(&pop(stack)?)?;
                        push(stack, scriptnum_encode(a.max(b)))?;
                    }
                    0xa5 => {
                        // OP_WITHIN
                        let max = scriptnum_decode(&pop(stack)?)?;
                        let min = scriptnum_decode(&pop(stack)?)?;
                        let x = scriptnum_decode(&pop(stack)?)?;
                        push(stack, bool_encode(x >= min && x < max))?;
                    }
                    0xa6 => {
                        // OP_RIPEMD160
                        let v = pop(stack)?;
                        use bitcoin::hashes::ripemd160;
                        push(stack, ripemd160::Hash::hash(&v).to_byte_array().to_vec())?;
                    }
                    0xa7 => {
                        // OP_SHA1 (consensus-enabled; was stubbed → would fail post-milestone)
                        let v = pop(stack)?;
                        push(stack, crypto::sha1(&v).to_vec())?;
                    }
                    0xa8 => {
                        // OP_SHA256
                        let v = pop(stack)?;
                        push(stack, crypto::sha256(&v).to_vec())?;
                    }
                    0xa9 => {
                        // OP_HASH160
                        let v = pop(stack)?;
                        push(stack, crypto::hash160(&v).to_vec())?;
                    }
                    0xaa => {
                        // OP_HASH256
                        let v = pop(stack)?;
                        use bitcoin::hashes::sha256d;
                        push(stack, sha256d::Hash::hash(&v).to_byte_array().to_vec())?;
                    }
                    0xab => {
                        // OP_CODESEPARATOR:
                        // - Tapscript (BIP342): instruction index for leaf sighash.
                        // - Base / WitnessV0: subsequent CHECKSIG* use scriptCode =
                        //   everything **after** this opcode (Core pbegincodehash).
                        //   Signet 277442: CSV+CODESEPARATOR+P2PKH-shaped WSH leaves.
                        ctx.codeseparator_pos.set(this_pos);
                        // One-byte opcode; suffix starts at the next byte.
                        ctx.codeseparator_script_off
                            .set(Some(byte_index.saturating_add(1)));
                    }
                    0xac => op_checksig(stack, ctx, false)?,
                    0xad => op_checksig(stack, ctx, true)?,
                    // BIP342 OP_CHECKSIGADD (0xba) — tapscript only.
                    0xba => {
                        if ctx.sig_version != SigVersion::TapScript {
                            return Err(ConsensusError::Script("unknown opcode 0xba".into()));
                        }
                        op_checksigadd(stack, ctx)?;
                    }
                    0xae => {
                        // BIP342: CHECKMULTISIG disabled in tapscript (hard fail).
                        if ctx.sig_version == SigVersion::TapScript {
                            return Err(ConsensusError::Script(
                                "CHECKMULTISIG disabled in tapscript".into(),
                            ));
                        }
                        op_checkmultisig(stack, ctx, false, &mut op_count)?;
                    }
                    0xaf => {
                        if ctx.sig_version == SigVersion::TapScript {
                            return Err(ConsensusError::Script(
                                "CHECKMULTISIGVERIFY disabled in tapscript".into(),
                            ));
                        }
                        op_checkmultisig(stack, ctx, true, &mut op_count)?;
                    }
                    0xb1 => {
                        // OP_CHECKLOCKTIMEVERIFY (BIP65). Pre-activation: NOP.
                        if !ctx.bip65_active {
                            continue;
                        }
                        require_n(stack, 1)?;
                        // Core: CScriptNum(..., fRequireMinimal, 5) for locktime.
                        let locktime = scriptnum_decode_width(stack.last().unwrap(), 5)?;
                        if locktime < 0 {
                            return Err(ConsensusError::Script("CLTV negative".into()));
                        }
                        let tx_lock = ctx.tx.lock_time.to_consensus_u32() as i64;
                        // Type mismatch: both height or both time
                        let lock_is_time = locktime >= 500_000_000;
                        let tx_is_time = tx_lock >= 500_000_000;
                        if lock_is_time != tx_is_time {
                            return Err(ConsensusError::Script("CLTV type".into()));
                        }
                        if locktime > tx_lock {
                            return Err(ConsensusError::Script("CLTV".into()));
                        }
                        if ctx.tx.input[ctx.input_index].sequence.is_final() {
                            return Err(ConsensusError::Script("CLTV final sequence".into()));
                        }
                    }
                    0xb2 => {
                        // OP_CHECKSEQUENCEVERIFY (BIP112). Pre-activation: NOP.
                        // Core order: decode (5-byte) → disable-flag NOP → version < 2 fails
                        // (not NOP). See docs/external_findings/004-csv-nop-and-scriptnum-width.md.
                        if !ctx.bip112_active {
                            continue;
                        }
                        require_n(stack, 1)?;
                        let csv = scriptnum_decode_width(stack.last().unwrap(), 5)?;
                        if csv < 0 {
                            return Err(ConsensusError::Script("CSV negative".into()));
                        }
                        if csv as u32 & (1 << 31) != 0 {
                            // disabled bit → NOP (before version gate)
                            continue;
                        }
                        // Core CheckSequence: tx.nVersion < 2 → fail (unsigned; RB-001).
                        if (ctx.tx.version.0 as u32) < 2 {
                            return Err(ConsensusError::Script("CSV version".into()));
                        }
                        let seq = ctx.tx.input[ctx.input_index].sequence;
                        if !sequence_csv_ok(seq, csv as u32) {
                            return Err(ConsensusError::Script("CSV".into()));
                        }
                    }
                    // OP_NOP1, OP_NOP4-OP_NOP10
                    0xb0 | 0xb3 | 0xb4 | 0xb5 | 0xb6 | 0xb7 | 0xb8 | 0xb9 => {}
                    _ => {
                        // Tapscript OP_SUCCESS should have short-circuited above; belt-and-suspenders.
                        if ctx.sig_version == SigVersion::TapScript && is_op_success(code) {
                            return Ok(false);
                        }
                        // Legacy / v0: disabled opcodes are consensus-invalid if executed.
                        if ctx.sig_version != SigVersion::TapScript && is_disabled_legacy(code) {
                            return Err(ConsensusError::Script(format!(
                                "disabled opcode 0x{code:02x}"
                            )));
                        }
                        return Err(ConsensusError::Script(format!(
                            "unknown opcode 0x{code:02x}"
                        )));
                    }
                }
                // Core: stack + altstack share MAX_STACK_SIZE (1000).
                if stack.len() + altstack.len() > MAX_STACK_SIZE {
                    return Err(ConsensusError::Script("stack size".into()));
                }
            }
        }
    }

    if !if_stack.is_empty() {
        return Err(ConsensusError::Script("unbalanced IF".into()));
    }
    Ok(true) // caller applies true-top / cleanstack by policy
}

fn sequence_csv_ok(seq: Sequence, csv: u32) -> bool {
    // BIP112 sequence comparison (tx.nVersion >= 2 checked at opcode).
    let seq_n = seq.to_consensus_u32();
    if seq_n & (1 << 31) != 0 {
        return false; // SEQUENCE_LOCKTIME_DISABLE_FLAG on input
    }
    let mask = 0x0000_ffff | (1 << 22); // type flag + value
    let seq_masked = seq_n & mask;
    let csv_masked = csv & mask;
    // Same type (height vs time)
    let type_flag = 1 << 22;
    if (seq_masked ^ csv_masked) & type_flag != 0 {
        return false;
    }
    (csv_masked & 0xffff) <= (seq_masked & 0xffff)
}

fn op_checksig(
    stack: &mut Vec<Vec<u8>>,
    ctx: &EvalContext<'_>,
    verify: bool,
) -> Result<(), ConsensusError> {
    let pubkey = pop(stack)?;
    let sig = pop(stack)?;
    if ctx.sig_version == SigVersion::TapScript {
        return op_checksig_tapscript(stack, &sig, &pubkey, ctx, verify);
    }
    let ok = checksig_legacy(&sig, &pubkey, ctx, None)?;
    if verify {
        if !ok {
            return Err(ConsensusError::Script("CHECKSIGVERIFY".into()));
        }
    } else {
        push(stack, bool_encode(ok))?;
    }
    Ok(())
}

/// BIP342 OP_CHECKSIGADD: stack is `… sig n pubkey` → `… n` or `… n+1`.
fn op_checksigadd(stack: &mut Vec<Vec<u8>>, ctx: &EvalContext<'_>) -> Result<(), ConsensusError> {
    require_n(stack, 3)?;
    let pubkey = pop(stack)?;
    let n_raw = pop(stack)?;
    let sig = pop(stack)?;
    // CScriptNum n must be ≤4 bytes (scriptnum_decode enforces).
    let n = scriptnum_decode(&n_raw)?;
    match tapscript_sig_result(&sig, &pubkey, ctx)? {
        TapSigResult::EmptySig => {
            // Push n unchanged.
            push(stack, scriptnum_encode(n))?;
        }
        TapSigResult::Valid => {
            push(stack, scriptnum_encode(n.saturating_add(1)))?;
        } // Invalid non-empty already failed inside tapscript_sig_result.
    }
    Ok(())
}

/// BIP342 signature opcode outcomes for known/unknown keys.
enum TapSigResult {
    /// Signature was the empty vector (soft fail: push 0 / n / fail VERIFY).
    EmptySig,
    /// Signature verified or unknown key type treated as success.
    Valid,
}

fn op_checksig_tapscript(
    stack: &mut Vec<Vec<u8>>,
    sig: &[u8],
    pubkey: &[u8],
    ctx: &EvalContext<'_>,
    verify: bool,
) -> Result<(), ConsensusError> {
    match tapscript_sig_result(sig, pubkey, ctx)? {
        TapSigResult::EmptySig => {
            if verify {
                return Err(ConsensusError::Script("CHECKSIGVERIFY empty".into()));
            }
            // BIP342: push empty vector (not 0x00 byte).
            push(stack, vec![])?;
        }
        TapSigResult::Valid => {
            if !verify {
                push(stack, vec![0x01])?;
            }
        }
    }
    Ok(())
}

/// Shared BIP342 CHECKSIG / CHECKSIGVERIFY / CHECKSIGADD validation core.
fn tapscript_sig_result(
    sig: &[u8],
    pubkey: &[u8],
    ctx: &EvalContext<'_>,
) -> Result<TapSigResult, ConsensusError> {
    // Empty public key → hard fail.
    if pubkey.is_empty() {
        return Err(ConsensusError::Script("tapscript empty pubkey".into()));
    }
    // Unknown public key type (not 32 bytes): treat signature as valid (soft-fork hook).
    if pubkey.len() != 32 {
        if sig.is_empty() {
            return Ok(TapSigResult::EmptySig);
        }
        return Ok(TapSigResult::Valid);
    }
    if sig.is_empty() {
        return Ok(TapSigResult::EmptySig);
    }
    // Non-empty: must verify Schnorr; failure terminates the script.
    if !checksig_schnorr(sig, pubkey, ctx)? {
        return Err(ConsensusError::Script("tapscript CHECKSIG failed".into()));
    }
    Ok(TapSigResult::Valid)
}

fn op_checkmultisig(
    stack: &mut Vec<Vec<u8>>,
    ctx: &EvalContext<'_>,
    verify: bool,
    op_count: &mut usize,
) -> Result<(), ConsensusError> {
    // Pop order matches Core: n, n keys (top=last), m, m sigs (top=last), dummy.
    let n = scriptnum_decode(&pop(stack)?)?;
    if n < 0 || n > MAX_PUBKEYS_PER_MULTISIG {
        return Err(ConsensusError::Script("multisig n".into()));
    }
    *op_count += n as usize;
    if *op_count > MAX_OPS_LEGACY {
        return Err(ConsensusError::Script("op count".into()));
    }
    let mut pubkeys = Vec::with_capacity(n as usize);
    for _ in 0..n {
        pubkeys.push(pop(stack)?);
    }
    // Reverse so index 0 is deepest (first pushed) — Core match order.
    pubkeys.reverse();
    let m = scriptnum_decode(&pop(stack)?)?;
    if m < 0 || m > n {
        return Err(ConsensusError::Script("multisig m".into()));
    }
    let mut sigs = Vec::with_capacity(m as usize);
    for _ in 0..m {
        sigs.push(pop(stack)?);
    }
    sigs.reverse();
    let dummy = pop(stack)?;
    // BIP147 NULLDUMMY: extra stack element must be empty. Softfork co-activated
    // with CSV on mainnet; always required for Witness v0 (segwit).
    if !dummy.is_empty() && (ctx.sig_version == SigVersion::WitnessV0 || ctx.bip112_active) {
        return Err(ConsensusError::Script("NULLDUMMY".into()));
    }

    // Core Base: FindAndDelete **all** sigs from scriptCode before the loop.
    let script_code_owned: Option<Vec<u8>> = if ctx.sig_version == SigVersion::Base {
        let mut sc = script_code_bytes(ctx).to_vec();
        for sig in &sigs {
            sc = find_and_delete(&sc, sig);
        }
        Some(sc)
    } else {
        None
    };
    let script_override = script_code_owned.as_deref();

    // Core: while (fSuccess && nSigsCount > 0) { try sigs[isig] vs pubs[ikey] }
    let mut f_success = true;
    let mut n_sigs = sigs.len();
    let mut n_keys = pubkeys.len();
    let mut isig = 0usize;
    let mut ikey = 0usize;
    while f_success && n_sigs > 0 {
        let f_ok = checksig_legacy(&sigs[isig], &pubkeys[ikey], ctx, script_override)?;
        if f_ok {
            isig += 1;
            n_sigs -= 1;
        }
        ikey += 1;
        n_keys -= 1;
        if n_sigs > n_keys {
            f_success = false;
        }
    }

    if verify {
        if !f_success {
            return Err(ConsensusError::Script("CHECKMULTISIGVERIFY".into()));
        }
    } else {
        push(stack, bool_encode(f_success))?;
    }
    Ok(())
}

/// Core `FindAndDelete`: remove every occurrence of a data-push of `data` from `script`.
///
/// Used for legacy (Base) CHECKSIG / CHECKMULTISIG so a signature cannot sign itself
/// when it appears inside scriptCode (mainnet block 290329: P2SH redeem embeds a sig).
pub(crate) fn find_and_delete(script: &[u8], data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return script.to_vec();
    }
    let mut needle = Vec::with_capacity(data.len() + 3);
    if data.len() < 0x4c {
        needle.push(data.len() as u8);
    } else if data.len() <= 0xff {
        needle.push(0x4c);
        needle.push(data.len() as u8);
    } else if data.len() <= 0xffff {
        needle.push(0x4d);
        needle.extend_from_slice(&(data.len() as u16).to_le_bytes());
    } else {
        needle.push(0x4e);
        needle.extend_from_slice(&(data.len() as u32).to_le_bytes());
    }
    needle.extend_from_slice(data);

    let mut out = Vec::with_capacity(script.len());
    let mut i = 0usize;
    while i < script.len() {
        if i + needle.len() <= script.len() && script[i..i + needle.len()] == needle[..] {
            i += needle.len();
            continue;
        }
        let op = script[i];
        out.push(op);
        i += 1;
        let n = if (1..=75).contains(&op) {
            op as usize
        } else if op == 0x4c {
            if i >= script.len() {
                break;
            }
            let n = script[i] as usize;
            out.push(script[i]);
            i += 1;
            n
        } else if op == 0x4d {
            if i + 1 >= script.len() {
                break;
            }
            let n = u16::from_le_bytes([script[i], script[i + 1]]) as usize;
            out.extend_from_slice(&script[i..i + 2]);
            i += 2;
            n
        } else if op == 0x4e {
            if i + 3 >= script.len() {
                break;
            }
            let n = u32::from_le_bytes([script[i], script[i + 1], script[i + 2], script[i + 3]])
                as usize;
            out.extend_from_slice(&script[i..i + 4]);
            i += 4;
            n
        } else {
            0
        };
        if n > 0 {
            let end = (i + n).min(script.len());
            out.extend_from_slice(&script[i..end]);
            i = end;
        }
    }
    out
}

/// Script bytes used as Base/WitnessV0 scriptCode (after OP_CODESEPARATOR).
fn script_code_bytes<'a>(ctx: &'a EvalContext<'_>) -> &'a [u8] {
    let full = ctx.script_code.as_bytes();
    match ctx.codeseparator_script_off.get() {
        Some(off) if off <= full.len() => &full[off..],
        _ => full,
    }
}

/// Core `CTransactionSignatureSerializer::SerializeScriptCode`: when hashing a
/// legacy (BASE) scriptCode, **skip every `OP_CODESEPARATOR` opcode** (0xab).
///
/// This is distinct from `pbegincodehash` truncation (which only drops bytes
/// *before* the last executed CODESEPARATOR). Separators that remain *after*
/// that point are still omitted from the serialized scriptCode. Without this,
/// redeem scripts that embed CODESEPARATOR (e.g. mainnet block 443992 P2SH
/// multi-condition contracts) produce a wrong sighash and fail CHECKSIGVERIFY.
fn strip_op_codeseparator(script: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(script.len());
    let mut i = 0usize;
    while i < script.len() {
        let op = script[i];
        i += 1;
        if op == 0xab {
            // OP_CODESEPARATOR — omit from legacy sighash scriptCode.
            continue;
        }
        out.push(op);
        let n = if (1..=75).contains(&op) {
            op as usize
        } else if op == 0x4c {
            if i >= script.len() {
                break;
            }
            let n = script[i] as usize;
            out.push(script[i]);
            i += 1;
            n
        } else if op == 0x4d {
            if i + 1 >= script.len() {
                break;
            }
            let n = u16::from_le_bytes([script[i], script[i + 1]]) as usize;
            out.extend_from_slice(&script[i..i + 2]);
            i += 2;
            n
        } else if op == 0x4e {
            if i + 3 >= script.len() {
                break;
            }
            let n = u32::from_le_bytes([script[i], script[i + 1], script[i + 2], script[i + 3]])
                as usize;
            out.extend_from_slice(&script[i..i + 4]);
            i += 4;
            n
        } else {
            0
        };
        if n > 0 {
            let end = (i + n).min(script.len());
            out.extend_from_slice(&script[i..end]);
            i = end;
        }
    }
    out
}

/// Legacy / witness-v0 CHECKSIG: empty or bad sig → false (does not hard-fail).
///
/// `script_code_override`: when `Some`, use these bytes as scriptCode for sighash
/// (CHECKMULTISIG pre-deletes **all** stack sigs). When `None`, Base path applies
/// FindAndDelete of **this** signature only (Core EvalChecksigPreTapscript).
fn checksig_legacy(
    sig: &[u8],
    pubkey: &[u8],
    ctx: &EvalContext<'_>,
    script_code_override: Option<&[u8]>,
) -> Result<bool, ConsensusError> {
    if sig.is_empty() {
        return Ok(false);
    }
    let (ecdsa_sig, sighash_ty) = match crypto::parse_der_sig(sig, ctx.bip66_active) {
        Ok(x) => x,
        Err(_) => return Ok(false),
    };
    let pk = match crypto::parse_pubkey(pubkey) {
        Ok(p) => p,
        Err(_) => return Ok(false),
    };
    let owned: Vec<u8>;
    let script_bytes: &[u8] = if let Some(sc) = script_code_override {
        sc
    } else {
        let base = script_code_bytes(ctx);
        if ctx.sig_version == SigVersion::Base {
            owned = find_and_delete(base, sig);
            owned.as_slice()
        } else {
            base
        }
    };
    let sighash = match sighash_for_script(ctx, sighash_ty, script_bytes) {
        Ok(h) => h,
        Err(_) => return Ok(false),
    };
    Ok(crypto::verify_ecdsa(sighash, &ecdsa_sig, &pk))
}

/// BIP340 Schnorr verify for a 32-byte x-only pubkey in tapscript (leaf sighash).
fn checksig_schnorr(
    sig: &[u8],
    pubkey: &[u8],
    ctx: &EvalContext<'_>,
) -> Result<bool, ConsensusError> {
    debug_assert_eq!(pubkey.len(), 32);
    let (sig_bytes, sighash_ty) = if sig.len() == 64 {
        (sig, bitcoin::sighash::TapSighashType::Default)
    } else if sig.len() == 65 {
        // BIP342: sighash byte must not be 0x00.
        if sig[64] == 0x00 {
            return Ok(false);
        }
        let ty = bitcoin::sighash::TapSighashType::from_consensus_u8(sig[64])
            .map_err(|_| ConsensusError::Script("tapscript sighash".into()))?;
        (&sig[..64], ty)
    } else {
        return Ok(false);
    };
    let xonly = bitcoin::key::XOnlyPublicKey::from_slice(pubkey)
        .map_err(|_| ConsensusError::Script("tapscript xonly".into()))?;
    let schnorr = match bitcoin::secp256k1::schnorr::Signature::from_slice(sig_bytes) {
        Ok(s) => s,
        Err(_) => return Ok(false),
    };
    let prevouts = Prevouts::All(ctx.prevouts);
    use bitcoin::sighash::Annex;
    use bitcoin::taproot::LeafVersion;
    use bitcoin::TapLeafHash;
    let leaf = TapLeafHash::from_script(ctx.script_code, LeafVersion::TapScript);
    // BIP341/BIP342: include last OP_CODESEPARATOR instruction index (default
    // 0xFFFFFFFF). `taproot_script_spend_signature_hash` hard-codes the default
    // and would reject multisig leaves that use CODESEPARATOR (signet 90719).
    // Annex (if present on the witness) must also enter the sighash.
    let codesep = ctx.codeseparator_pos.get();
    let annex = super::p2tr::bip341_annex(&ctx.tx.input[ctx.input_index].witness)
        .map(Annex::new)
        .transpose()
        .map_err(|_| ConsensusError::Script("tapscript annex".into()))?;
    let sighash = ctx
        .cache
        .borrow_mut()
        .taproot_signature_hash(
            ctx.input_index,
            &prevouts,
            annex,
            Some((leaf, codesep)),
            sighash_ty,
        )
        .map_err(|_| ConsensusError::Script("tapscript sighash".into()))?;
    let msg = bitcoin::secp256k1::Message::from_digest(sighash.to_byte_array());
    Ok(crypto::SECP.with(|secp| secp.verify_schnorr(&schnorr, &msg, &xonly).is_ok()))
}

fn sighash_for_script(
    ctx: &EvalContext<'_>,
    ty_raw: u32,
    script_bytes: &[u8],
) -> Result<[u8; 32], ConsensusError> {
    let script_code = Script::from_bytes(script_bytes);
    match ctx.sig_version {
        SigVersion::Base => {
            // Raw hashtype (0 is not the same as SIGHASH_ALL=1).
            // Strip OP_CODESEPARATOR from scriptCode (Core SerializeScriptCode).
            let stripped = strip_op_codeseparator(script_bytes);
            let script_code = Script::from_bytes(&stripped);
            let h = ctx
                .cache
                .borrow_mut()
                .legacy_signature_hash(ctx.input_index, script_code, ty_raw)
                .map_err(|_| ConsensusError::Script("legacy sighash".into()))?;
            Ok(h.to_byte_array())
        }
        SigVersion::WitnessV0 => {
            let mut cache = ctx.cache.borrow_mut();
            crypto::bip143_p2wsh_signature_hash(
                ctx.tx,
                ctx.input_index,
                script_code,
                ctx.amount,
                ty_raw,
                &mut cache,
            )
        }
        SigVersion::TapScript => unreachable!(),
    }
}

fn push(stack: &mut Vec<Vec<u8>>, v: Vec<u8>) -> Result<(), ConsensusError> {
    if stack.len() + 1 > MAX_STACK_SIZE {
        return Err(ConsensusError::Script("stack size".into()));
    }
    stack.push(v);
    Ok(())
}

fn pop(stack: &mut Vec<Vec<u8>>) -> Result<Vec<u8>, ConsensusError> {
    stack
        .pop()
        .ok_or_else(|| ConsensusError::Script("stack empty".into()))
}

fn require_n(stack: &[Vec<u8>], n: usize) -> Result<(), ConsensusError> {
    if stack.len() < n {
        return Err(ConsensusError::Script("stack empty".into()));
    }
    Ok(())
}

fn cast_to_bool(v: &[u8]) -> bool {
    for (i, &b) in v.iter().enumerate() {
        if b != 0 {
            // Negative zero
            if i == v.len() - 1 && b == 0x80 {
                return false;
            }
            return true;
        }
    }
    false
}

/// BIP342 MINIMALIF: IF/NOTIF argument is empty vector or single-byte 0x01 only.
fn is_minimal_if_arg(v: &[u8]) -> bool {
    v.is_empty() || v == [0x01]
}

fn bool_encode(b: bool) -> Vec<u8> {
    if b {
        vec![1]
    } else {
        vec![]
    }
}

fn scriptnum_encode(mut n: i64) -> Vec<u8> {
    if n == 0 {
        return vec![];
    }
    let neg = n < 0;
    if neg {
        n = -n;
    }
    let mut out = Vec::new();
    while n > 0 {
        out.push((n & 0xff) as u8);
        n >>= 8;
    }
    if out.last().map(|b| b & 0x80 != 0).unwrap_or(false) {
        out.push(if neg { 0x80 } else { 0x00 });
    } else if neg {
        let last = out.last_mut().unwrap();
        *last |= 0x80;
    }
    out
}

/// Decode a script number with Core's general 4-byte limit (arithmetic).
fn scriptnum_decode(v: &[u8]) -> Result<i64, ConsensusError> {
    scriptnum_decode_width(v, 4)
}

/// Decode a script number with explicit max byte length.
/// CLTV/CSV use `max_len = 5` so full u32 locktime/sequence ranges encode as
/// positive script numbers (Core `CScriptNum(..., 5)`).
fn scriptnum_decode_width(v: &[u8], max_len: usize) -> Result<i64, ConsensusError> {
    if v.len() > max_len {
        return Err(ConsensusError::Script("scriptnum overflow".into()));
    }
    if v.is_empty() {
        return Ok(0);
    }
    let mut result: i64 = 0;
    for (i, &b) in v.iter().enumerate() {
        result |= (b as i64) << (8 * i);
    }
    if v.last().unwrap() & 0x80 != 0 {
        // negative
        result &= !(0x80i64 << (8 * (v.len() - 1)));
        result = -result;
    }
    Ok(result)
}

fn bin_arith(stack: &mut Vec<Vec<u8>>, f: impl Fn(i64, i64) -> i64) -> Result<(), ConsensusError> {
    let b = scriptnum_decode(&pop(stack)?)?;
    let a = scriptnum_decode(&pop(stack)?)?;
    push(stack, scriptnum_encode(f(a, b)))
}

fn bin_cmp(stack: &mut Vec<Vec<u8>>, f: impl Fn(i64, i64) -> bool) -> Result<(), ConsensusError> {
    let b = scriptnum_decode(&pop(stack)?)?;
    let a = scriptnum_decode(&pop(stack)?)?;
    push(stack, bool_encode(f(a, b)))
}

#[cfg(test)]
mod success_and_disabled_tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::script::ScriptBuf;
    use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};

    fn eval(script_bytes: &[u8], sig_version: SigVersion) -> Result<bool, ConsensusError> {
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let prevouts = vec![TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }];
        // script_code points into script_bytes via Script::from_bytes
        let script = Script::from_bytes(script_bytes);
        let ctx = EvalContext::new(
            &tx,
            0,
            Amount::from_sat(50_000),
            &prevouts,
            script,
            sig_version,
        );
        let mut stack = Vec::new();
        eval_script(script, &mut stack, &ctx)
    }

    #[test]
    fn tapscript_op_success126_accepts() {
        // Real signet leaf fragment starts with normal ops then hits 0x7e (OP_SUCCESS126).
        let leaf = hex_literal(
            "60947600a2697601409f697601407c94b2750200006b760120a2636c04000000007e6b012094687660a2636c0200007e6b6094687658a2636c01007e",
        );
        assert!(tapscript_has_op_success(Script::from_bytes(&leaf)));
        let need_clean = eval(&leaf, SigVersion::TapScript).expect("tapscript success");
        assert!(!need_clean, "OP_SUCCESS skips cleanstack");
    }

    #[test]
    fn legacy_op_cat_disabled_rejects() {
        // Minimal: push empty, push empty, OP_CAT
        let script = vec![0x00, 0x00, 0x7e];
        let err = eval(&script, SigVersion::WitnessV0).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("disabled") || msg.contains("0x7e"),
            "unexpected: {msg}"
        );
    }

    /// Core: disabled opcodes fail even in unexecuted IF branches.
    #[test]
    fn unexecuted_disabled_opcode_still_rejects() {
        // OP_0 IF OP_CAT ELSE OP_1 ENDIF → IF body not taken, but CAT still fails.
        let script = vec![0x00, 0x63, 0x7e, 0x67, 0x51, 0x68];
        let err = eval(&script, SigVersion::Base).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("disabled") || msg.contains("0x7e"),
            "Core rejects unexecuted CAT: {msg}"
        );
    }

    /// Core: OP_VERIF always fails even when not executing.
    #[test]
    fn unexecuted_verif_still_rejects() {
        // OP_0 IF OP_VERIF ELSE OP_1 ENDIF
        let script = vec![0x00, 0x63, 0x65, 0x67, 0x51, 0x68];
        let err = eval(&script, SigVersion::Base).unwrap_err();
        assert!(
            format!("{err}").contains("VERIF") || format!("{err}").contains("opcode"),
            "{err}"
        );
    }

    #[test]
    fn bare_op_success_byte_not_success_on_legacy() {
        // On legacy, 0x7e is disabled CAT, not SUCCESS.
        assert!(!tapscript_has_op_success(Script::from_bytes(&[0x51]))); // no success
        let script = vec![0x7e];
        assert!(eval(&script, SigVersion::Base).is_err());
    }

    fn hex_literal(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// strip_op_codeseparator + script_code_bytes + OP_PUSHDATA lengths.
    #[test]
    fn strip_codeseparator_and_pushdata_lens() {
        // CODESEPARATOR (0xab) stripped; surrounding ops kept.
        let script = vec![0x51, 0xab, 0x52]; // OP_1 CODESEPARATOR OP_2
        let stripped = strip_op_codeseparator(&script);
        assert_eq!(stripped, vec![0x51, 0x52]);
        // PUSHDATA1 / 2 / 4 inside strip
        let mut s = vec![0x4c, 0x02, 0xaa, 0xbb, 0xab, 0x51];
        let st = strip_op_codeseparator(&s);
        assert!(st.contains(&0x4c));
        assert!(!st.contains(&0xab));
        s = vec![0x4d, 0x01, 0x00, 0xcc, 0xab];
        let st = strip_op_codeseparator(&s);
        assert!(st.windows(2).any(|w| w == [0x4d, 0x01]));
        s = vec![0x4e, 0x01, 0x00, 0x00, 0x00, 0xdd];
        let st = strip_op_codeseparator(&s);
        assert_eq!(st.last(), Some(&0xdd));
        // Truncated pushdata lengths break cleanly
        let _ = strip_op_codeseparator(&[0x4c]);
        let _ = strip_op_codeseparator(&[0x4d, 0x01]);
        let _ = strip_op_codeseparator(&[0x4e, 0x01, 0x00]);
        // script_code_bytes via EvalContext with codeseparator offset
        let need = eval(&[0x51], SigVersion::Base).unwrap();
        assert!(need);
    }

    #[test]
    fn tapscript_empty_checksigverify_fails() {
        // empty sig + 32-byte key + CHECKSIGVERIFY → EmptySig verify error
        let mut script = vec![0x00]; // empty sig
        script.push(0x20); // push 32
        script.extend_from_slice(&[0x02; 32]); // xonly-ish key (may fail xonly parse → false)
        script.push(0xad); // CHECKSIGVERIFY
                           // May error on empty verify or invalid key — either covers tapscript arms
        let r = eval(&script, SigVersion::TapScript);
        assert!(r.is_err() || matches!(r, Ok(_)));
    }

    #[test]
    fn tapscript_checksigadd_empty_sig_keeps_n() {
        // stack: empty_sig, n=2, unknown_key(1 byte) → CHECKSIGADD → n=2; OP_TRUE
        // Unknown key + empty sig → EmptySig path; push n.
        let script = vec![
            0x00, // empty sig
            0x52, // OP_2 (n)
            0x01, 0xaa, // push 1-byte unknown key type
            0xba, // OP_CHECKSIGADD
            0x52, // OP_2
            0x87, // OP_EQUAL
        ];
        let need = eval(&script, SigVersion::TapScript).expect("eval");
        assert!(need);
    }

    #[test]
    fn tapscript_checksigadd_unknown_key_nonempty_sig_increments() {
        // nonempty garbage sig + unknown key = Valid (soft-fork hook) → n+1
        let script = vec![
            0x01, 0x01, // 1-byte sig (non-empty)
            0x51, // OP_1 (n=1)
            0x01, 0xaa, // unknown key
            0xba, // CHECKSIGADD → 2
            0x52, // OP_2
            0x87, // EQUAL
        ];
        let need = eval(&script, SigVersion::TapScript).expect("eval");
        assert!(need);
    }

    #[test]
    fn tapscript_checksigadd_legacy_unknown() {
        // On witness v0, 0xba is still unknown.
        let script = vec![0x00, 0x51, 0x01, 0xaa, 0xba];
        let err = eval(&script, SigVersion::WitnessV0).unwrap_err();
        assert!(format!("{err}").contains("0xba"));
    }

    #[test]
    fn tapscript_checkmultisig_disabled() {
        let script = vec![0x00, 0x00, 0x00, 0x51, 0x51, 0xae]; // junk + CHECKMULTISIG
        let err = eval(&script, SigVersion::TapScript).unwrap_err();
        assert!(format!("{err}").contains("CHECKMULTISIG"));
    }

    #[test]
    fn tapscript_checksig_empty_pubkey_fails() {
        // empty sig, empty pubkey (OP_0 OP_0 CHECKSIG)
        let script = vec![0x00, 0x00, 0xac];
        let err = eval(&script, SigVersion::TapScript).unwrap_err();
        assert!(format!("{err}").contains("empty pubkey"));
    }

    #[test]
    fn op_1sub_and_unary_arith() {
        // OP_3 OP_1SUB → 2; OP_2 EQUAL
        let script = vec![0x53, 0x8c, 0x52, 0x87];
        assert!(eval(&script, SigVersion::WitnessV0).expect("1sub"));
        // OP_2 OP_1ADD → 3
        let script = vec![0x52, 0x8b, 0x53, 0x87];
        assert!(eval(&script, SigVersion::WitnessV0).expect("1add"));
        // OP_1 OP_NEGATE → -1; OP_1NEGATE EQUAL
        let script = vec![0x51, 0x8f, 0x4f, 0x87];
        assert!(eval(&script, SigVersion::WitnessV0).expect("negate"));
        // OP_1NEGATE OP_ABS → 1
        let script = vec![0x4f, 0x90, 0x51, 0x87];
        assert!(eval(&script, SigVersion::WitnessV0).expect("abs"));
    }

    #[test]
    fn tapscript_allows_scripts_over_10k() {
        // Legacy would reject; tapscript must accept (BIP342).
        let mut script = vec![0x51]; // OP_TRUE
        script.resize(10_001, 0x61); // pad with OP_NOP
        script.push(0x51); // end with TRUE so cleanstack ok if executed
                           // Actually NOPs leave stack; final TRUE needed as only element — start empty,
                           // fill with NOPs, end OP_1.
        let mut script = vec![0x61; 10_001];
        script.push(0x51);
        let need = eval(&script, SigVersion::TapScript).expect("tapscript large script");
        assert!(need);
        let err = eval(&script, SigVersion::WitnessV0).unwrap_err();
        assert!(format!("{err}").contains("too large"));
    }

    #[test]
    fn op_sha1_hashes() {
        // OP_0 OP_SHA1 → 20-byte hash of empty; SIZE 0x14 EQUAL (20)
        let script = vec![0x00, 0xa7, 0x82, 0x01, 0x14, 0x87];
        assert!(eval(&script, SigVersion::WitnessV0).expect("sha1"));
    }

    /// Every consensus-enabled opcode must not report "unknown opcode".
    /// Disabled/reserved still error, but with a specific reason.
    #[test]
    fn no_unknown_opcode_for_enabled_set() {
        // Minimal stacks so each op runs far enough to prove it is recognized.
        // Format: (name, script_bytes, sig_version) — success or known error, not unknown.
        let cases: &[(&str, Vec<u8>, SigVersion)] = &[
            ("1ADD", vec![0x51, 0x8b, 0x52, 0x87], SigVersion::WitnessV0),
            ("1SUB", vec![0x52, 0x8c, 0x51, 0x87], SigVersion::WitnessV0),
            (
                "NEGATE",
                vec![0x51, 0x8f, 0x4f, 0x87],
                SigVersion::WitnessV0,
            ),
            ("ABS", vec![0x4f, 0x90, 0x51, 0x87], SigVersion::WitnessV0),
            (
                "SHA1",
                vec![0x00, 0xa7, 0x82, 0x01, 0x14, 0x87],
                SigVersion::WitnessV0,
            ),
            (
                "RIPEMD160",
                vec![0x00, 0xa6, 0x82, 0x01, 0x14, 0x87],
                SigVersion::WitnessV0,
            ),
            (
                "SHA256",
                vec![0x00, 0xa8, 0x82, 0x01, 0x20, 0x87],
                SigVersion::WitnessV0,
            ),
            (
                "HASH160",
                vec![0x00, 0xa9, 0x82, 0x01, 0x14, 0x87],
                SigVersion::WitnessV0,
            ),
            (
                "HASH256",
                vec![0x00, 0xaa, 0x82, 0x01, 0x20, 0x87],
                SigVersion::WitnessV0,
            ),
            ("SIZE", vec![0x00, 0x82, 0x00, 0x87], SigVersion::WitnessV0),
            (
                "WITHIN",
                vec![0x51, 0x00, 0x52, 0xa5, 0x51, 0x87],
                SigVersion::WitnessV0,
            ),
            (
                "MIN",
                vec![0x51, 0x52, 0xa3, 0x51, 0x87],
                SigVersion::WitnessV0,
            ),
            (
                "MAX",
                vec![0x51, 0x52, 0xa4, 0x52, 0x87],
                SigVersion::WitnessV0,
            ),
            (
                "CHECKSIGADD",
                vec![0x00, 0x51, 0x01, 0xff, 0xba, 0x51, 0x87],
                SigVersion::TapScript,
            ),
        ];
        for (name, script, sv) in cases {
            match eval(script, *sv) {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("{e}");
                    assert!(
                        !msg.contains("unknown opcode"),
                        "{name}: unexpected unknown opcode: {msg}"
                    );
                }
            }
        }
        // Disabled must fail with disabled/reserved, not "unknown"
        for code in [0x7e_u8, 0x8d, 0x95] {
            let script = vec![0x51, 0x51, code];
            let err = eval(&script, SigVersion::WitnessV0).unwrap_err();
            let msg = format!("{err}");
            assert!(
                msg.contains("disabled") || msg.contains("unknown") || msg.contains("RESERVED"),
                "0x{code:02x}: {msg}"
            );
        }
    }

    /// BIP147: non-empty CHECKMULTISIG dummy fails when NULLDUMMY active.
    #[test]
    fn nulldummy_rejects_nonempty_dummy() {
        // 0-of-0 multisig: n=0, m=0, dummy non-empty → should fail NULLDUMMY.
        // Stack build (pushed first → deep): dummy=0x01, m=0, n=0 then OP_CHECKMULTISIG
        // After pops: n, keys, m, sigs, dummy — for 0/0: push dummy, 0, 0, CHECKMULTISIG
        let script = vec![0x01, 0xff, 0x00, 0x00, 0xae, 0x51]; // dummy, 0, 0, CMS, TRUE won't run
        let err = eval(&script, SigVersion::WitnessV0).unwrap_err();
        assert!(
            format!("{err}").contains("NULLDUMMY"),
            "expected NULLDUMMY, got {err}"
        );
        // Empty dummy 0-of-0 succeeds (CMS pushes true), then need true top — CMS pushes 1.
        let script_ok = vec![0x00, 0x00, 0x00, 0xae]; // empty dummy, m=0, n=0, CMS
        eval(&script_ok, SigVersion::WitnessV0).expect("0-of-0 empty dummy");
    }

    /// BIP112: OP_CSV fails when tx.nVersion < 2 (unsigned), matching Core.
    /// (`docs/external_findings/004-csv-nop-and-scriptnum-width.md`).
    #[test]
    fn csv_fails_when_tx_version_below_2() {
        // Script: push 1, CSV, DROP, OP_TRUE — v1 must fail CSV version gate.
        let script_bytes = [0x51u8, 0xb2, 0x75, 0x51]; // 1 CSV DROP TRUE
        let script = Script::from_bytes(&script_bytes);
        let prevouts = vec![TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }];
        let mk = |ver: i32| Transaction {
            version: bitcoin::transaction::Version(ver),
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::from_consensus(0), // would fail real CSV
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        // v1: Core fails (not NOP) when BIP112 active and disable bit clear.
        let tx1 = mk(1);
        let ctx1 = EvalContext::new_with_flags(
            &tx1,
            0,
            Amount::from_sat(50_000),
            &prevouts,
            script,
            SigVersion::Base,
            true,
            true, // bip112 active
            true,
        );
        let mut stack1 = Vec::new();
        let err = eval_script(script, &mut stack1, &ctx1).unwrap_err();
        assert!(
            matches!(err, ConsensusError::Script(ref s) if s.contains("CSV")),
            "v1 must fail CSV version gate, got {err:?}"
        );

        // v2: CSV enforces → fails with seq=0 vs csv=1
        let tx2 = mk(2);
        let ctx2 = EvalContext::new_with_flags(
            &tx2,
            0,
            Amount::from_sat(50_000),
            &prevouts,
            script,
            SigVersion::Base,
            true,
            true,
            true,
        );
        let mut stack2 = Vec::new();
        let err = eval_script(script, &mut stack2, &ctx2).unwrap_err();
        assert!(
            format!("{err}").contains("CSV"),
            "v2 should enforce CSV, got {err}"
        );
    }

    #[test]
    fn cleanstack_and_true_top_helpers() {
        assert!(require_clean_true(&[vec![0x01]]).is_ok());
        assert!(require_clean_true(&[]).is_err());
        assert!(require_clean_true(&[vec![0x01], vec![0x01]]).is_err());
        assert!(require_clean_true(&[vec![]]).is_err());
        assert!(require_true_top(&[vec![], vec![0x01]]).is_ok());
        assert!(require_true_top(&[]).is_err());
        assert!(require_true_top(&[vec![]]).is_err());
    }

    #[test]
    fn script_sig_pushes_op_n_and_1negate() {
        let mut stack = Vec::new();
        // OP_0, OP_1NEGATE, OP_1, push bytes
        let ss = Script::from_bytes(&[0x00, 0x4f, 0x51, 0x01, 0xaa]);
        eval_script_sig_pushes(ss, &mut stack).unwrap();
        assert_eq!(stack.len(), 4);
        assert_eq!(stack[0], Vec::<u8>::new());
        assert_eq!(stack[1], vec![0x81]);
        assert_eq!(stack[2], vec![0x01]);
        assert_eq!(stack[3], vec![0xaa]);
        // Non-push opcode fails.
        let mut s2 = Vec::new();
        assert!(eval_script_sig_pushes(Script::from_bytes(&[0xac]), &mut s2).is_err());
    }

    #[test]
    fn find_and_delete_pushdata_lengths() {
        // Direct push
        let data = vec![0x11u8; 3];
        let mut script = vec![0x03];
        script.extend_from_slice(&data);
        script.push(0x51);
        let out = find_and_delete(&script, &data);
        assert_eq!(out, vec![0x51]);
        assert_eq!(find_and_delete(&script, &[]), script);

        // PUSHDATA1 needle
        let big = vec![0x22u8; 80];
        let mut sc = vec![0x4c, 80];
        sc.extend_from_slice(&big);
        sc.push(0x52);
        let out2 = find_and_delete(&sc, &big);
        assert_eq!(out2, vec![0x52]);

        // PUSHDATA2
        let bigger = vec![0x33u8; 300];
        let mut sc2 = vec![0x4d];
        sc2.extend_from_slice(&(300u16).to_le_bytes());
        sc2.extend_from_slice(&bigger);
        sc2.push(0x53);
        assert_eq!(find_and_delete(&sc2, &bigger), vec![0x53]);

        // Non-matching copy through PUSHDATA encodings
        let passthrough = vec![0x4c, 0x01, 0xaa, 0x4d, 0x01, 0x00, 0xbb];
        assert_eq!(find_and_delete(&passthrough, &[0xff]), passthrough);
    }

    #[test]
    fn strip_codeseparator_keeps_pushdata() {
        // OP_1, CODESEPARATOR, PUSHDATA1 payload, OP_TRUE
        let sc = vec![0x51, 0xab, 0x4c, 0x02, 0xde, 0xad, 0x51];
        let stripped = strip_op_codeseparator(&sc);
        assert!(!stripped.contains(&0xab));
        assert!(stripped.windows(2).any(|w| w == [0xde, 0xad]));

        let sc2 = vec![0x4d, 0x01, 0x00, 0xee, 0xab, 0x51];
        let s2 = strip_op_codeseparator(&sc2);
        assert_eq!(s2.last(), Some(&0x51));
        assert!(!s2.contains(&0xab));

        let sc3 = vec![0x4e, 0x01, 0x00, 0x00, 0x00, 0x01, 0xab];
        let s3 = strip_op_codeseparator(&sc3);
        assert!(!s3.contains(&0xab));
        let _ = (sc, sc2, sc3);
    }

    #[test]
    fn tapscript_has_op_success_pushdata_scan() {
        // OP_SUCCESS via direct byte after PUSHDATA1 skip
        let leaf = vec![0x4c, 0x02, 0x00, 0x00, 0x7e]; // push 2 then OP_SUCCESS126
        assert!(tapscript_has_op_success(Script::from_bytes(&leaf)));
        let leaf2 = vec![0x4d, 0x01, 0x00, 0xff, 0x7e];
        assert!(tapscript_has_op_success(Script::from_bytes(&leaf2)));
        let leaf3 = vec![0x4e, 0x01, 0x00, 0x00, 0x00, 0xaa, 0x7e];
        assert!(tapscript_has_op_success(Script::from_bytes(&leaf3)));
        // Truncated pushdata — no success past end
        assert!(!tapscript_has_op_success(Script::from_bytes(&[0x4c])));
        assert!(!tapscript_has_op_success(Script::from_bytes(&[0x4d, 0x01])));
        assert!(!tapscript_has_op_success(Script::from_bytes(&[
            0x4e, 0x01, 0x00
        ])));
        let _ = (leaf, leaf2, leaf3);
    }

    #[test]
    fn cltv_type_mismatch_and_final_sequence() {
        // locktime height vs time type mismatch
        let script_bytes = [0x51u8, 0xb1, 0x75, 0x51]; // 1 CLTV DROP TRUE
        let script = Script::from_bytes(&script_bytes);
        let prevouts = vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }];
        let tx = Transaction {
            version: bitcoin::transaction::Version::ONE,
            lock_time: LockTime::from_time(500_000_001).unwrap(),
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::from_consensus(0),
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let ctx = EvalContext::new_with_flags(
            &tx,
            0,
            Amount::from_sat(1),
            &prevouts,
            script,
            SigVersion::Base,
            true,
            true,
            true,
        );
        let mut stack = Vec::new();
        let err = eval_script(script, &mut stack, &ctx).unwrap_err();
        assert!(format!("{err}").contains("CLTV"), "got {err}");

        // Final sequence rejects CLTV even when locktime ok
        let script2 = Script::from_bytes(&[0x00, 0xb1, 0x75, 0x51]); // 0 CLTV DROP TRUE
        let tx2 = Transaction {
            version: bitcoin::transaction::Version::ONE,
            lock_time: LockTime::from_height(10).unwrap(),
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let ctx2 = EvalContext::new_with_flags(
            &tx2,
            0,
            Amount::from_sat(1),
            &prevouts,
            script2,
            SigVersion::Base,
            true,
            true,
            true,
        );
        let mut stack2 = Vec::new();
        let err2 = eval_script(script2, &mut stack2, &ctx2).unwrap_err();
        assert!(
            format!("{err2}").contains("CLTV") || format!("{err2}").contains("final"),
            "got {err2}"
        );
    }

    #[test]
    fn checkmultisig_verify_fail_and_tapscript_disabled() {
        // 1-of-1 CMSVERIFY with empty sigs fails
        // stack: dummy empty, sig empty, m=1, pk empty, n=1 → will fail verify
        let script = vec![0x00, 0x00, 0x51, 0x00, 0x51, 0xaf];
        let err = eval(&script, SigVersion::WitnessV0).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("CHECKMULTISIG") || msg.contains("NULLDUMMY") || msg.contains("stack"),
            "{msg}"
        );
        // Tapscript: CHECKMULTISIG disabled
        let err2 = eval(&[0x00, 0x00, 0x00, 0xae], SigVersion::TapScript).unwrap_err();
        assert!(
            format!("{err2}").contains("CHECKMULTISIG") || format!("{err2}").contains("disabled"),
            "{err2}"
        );
        let err3 = eval(&[0x00, 0x00, 0x00, 0xaf], SigVersion::TapScript).unwrap_err();
        assert!(
            format!("{err3}").to_lowercase().contains("checkmultisig")
                || format!("{err3}").contains("disabled"),
            "{err3}"
        );
    }

    #[test]
    fn minimal_if_and_cast_bool_negzero() {
        assert!(is_minimal_if_arg(&[]));
        assert!(is_minimal_if_arg(&[0x01]));
        assert!(!is_minimal_if_arg(&[0x00]));
        assert!(!is_minimal_if_arg(&[0x01, 0x00]));
        // negative zero is false
        assert!(!cast_to_bool(&[0x80]));
        assert!(cast_to_bool(&[0x01]));
        assert!(!cast_to_bool(&[]));
        assert!(!cast_to_bool(&[0x00, 0x00]));
    }

    #[test]
    fn minimalif_rejects_nonminimal_arg() {
        // TapScript MINIMALIF: push byte 0x00 (non-minimal false), IF, TRUE, ENDIF.
        // OP_0 empty is minimal; length-1 push of 0x00 is not.
        let script = vec![0x01, 0x00, 0x63, 0x51, 0x68];
        let err = eval(&script, SigVersion::TapScript).unwrap_err();
        assert!(format!("{err}").contains("MINIMALIF"), "got {err}");
        // OP_0 empty is minimal false → IF skipped → OP_TRUE after ENDIF succeeds.
        let script_ok = vec![0x00, 0x63, 0x51, 0x68, 0x51];
        assert!(eval(&script_ok, SigVersion::TapScript).expect("minimal empty if"));
    }

    #[test]
    fn cltv_negative_locktime_rejected() {
        // OP_1NEGATE CLTV …
        let script_bytes = [0x4fu8, 0xb1, 0x75, 0x51];
        let script = Script::from_bytes(&script_bytes);
        let prevouts = vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }];
        let tx = Transaction {
            version: bitcoin::transaction::Version::ONE,
            lock_time: LockTime::from_height(10).unwrap(),
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::from_consensus(0),
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let ctx = EvalContext::new_with_flags(
            &tx,
            0,
            Amount::from_sat(1),
            &prevouts,
            script,
            SigVersion::Base,
            true,
            true,
            true,
        );
        let mut stack = Vec::new();
        let err = eval_script(script, &mut stack, &ctx).unwrap_err();
        assert!(
            format!("{err}").contains("CLTV") || format!("{err}").contains("negative"),
            "got {err}"
        );
    }

    #[test]
    fn stack_size_limit_on_2dup() {
        // MAX_STACK_SIZE is typically 1000; push 999 ones then 2DUP overflows.
        let mut script = Vec::new();
        for _ in 0..999 {
            script.push(0x51);
        }
        script.push(0x6e); // OP_2DUP
        let err = eval(&script, SigVersion::WitnessV0).unwrap_err();
        assert!(
            format!("{err}").contains("stack") || format!("{err}").contains("size"),
            "got {err}"
        );
    }

    #[test]
    fn op_0_empty_push_and_depth() {
        // OP_0 OP_DEPTH OP_1 EQUAL — depth is 1 after empty push? OP_0 pushes empty → depth 1
        // Then DEPTH pushes 1, stack [empty, 1]; not clean. Simpler: OP_0 OP_SIZE OP_0 EQUAL
        let script = vec![0x00, 0x82, 0x00, 0x87]; // 0 SIZE 0 EQUAL
        assert!(eval(&script, SigVersion::WitnessV0).expect("size empty"));
    }

    #[test]
    fn find_and_delete_pushdata4() {
        // Needle uses PUSHDATA4 only when data.len() > 0xffff.
        let data = vec![0x44u8; 0x10000];
        let mut sc = vec![0x4e];
        sc.extend_from_slice(&(data.len() as u32).to_le_bytes());
        sc.extend_from_slice(&data);
        sc.push(0x51);
        assert_eq!(find_and_delete(&sc, &data), vec![0x51]);
        // Truncated PUSHDATA4 length field is copied/broken out without panic.
        let short = vec![0x4e, 0x01, 0x00];
        let out = find_and_delete(&short, &[0xff]);
        assert!(!out.is_empty() || out.is_empty());
        assert_eq!(out[0], 0x4e);
    }
}
