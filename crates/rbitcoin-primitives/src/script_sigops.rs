//! Core-style legacy sigop count (CHECKSIG=1, CHECKMULTISIG=20 or accurate N).

/// Count CHECKSIG / CHECKMULTISIG in `script`.
///
/// `accurate`: CHECKMULTISIG after OP_1..OP_16 counts that N; else 20.
#[must_use]
pub fn script_sigop_count(script: &[u8], accurate: bool) -> u64 {
    let mut n = 0u64;
    let mut i = 0usize;
    let mut last_opcode = 0xffu8;
    while i < script.len() {
        let opcode = script[i];
        i += 1;
        if opcode <= 0x4b {
            let push = opcode as usize;
            i = i.saturating_add(push);
        } else if opcode == 0x4c && i < script.len() {
            let push = script[i] as usize;
            i = i.saturating_add(1 + push);
        } else if opcode == 0x4d && i + 1 < script.len() {
            let push = u16::from_le_bytes([script[i], script[i + 1]]) as usize;
            i = i.saturating_add(2 + push);
        } else if opcode == 0x4e && i + 3 < script.len() {
            let push = u32::from_le_bytes(script[i..i + 4].try_into().unwrap_or([0; 4])) as usize;
            i = i.saturating_add(4 + push);
        } else if opcode == 0xac || opcode == 0xad {
            n = n.saturating_add(1);
        } else if opcode == 0xae || opcode == 0xaf {
            if accurate && last_opcode >= 0x51 && last_opcode <= 0x60 {
                n = n.saturating_add(u64::from(last_opcode - 0x50));
            } else {
                n = n.saturating_add(20);
            }
        }
        last_opcode = opcode;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accurate_checkmultisig_uses_op_n() {
        assert_eq!(script_sigop_count(&[0xac], false), 1);
        assert_eq!(script_sigop_count(&[0xad], false), 1);
        assert_eq!(script_sigop_count(&[0xae], false), 20);
        assert_eq!(script_sigop_count(&[0xaf], false), 20);
        assert_eq!(script_sigop_count(&[0x52, 0xae], true), 2);
        assert_eq!(script_sigop_count(&[0x53, 0xae], true), 3);
        assert_eq!(script_sigop_count(&[0x01, 0xff, 0xac], false), 1);
        assert_eq!(script_sigop_count(&[0x4c, 0x01, 0xab, 0xac], false), 1);
    }
}
