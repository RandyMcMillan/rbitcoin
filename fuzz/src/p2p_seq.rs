//! First-slice stateful P2P sequence: ping / headers / block kinds.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pSeqKind {
    Ping,
    Block,
    Headers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct P2pSeqStep {
    pub kind: P2pSeqKind,
    pub skip: bool,
}

pub fn parse_p2p_sequence(data: &[u8]) -> Vec<P2pSeqStep> {
    let mut out = Vec::new();
    for &b in data.iter().take(8) {
        let kind = match b % 4 {
            0 => P2pSeqKind::Ping,
            1 => P2pSeqKind::Block,
            2 => P2pSeqKind::Headers,
            _ => {
                out.push(P2pSeqStep { kind: P2pSeqKind::Ping, skip: true });
                continue;
            }
        };
        out.push(P2pSeqStep { kind, skip: false });
    }
    out
}

pub fn p2p_sequence_ping_comparisons(data: &[u8]) -> u32 {
    parse_p2p_sequence(data).iter().filter(|s| !s.skip && s.kind == P2pSeqKind::Ping).count() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_ping_steps_record_two_comparisons() {
        assert_eq!(p2p_sequence_ping_comparisons(&[0, 0]), 2);
        assert_eq!(parse_p2p_sequence(&[0, 0]).len(), 2);
    }

    #[test]
    fn malformed_step_is_skip_not_death() {
        let steps = parse_p2p_sequence(&[3]);
        assert_eq!(steps.len(), 1);
        assert!(steps[0].skip);
        assert_eq!(p2p_sequence_ping_comparisons(&[3]), 0);
    }
}
