//! One scripthash or Bitcoin address per line.

use crate::hex;
use bitcoin::address::NetworkUnchecked;
use bitcoin::hashes::{sha256, Hash};
use bitcoin::Address;
use std::path::Path;
use std::str::FromStr;

/// Electrum / Esplora display scripthash: SHA256(spk) bytes reversed, hex.
pub fn electrum_scripthash_hex(spk: &[u8]) -> String {
    let h = sha256::Hash::hash(spk).to_byte_array();
    let mut rev = h;
    rev.reverse();
    hex::encode(&rev)
}

pub fn parse_target_line(line: &str) -> Result<Option<String>, String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Ok(None);
    }
    if line.len() == 64 && hex::decode(line).ok().is_some_and(|b| b.len() == 32) {
        return Ok(Some(line.to_ascii_lowercase()));
    }
    let addr: Address<NetworkUnchecked> = Address::from_str(line).map_err(|e| e.to_string())?;
    let addr = addr.assume_checked();
    Ok(Some(electrum_scripthash_hex(
        addr.script_pubkey().as_bytes(),
    )))
}

pub fn load_targets(path: &Path) -> Result<Vec<String>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if let Some(sh) = parse_target_line(line)? {
            out.push(sh);
        }
        if i > 1_000_000 {
            return Err("target file too large".into());
        }
    }
    if out.is_empty() {
        return Err("no scripthashes or addresses in target file".into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn skips_blank_and_comment() {
        assert_eq!(parse_target_line("").unwrap(), None);
        assert_eq!(parse_target_line("  # hi").unwrap(), None);
    }

    #[test]
    fn accepts_64_hex() {
        let h = "ab".repeat(32);
        assert_eq!(parse_target_line(&h).unwrap().as_deref(), Some(h.as_str()));
    }

    #[test]
    fn p2wpkh_mainnet_is_stable_scripthash() {
        // bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq
        let line = "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq";
        let sh = parse_target_line(line).unwrap().unwrap();
        assert_eq!(sh.len(), 64);
        assert_eq!(
            parse_target_line(&sh).unwrap().as_deref(),
            Some(sh.as_str())
        );
    }

    #[test]
    fn load_targets_file() {
        let dir = std::env::temp_dir().join(format!("rbtc-bench-targets-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("t.txt");
        let mut f = std::fs::File::create(&p).unwrap();
        writeln!(f, "# comment\n{}\n", "cd".repeat(32)).unwrap();
        let got = load_targets(&p).unwrap();
        assert_eq!(got.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_targets_empty_errors() {
        let dir = std::env::temp_dir().join(format!("rbtc-bench-empty-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("e.txt");
        std::fs::write(&p, "# only\n").unwrap();
        assert!(load_targets(&p).unwrap_err().contains("no scripthashes"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
