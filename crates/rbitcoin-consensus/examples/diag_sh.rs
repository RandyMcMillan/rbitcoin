use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::script::Script;
use bitcoin::secp256k1::ecdsa::Signature;
use bitcoin::secp256k1::{Message, PublicKey, Secp256k1};
use bitcoin::sighash::SighashCache;
use bitcoin::Block;

fn hx(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Core FindAndDelete: remove all occurrences of a data-push of `data` from script.
fn find_and_delete(script: &[u8], data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return script.to_vec();
    }
    // Build the push encoding of data
    let mut needle = Vec::new();
    if data.len() < 76 {
        needle.push(data.len() as u8);
    } else if data.len() <= 0xff {
        needle.push(0x4c);
        needle.push(data.len() as u8);
    } else {
        panic!("long push");
    }
    needle.extend_from_slice(data);

    let mut out = Vec::new();
    let mut i = 0usize;
    while i < script.len() {
        if i + needle.len() <= script.len() && &script[i..i + needle.len()] == needle.as_slice() {
            i += needle.len();
            continue;
        }
        // copy one instruction
        let op = script[i];
        out.push(op);
        i += 1;
        if (1..=75).contains(&op) {
            let n = op as usize;
            out.extend_from_slice(&script[i..i + n]);
            i += n;
        } else if op == 0x4c {
            let n = script[i] as usize;
            out.push(script[i]);
            i += 1;
            out.extend_from_slice(&script[i..i + n]);
            i += n;
        }
    }
    out
}

fn main() {
    let block: Block = deserialize(&std::fs::read("/tmp/b290329/block.bin").unwrap()).unwrap();
    let want = "5df1375ffe61ac35ca178ebb0cab9ea26dedbd0e96005dfcee7e379fa513232f";
    let tx = block
        .txdata
        .iter()
        .find(|t| t.compute_txid().to_string() == want)
        .unwrap();
    let mut items: Vec<Vec<u8>> = Vec::new();
    {
        let ss = tx.input[1].script_sig.as_bytes();
        let mut i = 0usize;
        while i < ss.len() {
            let op = ss[i];
            i += 1;
            if op == 0 {
                items.push(vec![]);
            } else if op < 76 {
                let n = op as usize;
                items.push(ss[i..i + n].to_vec());
                i += n;
            } else if op == 0x4c {
                let n = ss[i] as usize;
                i += 1;
                items.push(ss[i..i + n].to_vec());
                i += n;
            }
        }
    }
    let redeem = &items[3];
    let sig_s = &items[1];
    let sig_a = &items[2];
    let pk = PublicKey::from_slice(&redeem[1 + 1 + 72 + 1..1 + 1 + 72 + 1 + 33]).unwrap();
    let cache = SighashCache::new(tx);
    let secp = Secp256k1::verification_only();

    // After deleting BOTH sigs from redeem
    let mut sc = redeem.clone();
    sc = find_and_delete(&sc, sig_s);
    sc = find_and_delete(&sc, sig_a);
    println!("after both deletes: {}", hx(&sc));

    for (name, sig) in [("S", sig_s), ("A", sig_a)] {
        let ht = sig[sig.len() - 1] as u32;
        let der = &sig[..sig.len() - 1];
        // current-only delete
        let only = find_and_delete(redeem, sig);
        let h_only = cache
            .legacy_signature_hash(1, Script::from_bytes(&only), ht)
            .unwrap()
            .to_byte_array();
        // both deleted
        let h_both = cache
            .legacy_signature_hash(1, Script::from_bytes(&sc), ht)
            .unwrap()
            .to_byte_array();
        // full
        let h_full = cache
            .legacy_signature_hash(1, Script::from_bytes(redeem), ht)
            .unwrap()
            .to_byte_array();

        for (label, h) in [("full", h_full), ("only", h_only), ("both", h_both)] {
            let msg = Message::from_digest(h);
            let mut s = Signature::from_der_lax(der).unwrap();
            s.normalize_s();
            let ok = secp.verify_ecdsa(&msg, &s, &pk).is_ok();
            println!("{name}/{label} ok={ok} hash={}", hx(&h));
        }
    }
}
