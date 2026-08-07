use bitcoin::blockdata::constants;
use bitcoin::consensus::Params as BtcParams;
use bitcoin::hashes::Hash;
use bitcoin::script::ScriptBuf;
use bitcoin::{BlockHash, CompactTarget, Network, Target};
use rbitcoin_primitives::Height;

/// Static chain parameters for validation.
#[derive(Debug, Clone)]
pub struct ChainParams {
    pub network: Network,
    pub genesis_hash: BlockHash,
    pub pow_limit: Target,
    pub checkpoints: Vec<Checkpoint>,
    /// rust-bitcoin consensus params (retarget spacing, no_pow_retargeting, …).
    pub btc: BtcParams,
    /// BIP325 signet challenge script (`None` = not a signet).
    pub signet_challenge: Option<ScriptBuf>,
}

#[derive(Debug, Clone, Copy)]
pub struct Checkpoint {
    pub height: u32,
    pub hash: BlockHash,
}

impl ChainParams {
    pub fn for_network(network: rbitcoin_primitives::Network) -> Self {
        match network {
            rbitcoin_primitives::Network::Mainnet => Self::mainnet(),
            rbitcoin_primitives::Network::Testnet => Self::testnet(),
            rbitcoin_primitives::Network::Signet => Self::signet(),
            rbitcoin_primitives::Network::Regtest => Self::regtest(),
        }
    }

    pub fn regtest() -> Self {
        let genesis = constants::genesis_block(Network::Regtest);
        Self {
            network: Network::Regtest,
            genesis_hash: genesis.block_hash(),
            pow_limit: Target::MAX_ATTAINABLE_REGTEST,
            checkpoints: vec![],
            btc: BtcParams::new(Network::Regtest),
            signet_challenge: None,
        }
    }

    pub fn mainnet() -> Self {
        let genesis = constants::genesis_block(Network::Bitcoin);
        Self {
            network: Network::Bitcoin,
            genesis_hash: genesis.block_hash(),
            pow_limit: Target::MAX_ATTAINABLE_MAINNET,
            checkpoints: mainnet_checkpoints(genesis.block_hash()),
            btc: BtcParams::new(Network::Bitcoin),
            signet_challenge: None,
        }
    }

    pub fn testnet() -> Self {
        let genesis = constants::genesis_block(Network::Testnet);
        Self {
            network: Network::Testnet,
            genesis_hash: genesis.block_hash(),
            pow_limit: Target::MAX_ATTAINABLE_TESTNET,
            checkpoints: vec![],
            btc: BtcParams::new(Network::Testnet),
            signet_challenge: None,
        }
    }

    pub fn signet() -> Self {
        Self::custom_signet(crate::signet::default_signet_challenge(), 10 * 60)
            .expect("default signet block time is nonzero")
    }

    /// Build custom BIP325 signet parameters.
    ///
    /// `block_time` changes PoW target spacing while retaining Signet's two-week
    /// target timespan, matching Bitcoin Core's `signetblocktime` behavior.
    pub fn custom_signet(challenge: ScriptBuf, block_time: u64) -> Result<Self, &'static str> {
        if block_time == 0 {
            return Err("signet block time must be greater than zero");
        }
        let genesis = constants::genesis_block(Network::Signet);
        let mut btc = BtcParams::new(Network::Signet);
        btc.pow_target_spacing = block_time;
        Ok(Self {
            network: Network::Signet,
            genesis_hash: genesis.block_hash(),
            pow_limit: Target::MAX_ATTAINABLE_SIGNET,
            checkpoints: vec![],
            btc,
            signet_challenge: Some(challenge),
        })
    }

    pub fn checkpoint_at(&self, height: Height) -> Option<BlockHash> {
        self.checkpoints
            .iter()
            .find(|c| c.height == height.0)
            .map(|c| c.hash)
    }

    pub fn min_difficulty_target(&self) -> CompactTarget {
        self.pow_limit.to_compact_lossy()
    }

    pub fn difficulty_adjustment_interval(&self) -> u32 {
        self.btc.difficulty_adjustment_interval() as u32
    }

    pub fn no_pow_retargeting(&self) -> bool {
        self.btc.no_pow_retargeting
    }

    /// Coinbase maturity in blocks (Core default).
    pub fn coinbase_maturity(&self) -> u32 {
        100
    }

    /// BIP34 coinbase-height push required at this height?
    #[inline]
    pub fn bip34_active_at(&self, height: u32) -> bool {
        height >= self.btc.bip34_height
    }

    /// BIP65 CLTV active?
    #[inline]
    pub fn bip65_active_at(&self, height: u32) -> bool {
        height >= self.btc.bip65_height
    }

    /// BIP66 strict DER encoding required?
    #[inline]
    pub fn bip66_active_at(&self, height: u32) -> bool {
        height >= self.btc.bip66_height
    }

    /// BIP112 CHECKSEQUENCEVERIFY active?
    ///
    /// Buried heights match Bitcoin Core `CMainParams` / `SigNetParams` and
    /// Bitcoin Inquisition (same buried CSV on mainnet/signet):
    /// mainnet 419328, testnet3 770112, signet/testnet4 1, regtest 1.
    #[inline]
    pub fn csv_active_at(&self, height: u32) -> bool {
        height >= self.csv_height()
    }

    /// Buried height for BIP68/112/113 (CSV package).
    pub fn csv_height(&self) -> u32 {
        match self.network {
            Network::Bitcoin => 419_328,
            Network::Testnet => 770_112,
            Network::Signet | Network::Testnet4 => 1,
            // Core regtest historically 432; we use 1 (always-on for local mining).
            Network::Regtest => 1,
        }
    }

    /// BIP141/143/147 segwit consensus active?
    ///
    /// Core + Inquisition: mainnet 481824, testnet3 834624, signet 1, regtest 0
    /// (always). Signet BIP325 blocks carry witness from height 1 — archive prep
    /// must not apply this gate with a fake height 0 (see `ValidationContext`).
    #[inline]
    pub fn segwit_active_at(&self, height: u32) -> bool {
        height >= self.segwit_height()
    }

    pub fn segwit_height(&self) -> u32 {
        match self.network {
            Network::Bitcoin => 481_824,
            Network::Testnet => 834_624,
            Network::Signet | Network::Testnet4 => 1,
            Network::Regtest => 0,
        }
    }

    /// BIP341/342 taproot active?
    ///
    /// Core mainnet buried 709632 (Inquisition same). Signet/testnet4: 1.
    /// Testnet3: ~2011968 (Inquisition `TaprootHeight`; Core may still use
    /// versionbits — we pin buried for script flags).
    #[inline]
    pub fn taproot_active_at(&self, height: u32) -> bool {
        height >= self.taproot_height()
    }

    pub fn taproot_height(&self) -> u32 {
        match self.network {
            Network::Bitcoin => 709_632,
            Network::Testnet => 2_011_968,
            Network::Signet | Network::Testnet4 => 1,
            Network::Regtest => 0,
        }
    }
}

/// Sparse mainnet checkpoints from historical Bitcoin Core `mapCheckpoints`
/// (display-order hex). Do **not** invent hashes — a wrong entry rejects the
/// real chain. Later mainnet safety relies on milestone + most-work, not dense
/// checkpoints (Core itself stopped extending this list).
fn mainnet_checkpoints(genesis: BlockHash) -> Vec<Checkpoint> {
    fn h(hex: &str) -> BlockHash {
        let bytes = rbitcoin_primitives::hex_decode(hex).expect("checkpoint hex");
        let arr: [u8; 32] = bytes.try_into().expect("32 bytes");
        // Bitcoin displays hashes internal-byte-order reversed in hex.
        let mut rev = arr;
        rev.reverse();
        BlockHash::from_byte_array(rev)
    }
    vec![
        Checkpoint {
            height: 0,
            hash: genesis,
        },
        Checkpoint {
            height: 11_111,
            hash: h("0000000069e244f73d78e8fd29ba2fd2ed618bd6fa2ee92559f542fdb26e7c1d"),
        },
        Checkpoint {
            height: 33_333,
            hash: h("000000002dd5588a74784eaa7ab0507a18ad16a236e7b1ce69f00d7ddfb5d0a6"),
        },
        Checkpoint {
            height: 74_000,
            hash: h("0000000000573993a3c9e41ce34471c079dcf5f52a0e824a81e7f953b8661a20"),
        },
        Checkpoint {
            height: 105_000,
            hash: h("00000000000291ce28027faea320c8d2b054b2e0fe44a773f3eefb151d6bdc97"),
        },
        Checkpoint {
            height: 134_444,
            hash: h("00000000000005b12ffd4cd315cd34ffd4a594f430ac814c91184a0d42d2b0fe"),
        },
        Checkpoint {
            height: 168_000,
            hash: h("000000000000099e61ea72015e79632f216fe6cb33d7899acb35b75c8303b763"),
        },
        Checkpoint {
            height: 193_000,
            hash: h("000000000000059f452a5f7340de6682a977387c17010ff6e6c3bd83ca8b1317"),
        },
        Checkpoint {
            height: 210_000,
            hash: h("000000000000048b95347e83192f69cf0366076336c639f9b7228e9ba171342e"),
        },
        Checkpoint {
            height: 216_116,
            hash: h("00000000000001b4f4b433e81ee46494af945cf96014816a4e2370f11b23df4e"),
        },
        Checkpoint {
            height: 225_430,
            hash: h("00000000000001c108384350f74090433e7fcf79a606b8e797f065b130575932"),
        },
        Checkpoint {
            height: 250_000,
            hash: h("000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214"),
        },
        Checkpoint {
            height: 279_000,
            hash: h("0000000000000001ae8c72a0b0c301f67e3afca10e819efa9041e458e9bd7e40"),
        },
        // Core mapCheckpoints last entry — must match mainnet (IBD tip stall @ 294999).
        Checkpoint {
            height: 295_000,
            hash: h("00000000000000004d9b4ef50f0f9d686fd69db2e03af35a100370c64632a983"),
        },
    ]
}

/// Default IBD milestone (coarse assumevalid): skip **script/sig** checks
/// at/below height. Prevouts, double-spend, maturity, and fees still run.
///
/// `0` means full validation. Operators override with `--milestone HEIGHT`
/// or disable with `--milestone 0` after CLI applies network defaults.
/// Raise mainnet as deeper buried tips become the norm.
pub fn default_milestone_height(network: rbitcoin_primitives::Network) -> u32 {
    match network {
        // Scripts skipped through a deeply buried height for experimental mainnet IBD.
        rbitcoin_primitives::Network::Mainnet => 840_000,
        rbitcoin_primitives::Network::Testnet => 2_500_000,
        // Signet tip moves; keep default above typical tip so catch-up stays under
        // milestone until operators opt into full validation (`--milestone 0`).
        rbitcoin_primitives::Network::Signet => 2_000_000,
        rbitcoin_primitives::Network::Regtest => 0,
    }
}

/// Verify height-0 block hash matches params genesis.
pub fn check_genesis_hash(params: &ChainParams, hash: BlockHash) -> bool {
    hash == params.genesis_hash
}

pub fn genesis_block(params: &ChainParams) -> bitcoin::block::Block {
    constants::genesis_block(params.network)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    fn display_hash(hex: &str) -> BlockHash {
        let bytes = rbitcoin_primitives::hex_decode(hex).expect("hex");
        let arr: [u8; 32] = bytes.try_into().expect("32");
        let mut rev = arr;
        rev.reverse();
        BlockHash::from_byte_array(rev)
    }

    /// Core `SigNetParams` / Inquisition signet: soft forks at height 1 (not 0).
    #[test]
    fn signet_buried_deployments_match_core() {
        let p = ChainParams::signet();
        assert_eq!(p.btc.bip34_height, 1);
        assert_eq!(p.btc.bip65_height, 1);
        assert_eq!(p.btc.bip66_height, 1);
        assert_eq!(p.csv_height(), 1);
        assert_eq!(p.segwit_height(), 1);
        assert_eq!(p.taproot_height(), 1);
        assert!(!p.segwit_active_at(0));
        assert!(p.segwit_active_at(1));
        assert!(p.signet_challenge.is_some());
    }

    #[test]
    fn custom_signet_uses_challenge_and_block_time() {
        let challenge = ScriptBuf::from_bytes(vec![0x51]);
        let p = ChainParams::custom_signet(challenge.clone(), 60).unwrap();

        assert_eq!(p.signet_challenge.as_ref(), Some(&challenge));
        assert_eq!(p.btc.pow_target_spacing, 60);
        assert_eq!(p.btc.pow_target_timespan, 14 * 24 * 60 * 60);
        assert_eq!(p.difficulty_adjustment_interval(), 20_160);
        assert!(ChainParams::custom_signet(challenge, 0).is_err());
    }

    /// Mainnet buried heights (Core + Inquisition).
    #[test]
    fn mainnet_buried_deployments_match_core() {
        let p = ChainParams::mainnet();
        assert_eq!(p.btc.bip34_height, 227_931);
        assert_eq!(p.csv_height(), 419_328);
        assert_eq!(p.segwit_height(), 481_824);
        assert_eq!(p.taproot_height(), 709_632);
        assert!(!p.segwit_active_at(481_823));
        assert!(p.segwit_active_at(481_824));
    }

    /// Regression: wrong invented hash at 295000 blacklisted the real mainnet tip
    /// block (`checkpoint mismatch`) and froze confirm at tip 294999.
    #[test]
    fn mainnet_checkpoints_match_core_chain() {
        let p = ChainParams::mainnet();
        let expected: &[(u32, &str)] = &[
            (
                0,
                "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
            ),
            (
                11_111,
                "0000000069e244f73d78e8fd29ba2fd2ed618bd6fa2ee92559f542fdb26e7c1d",
            ),
            (
                33_333,
                "000000002dd5588a74784eaa7ab0507a18ad16a236e7b1ce69f00d7ddfb5d0a6",
            ),
            (
                74_000,
                "0000000000573993a3c9e41ce34471c079dcf5f52a0e824a81e7f953b8661a20",
            ),
            (
                105_000,
                "00000000000291ce28027faea320c8d2b054b2e0fe44a773f3eefb151d6bdc97",
            ),
            (
                134_444,
                "00000000000005b12ffd4cd315cd34ffd4a594f430ac814c91184a0d42d2b0fe",
            ),
            (
                168_000,
                "000000000000099e61ea72015e79632f216fe6cb33d7899acb35b75c8303b763",
            ),
            (
                193_000,
                "000000000000059f452a5f7340de6682a977387c17010ff6e6c3bd83ca8b1317",
            ),
            (
                210_000,
                "000000000000048b95347e83192f69cf0366076336c639f9b7228e9ba171342e",
            ),
            (
                216_116,
                "00000000000001b4f4b433e81ee46494af945cf96014816a4e2370f11b23df4e",
            ),
            (
                225_430,
                "00000000000001c108384350f74090433e7fcf79a606b8e797f065b130575932",
            ),
            (
                250_000,
                "000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214",
            ),
            (
                279_000,
                "0000000000000001ae8c72a0b0c301f67e3afca10e819efa9041e458e9bd7e40",
            ),
            (
                295_000,
                "00000000000000004d9b4ef50f0f9d686fd69db2e03af35a100370c64632a983",
            ),
        ];
        assert_eq!(p.checkpoints.len(), expected.len());
        for (i, (h, hex)) in expected.iter().enumerate() {
            assert_eq!(p.checkpoints[i].height, *h, "height order");
            assert_eq!(
                p.checkpoint_at(Height(*h)).unwrap(),
                display_hash(hex),
                "checkpoint {h}"
            );
        }
        // Explicit pin for the IBD stall case.
        assert_eq!(
            p.checkpoint_at(Height(295_000)).unwrap().to_string(),
            "00000000000000004d9b4ef50f0f9d686fd69db2e03af35a100370c64632a983"
        );
    }

    #[test]
    fn for_network_and_helpers() {
        use rbitcoin_primitives::Network;
        assert_eq!(
            ChainParams::for_network(Network::Mainnet).network,
            bitcoin::Network::Bitcoin
        );
        assert_eq!(
            ChainParams::for_network(Network::Testnet).network,
            bitcoin::Network::Testnet
        );
        assert_eq!(
            ChainParams::for_network(Network::Signet).network,
            bitcoin::Network::Signet
        );
        assert_eq!(
            ChainParams::for_network(Network::Regtest).network,
            bitcoin::Network::Regtest
        );

        let tn = ChainParams::testnet();
        assert_eq!(tn.csv_height(), 770_112);
        assert_eq!(tn.segwit_height(), 834_624);
        assert_eq!(tn.taproot_height(), 2_011_968);
        assert_eq!(tn.coinbase_maturity(), 100);
        assert!(!tn.no_pow_retargeting());
        let _ = tn.min_difficulty_target();
        assert!(tn.difficulty_adjustment_interval() > 0);
        assert!(tn.bip34_active_at(tn.btc.bip34_height));
        assert!(tn.bip65_active_at(tn.btc.bip65_height));
        assert!(tn.bip66_active_at(tn.btc.bip66_height));
        assert!(tn.csv_active_at(tn.csv_height()));
        assert!(tn.taproot_active_at(tn.taproot_height()));
        assert!(check_genesis_hash(&tn, tn.genesis_hash));
        let g = genesis_block(&tn);
        assert_eq!(g.block_hash(), tn.genesis_hash);

        let rt = ChainParams::regtest();
        assert!(rt.no_pow_retargeting() || rt.difficulty_adjustment_interval() > 0);
        // Missing checkpoint → None.
        assert!(rt.checkpoint_at(Height(1)).is_none());
    }
}
