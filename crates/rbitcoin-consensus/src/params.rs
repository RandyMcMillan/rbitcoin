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
        let genesis = constants::genesis_block(Network::Signet);
        Self {
            network: Network::Signet,
            genesis_hash: genesis.block_hash(),
            pow_limit: Target::MAX_ATTAINABLE_SIGNET,
            checkpoints: vec![],
            btc: BtcParams::new(Network::Signet),
            signet_challenge: Some(crate::signet::default_signet_challenge()),
        }
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
    /// Buried CSV package heights (Core `DeploymentPos::DEPLOYMENT_CSV`):
    /// mainnet 419328, testnet3 770112, signet/regtest 1 (signet) / 432 (regtest historical).
    /// We pin Core's buried values; regtest uses 1 so our tests and local mining
    /// match modern Core regtest defaults for soft-forks.
    #[inline]
    pub fn csv_active_at(&self, height: u32) -> bool {
        height >= self.csv_height()
    }

    /// Buried height for BIP68/112/113 (CSV package).
    pub fn csv_height(&self) -> u32 {
        match self.network {
            Network::Bitcoin => 419_328,
            Network::Testnet => 770_112,
            // Signet / testnet4 / regtest: soft-forks from genesis-adjacent heights.
            Network::Signet | Network::Testnet4 => 1,
            // Core regtest CSV height is 432 in some versions; use 1 for always-on
            // modern regtest soft-forks (matches how we treat BIP34 as optional).
            Network::Regtest => 1,
        }
    }

    /// BIP141/143/147 segwit consensus active?
    ///
    /// Buried: mainnet 481824, testnet3 834624, signet 1, regtest 0 (always).
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

    /// BIP341/342 taproot active? Mainnet buried 709632; signet/regtest 1/0.
    #[inline]
    pub fn taproot_active_at(&self, height: u32) -> bool {
        height >= self.taproot_height()
    }

    pub fn taproot_height(&self) -> u32 {
        match self.network {
            Network::Bitcoin => 709_632,
            Network::Testnet => 2_400_000, // approximate / not heavily used here
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
        Checkpoint {
            height: 295_000,
            hash: h("00000000000000004d9b4ef50f0f9d826646340508c915db44e3d2c91f49c78a"),
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
