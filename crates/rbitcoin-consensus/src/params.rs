use bitcoin::blockdata::constants;
use bitcoin::consensus::Params as BtcParams;
use bitcoin::hashes::Hash;
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
}

/// Sparse mainnet checkpoints from historical Bitcoin Core `mapCheckpoints`
/// (display-order hex). Do **not** invent hashes — a wrong entry rejects the
/// real chain. Later mainnet safety relies on milestone + most-work, not dense
/// checkpoints (Core itself stopped extending this list).
fn mainnet_checkpoints(genesis: BlockHash) -> Vec<Checkpoint> {
    fn h(hex: &str) -> BlockHash {
        let bytes = hex::decode(hex).expect("checkpoint hex");
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

/// Default IBD milestone (coarse assumevalid): skip script/prevout at/below height.
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
