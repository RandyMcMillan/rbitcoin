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
            height: 250_000,
            hash: h("000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214"),
        },
    ]
}

/// Verify height-0 block hash matches params genesis.
pub fn check_genesis_hash(params: &ChainParams, hash: BlockHash) -> bool {
    hash == params.genesis_hash
}

pub fn genesis_block(params: &ChainParams) -> bitcoin::block::Block {
    constants::genesis_block(params.network)
}
