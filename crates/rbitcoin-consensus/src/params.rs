use bitcoin::blockdata::constants;
use bitcoin::{BlockHash, CompactTarget, Network, Target};
use rbitcoin_primitives::Height;

/// Static chain parameters for validation.
#[derive(Debug, Clone)]
pub struct ChainParams {
    pub network: Network,
    pub genesis_hash: BlockHash,
    pub pow_limit: Target,
    pub checkpoints: &'static [Checkpoint],
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
            checkpoints: &[],
        }
    }

    pub fn mainnet() -> Self {
        let genesis = constants::genesis_block(Network::Bitcoin);
        Self {
            network: Network::Bitcoin,
            genesis_hash: genesis.block_hash(),
            pow_limit: Target::MAX_ATTAINABLE_MAINNET,
            checkpoints: &[],
        }
    }

    pub fn testnet() -> Self {
        let genesis = constants::genesis_block(Network::Testnet);
        Self {
            network: Network::Testnet,
            genesis_hash: genesis.block_hash(),
            pow_limit: Target::MAX_ATTAINABLE_TESTNET,
            checkpoints: &[],
        }
    }

    pub fn signet() -> Self {
        let genesis = constants::genesis_block(Network::Signet);
        Self {
            network: Network::Signet,
            genesis_hash: genesis.block_hash(),
            pow_limit: Target::MAX_ATTAINABLE_SIGNET,
            checkpoints: &[],
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
}

/// Verify height-0 block hash matches params genesis.
pub fn check_genesis_hash(params: &ChainParams, hash: BlockHash) -> bool {
    hash == params.genesis_hash
}

pub fn genesis_block(params: &ChainParams) -> bitcoin::block::Block {
    constants::genesis_block(params.network)
}
