use std::time::Duration;

use gasket::runtime::spawn_stage;
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use crate::{bootstrap, crosscut, model};

type InputPort = gasket::messaging::TwoPhaseInputPort<model::EnrichedBlockPayload>;
type OutputPort = gasket::messaging::OutputPort<model::CRDTCommand>;

pub mod balance_by_address;
pub mod balance_by_genius_stake;
pub mod macros;
mod worker;

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    BalanceByAddress(balance_by_address::Config),
    BalanceByGeniusStake(balance_by_genius_stake::Config),
}

impl Config {
    fn plugin(
        self,
        _chain: &crosscut::ChainWellKnownInfo,
        policy: &crosscut::policies::RuntimePolicy,
    ) -> Reducer {
        match self {
            Config::BalanceByAddress(c) => c.plugin(policy),
            Config::BalanceByGeniusStake(c) => c.plugin(policy),
        }
    }
}

pub struct Bootstrapper {
    input: InputPort,
    output: OutputPort,
    reducers: Vec<Reducer>,
    policy: crosscut::policies::RuntimePolicy,
}

impl Bootstrapper {
    pub fn new(
        configs: Vec<Config>,
        chain: &crosscut::ChainWellKnownInfo,
        policy: &crosscut::policies::RuntimePolicy,
    ) -> Self {
        Self {
            reducers: configs
                .into_iter()
                .map(|x| x.plugin(chain, policy))
                .collect(),
            input: Default::default(),
            output: Default::default(),
            policy: policy.clone(),
        }
    }

    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort {
        &mut self.input
    }

    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort {
        &mut self.output
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline) {
        let worker = worker::Worker::new(self.reducers, self.input, self.output, self.policy);
        pipeline.register_stage(spawn_stage(
            worker,
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(600)),
                ..Default::default()
            },
            Some("reducers"),
        ));
    }
}

pub enum Reducer {
    BalanceByAddress(balance_by_address::Reducer),
    BalanceByGeniusStake(balance_by_genius_stake::Reducer),
}

impl Reducer {
    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut OutputPort,
    ) -> Result<(), gasket::error::Error> {
        match self {
            Reducer::BalanceByAddress(x) => x.reduce_block(block, ctx, output),
            Reducer::BalanceByGeniusStake(x) => x.reduce_block(block, ctx, output),
        }
    }
}
