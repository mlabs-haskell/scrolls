pub mod postgres;
pub mod skip;

use gasket::messaging::TwoPhaseInputPort;
use serde::Deserialize;

use crate::{
    bootstrap,
    crosscut::{self, PointArg},
    model,
};

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    Skip(skip::Config),
    Postgres(postgres::Config),
}

impl Config {
    pub fn plugin(
        self,
        chain: &crosscut::ChainWellKnownInfo,
        intersect: &crosscut::IntersectConfig,
        _policy: &crosscut::policies::RuntimePolicy,
    ) -> Bootstrapper {
        match self {
            Config::Skip(c) => Bootstrapper::Skip(c.bootstrapper()),
            Config::Postgres(c) => Bootstrapper::Postgres(c.bootstrapper(chain, intersect)),
        }
    }
}

pub enum Bootstrapper {
    Skip(skip::Bootstrapper),
    Postgres(postgres::Bootstrapper),
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut TwoPhaseInputPort<model::CRDTCommand> {
        match self {
            Bootstrapper::Skip(x) => x.borrow_input_port(),
            Bootstrapper::Postgres(x) => x.borrow_input_port(),
        }
    }

    pub fn build_cursor(&mut self) -> Cursor {
        match self {
            Bootstrapper::Skip(x) => Cursor::Skip(x.build_cursor()),
            Bootstrapper::Postgres(x) => Cursor::Postgres(x.build_cursor()),
        }
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline) {
        match self {
            Bootstrapper::Skip(x) => x.spawn_stages(pipeline),
            Bootstrapper::Postgres(x) => x.spawn_stages(pipeline),
        }
    }
}

pub enum Cursor {
    Skip(skip::Cursor),
    Postgres(postgres::Cursor),
}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<PointArg>, crate::Error> {
        match self {
            Cursor::Skip(x) => x.last_point(),
            Cursor::Postgres(x) => x.last_point(),
        }
    }
}
