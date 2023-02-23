use gasket::{
    error::AsWorkError,
    runtime::{spawn_stage, WorkOutcome},
};
use pallas::network::miniprotocols::Point;
use serde::Deserialize;
use std::{str::FromStr, time::Duration};

use crate::{bootstrap, crosscut, model};

type InputPort = gasket::messaging::TwoPhaseInputPort<model::CRDTCommand>;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub connection_params: String,
}

impl Config {
    pub fn bootstrapper(
        self,
        _chain: &crosscut::ChainWellKnownInfo,
        _intersect: &crosscut::IntersectConfig,
    ) -> Bootstrapper {
        Bootstrapper {
            config: self,
            input: Default::default(),
        }
    }
}

pub struct Bootstrapper {
    config: Config,
    input: InputPort,
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort {
        &mut self.input
    }

    pub fn build_cursor(&self) -> Cursor {
        Cursor {
            config: self.config.clone(),
        }
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline) {
        let worker = Worker {
            config: self.config.clone(),
            connection: None,
            input: self.input,
            ops_count: Default::default(),
        };

        pipeline.register_stage(spawn_stage(
            worker,
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(600)),
                bootstrap_retry: gasket::retries::Policy {
                    max_retries: 20,
                    backoff_unit: Duration::from_secs(1),
                    backoff_factor: 2,
                    max_backoff: Duration::from_secs(60),
                },
                ..Default::default()
            },
            Some("postgres"),
        ));
    }
}

pub struct Cursor {
    config: Config,
}

impl Cursor {
    fn rows_to_point(res: Vec<postgres::Row>) -> Option<crosscut::PointArg> {
        let row: &postgres::Row = res.get(0)?;
        let slot: i64 = row.get(0);
        let hash: String = row.get(1);
        let s = format!("{},{}", slot, hash);
        crosscut::PointArg::from_str(s.as_str()).ok()
    }

    pub fn last_point(&mut self) -> Result<Option<crosscut::PointArg>, crate::Error> {
        let mut connection = postgres::Client::connect(
            self.config.connection_params.clone().as_str(),
            postgres::NoTls,
        )
        .map_err(crate::Error::storage)?;

        let raw = connection
            .query(
                "SELECT slot, hash FROM cursor ORDER BY slot DESC LIMIT 1",
                &[],
            )
            .map_err(crate::Error::storage)?;

        let point = Self::rows_to_point(raw);

        Ok(point)
    }
}

pub struct Worker {
    config: Config,
    connection: Option<postgres::Client>,
    ops_count: gasket::metrics::Counter,
    input: InputPort,
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("storage_ops", &self.ops_count)
            .build()
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let connection = postgres::Client::connect(
            self.config.connection_params.clone().as_str(),
            postgres::NoTls,
        )
        .or_restart()?;
        self.connection = Some(connection);

        self.connection
            .as_mut()
            .unwrap()
            .batch_execute(
                "
                CREATE TABLE IF NOT EXISTS cursor (
                    slot   BIGINT NOT NULL,
                    hash   TEXT NOT NULL,
                    PRIMARY KEY (slot)
                );
            ",
            )
            .or_restart()?;

        self.connection
            .as_mut()
            .unwrap()
            .batch_execute(
                "
                CREATE TABLE IF NOT EXISTS voting_power (
                    id       SERIAL PRIMARY KEY,
                    address  TEXT NOT NULL,
                    policy   TEXT NOT NULL,
                    delta    BIGINT NOT NULL,
                    slot     BIGINT NOT NULL REFERENCES cursor
                );
            ",
            )
            .or_restart()?;

        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.input.recv_or_idle()?;
        match msg.payload {
            model::CRDTCommand::BlockStarting(Point::Specific(slot, hash)) => {
                let hash_str = hex::encode(hash);
                self.connection
                    .as_mut()
                    .unwrap()
                    .execute(
                        "INSERT INTO cursor (slot, hash) VALUES ($1, $2)",
                        &[&(slot as i64), &hash_str],
                    )
                    .or_restart()?;
            }
            model::CRDTCommand::BlockStarting(Point::Origin) => {}
            model::CRDTCommand::BlockFinished(_) => {}
            model::CRDTCommand::VotingPowerChange(
                address,
                policy,
                delta,
                Point::Specific(slot, _hash),
            ) => {
                self.connection
                    .as_mut()
                    .unwrap()
                    .execute("INSERT INTO voting_power (address, policy, delta, slot) VALUES ($1, $2, $3, $4)"
                             , &[&address, &policy, &delta, &(slot as i64)])
                    .or_restart()?;
            }
            model::CRDTCommand::VotingPowerChange(_, _, _, Point::Origin) => {}
        };
        self.ops_count.inc(1);
        self.input.commit();
        Ok(WorkOutcome::Partial)
    }
}
