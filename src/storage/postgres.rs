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
        .or_panic()?;
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

                CREATE TABLE IF NOT EXISTS voting_power (
                    id           SERIAL PRIMARY KEY,
                    spending     TEXT NOT NULL,
                    staking      TEXT NOT NULL,
                    policy       TEXT NOT NULL,
                    token        TEXT NOT NULL,
                    amount       BIGINT NOT NULL,
                    created_slot BIGINT NOT NULL REFERENCES cursor ON DELETE CASCADE,
                    tx_id        TEXT NOT NULL,
                    tx_idx       BIGINT NOT NULL,
                    spent_slot   BIGINT
                );

                CREATE INDEX IF NOT EXISTS voting_power_spending_idx ON voting_power (spending);
                CREATE INDEX IF NOT EXISTS voting_power_staking_idx ON voting_power (staking);
                CREATE INDEX IF NOT EXISTS voting_power_policy_idx ON voting_power (policy);
                CREATE INDEX IF NOT EXISTS voting_power_token_idx ON voting_power (token);
                CREATE INDEX IF NOT EXISTS voting_power_utxo_idx ON voting_power (tx_id, tx_idx);
            ",
            )
            .or_panic()?;

        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.input.recv_or_idle()?;
        match msg.payload {
            model::CRDTCommand::BlockStarting(Point::Specific(slot, hash)) => {
                log::debug!("block started {:?}", slot);
                let hash_str = hex::encode(hash);
                self.connection
                    .as_mut()
                    .unwrap()
                    .execute(
                        "INSERT INTO cursor (slot, hash) VALUES ($1, $2)",
                        &[&(slot as i64), &hash_str],
                    )
                    .or_panic()?;
            }
            model::CRDTCommand::BlockStarting(Point::Origin) => {}
            model::CRDTCommand::BlockFinished(_) => {}
            model::CRDTCommand::VotingPowerCreated {
                owner,
                policy,
                name,
                amount,
                point: Point::Specific(slot, _hash),
                tx_id,
                tx_idx,
            } => {
                let spending = owner.payment().to_hex();
                let staking = owner.delegation().to_hex();

                self.connection
                    .as_mut()
                    .unwrap()
                    .execute("INSERT INTO voting_power (spending, staking, policy, token, amount, created_slot, tx_id, tx_idx, spent_slot) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL)"
                             , &[&spending,
				 &staking,
				 &policy,
				 &name,
				 &(amount as i64),
				 &(slot as i64),
				 &tx_id,
				 &(tx_idx as i64)
			     ])
                    .or_panic()?;
            }
            model::CRDTCommand::VotingPowerCreated {
                point: Point::Origin,
                ..
            } => unreachable!(),
            model::CRDTCommand::VotingPowerSpent {
                tx_id,
                tx_idx,
                point,
            } => {
                let slot = match point {
                    Point::Specific(slot, _) => slot,
                    Point::Origin => 0,
                };

                self.connection
                    .as_mut()
                    .unwrap()
                    .execute(
                        "UPDATE voting_power SET spent_slot = $1 WHERE tx_id = $2 AND tx_idx = $3",
                        &[&(slot as i64), &tx_id, &(tx_idx as i64)],
                    )
                    .or_panic()?;
            }
            model::CRDTCommand::RollBack(point) => {
                let slot = match point {
                    Point::Specific(slot, _) => slot,
                    Point::Origin => 0,
                };
                self.connection
                    .as_mut()
                    .unwrap()
                    .execute("DELETE FROM cursor WHERE slot > $1", &[&(slot as i64)])
                    .or_panic()?;

                self.connection
                    .as_mut()
                    .unwrap()
                    .execute(
                        "UPDATE voting_power SET spent_slot = NULL WHERE spent_slot > $1",
                        &[&(slot as i64)],
                    )
                    .or_panic()?;
            }
        };
        self.ops_count.inc(1);
        self.input.commit();
        Ok(WorkOutcome::Partial)
    }
}
