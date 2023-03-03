use pallas::ledger::addresses::ShelleyAddress;
use std::{collections::HashMap, fmt::Debug};

use pallas::{
    ledger::traverse::{Era, MultiEraBlock, MultiEraOutput, MultiEraTx, OutputRef},
    network::miniprotocols::Point,
};

use crate::prelude::*;

#[derive(Debug, Clone)]
pub enum RawBlockPayload {
    RollForward(Vec<u8>),
    RollBack(Point),
}

impl RawBlockPayload {
    pub fn roll_forward(block: Vec<u8>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(block),
        }
    }

    pub fn roll_back(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(point),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockContext {
    utxos: HashMap<String, (Era, Vec<u8>)>,
}

impl BlockContext {
    pub fn import_ref_output(&mut self, key: &OutputRef, era: Era, cbor: Vec<u8>) {
        self.utxos.insert(key.to_string(), (era, cbor));
    }

    pub fn find_utxo(&self, key: &OutputRef) -> Result<MultiEraOutput, Error> {
        let (era, cbor) = self
            .utxos
            .get(&key.to_string())
            .ok_or_else(|| Error::missing_utxo(key))?;

        MultiEraOutput::decode(*era, cbor).map_err(crate::Error::cbor)
    }

    pub fn get_all_keys(&self) -> Vec<String> {
        self.utxos.keys().map(|x| x.clone()).collect()
    }

    pub fn find_consumed_txos(
        &self,
        tx: &MultiEraTx,
        policy: &RuntimePolicy,
    ) -> Result<Vec<(OutputRef, MultiEraOutput)>, Error> {
        let items = tx
            .consumes()
            .iter()
            .map(|i| i.output_ref())
            .map(|r| self.find_utxo(&r).map(|u| (r, u)))
            .map(|r| r.apply_policy(policy))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(items)
    }
}

#[derive(Debug, Clone)]
pub enum EnrichedBlockPayload {
    RollForward(Vec<u8>, BlockContext),
    RollBack(Point),
}

impl EnrichedBlockPayload {
    pub fn roll_forward(block: Vec<u8>, ctx: BlockContext) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(block, ctx),
        }
    }

    pub fn roll_back(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(point),
        }
    }
}

pub type Set = String;
pub type Member = String;
pub type PolicyId = String;
pub type Delta = i64;
pub type Timestamp = u64;

#[derive(Clone, Debug)]
pub enum Value {
    String(String),
    BigInt(i128),
    Cbor(Vec<u8>),
    Json(serde_json::Value),
}

impl From<String> for Value {
    fn from(x: String) -> Self {
        Value::String(x)
    }
}

impl From<Vec<u8>> for Value {
    fn from(x: Vec<u8>) -> Self {
        Value::Cbor(x)
    }
}

impl From<serde_json::Value> for Value {
    fn from(x: serde_json::Value) -> Self {
        Value::Json(x)
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum CRDTCommand {
    BlockStarting(Point),
    VotingPowerChange(ShelleyAddress, PolicyId, Delta, Point),
    BlockFinished(Point),
    RollBack(Point),
}

impl CRDTCommand {
    pub fn block_starting(block: &MultiEraBlock) -> CRDTCommand {
        let hash = block.hash();
        let slot = block.slot();
        let point = Point::Specific(slot, hash.to_vec());
        CRDTCommand::BlockStarting(point)
    }

    pub fn voting_power_change(
        address: ShelleyAddress,
        policy: PolicyId,
        delta: Delta,
        point: Point,
    ) -> CRDTCommand {
        CRDTCommand::VotingPowerChange(address, policy, delta, point)
    }

    pub fn block_finished(block: &MultiEraBlock) -> CRDTCommand {
        let hash = block.hash();
        let slot = block.slot();
        let point = Point::Specific(slot, hash.to_vec());
        CRDTCommand::BlockFinished(point)
    }

    pub fn rollback(point: Point) -> CRDTCommand {
        CRDTCommand::RollBack(point)
    }
}
