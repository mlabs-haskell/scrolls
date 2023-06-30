use hex::ToHex;
use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::Address;
use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{Asset, MultiEraBlock, OutputRef};
use pallas::network::miniprotocols::Point;
use serde::Deserialize;

use crate::model::CRDTCommand;
use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    // TODO: save_ada: bool // default to false
    pub filter: Option<crosscut::filters::Predicate>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        block: &MultiEraBlock,
        input: &OutputRef,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let point = Point::Specific(block.slot(), block.hash().to_vec());
        output.send(gasket::messaging::Message::from(
            CRDTCommand::VotingPowerSpent {
                tx_id: input.hash().encode_hex(),
                tx_idx: input.index() as usize,
                point,
            },
        ))
    }

    fn process_produced_txo(
        &mut self,
        block: &MultiEraBlock,
        tx_output: &MultiEraOutput,
        output: &mut super::OutputPort,
        utxo_idx: usize,
        tx_hash: Hash<32>,
    ) -> Result<(), gasket::error::Error> {
        let point = Point::Specific(block.slot(), block.hash().to_vec());
        let address = match tx_output.address().or_panic()? {
            Address::Shelley(x) => x,
            _ => return Ok(()),
        };

        tx_output.non_ada_assets().iter().for_each(|asset| {
            let (policy, name, amount) = match &asset {
                Asset::Ada(_) => unreachable!(),
                Asset::NativeAsset(cs, tn, amount) => (cs.encode_hex(), tn.encode_hex(), *amount),
            };

            output
                .send(gasket::messaging::Message::from(
                    CRDTCommand::VotingPowerCreated {
                        owner: address.clone(),
                        policy,
                        name,
                        amount,
                        point: point.clone(),
                        tx_id: tx_hash.encode_hex(),
                        tx_idx: utxo_idx,
                    },
                ))
                .unwrap()
        });
        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            if filter_matches!(self, block, &tx, ctx) {
                for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                    self.process_consumed_txo(block, &consumed, output)?;
                }

                for (idx, produced) in tx.produces() {
                    self.process_produced_txo(block, &produced, output, idx, tx.hash())?;
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            policy: policy.clone(),
        };

        super::Reducer::BalanceByAddress(reducer)
    }
}
