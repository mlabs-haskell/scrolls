use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::Address;
use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{Asset, MultiEraBlock, OutputRef};
use pallas::network::miniprotocols::Point;
use serde::Deserialize;

use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: String,
    pub filter: Option<crosscut::filters::Predicate>,
    pub policy_id_hex: Option<String>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
}

impl Reducer {
    fn get_native(asset: &Asset) -> Option<(&Hash<28>, &Vec<u8>, &u64)> {
        match asset {
            Asset::Ada(..) => None,
            Asset::NativeAsset(cs, tn, amt) => Some((cs, tn, amt)),
        }
    }

    fn get_tokens_amount(&self, utxo: &MultiEraOutput) -> i64 {
        match &self.config.policy_id_hex {
            None => utxo.lovelace_amount() as i64,
            Some(policy_id_hex) => utxo
                .non_ada_assets()
                .iter()
                .flat_map(|asset| Self::get_native(asset))
                .filter(|(cs, _, _)| &hex::encode(cs) == policy_id_hex)
                .map(|(_, _, amt)| *amt as i64)
                .sum(),
        }
    }

    fn process_consumed_txo(
        &mut self,
        block: &MultiEraBlock,
        ctx: &model::BlockContext,
        input: &OutputRef,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let point = Point::Specific(block.slot(), block.hash().to_vec());
        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(()),
        };

        let address = match utxo.address().or_panic()? {
            Address::Shelley(x) => x,
            _ => return Ok(()),
        };

        let prefix = self.config.key_prefix.clone();

        let delta = self.get_tokens_amount(&utxo);
        if delta != 0 {
            let crdt = model::CRDTCommand::voting_power_change(address, prefix, -1 * delta, point);
            output.send(gasket::messaging::Message::from(crdt))?;
        }

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        block: &MultiEraBlock,
        tx_output: &MultiEraOutput,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let point = Point::Specific(block.slot(), block.hash().to_vec());
        let address = match tx_output.address().or_panic()? {
            Address::Shelley(x) => x,
            _ => return Ok(()),
        };

        let prefix = self.config.key_prefix.clone();

        let delta = self.get_tokens_amount(&tx_output);
        if delta != 0 {
            let crdt = model::CRDTCommand::voting_power_change(address, prefix, delta, point);
            output.send(gasket::messaging::Message::from(crdt))?;
        }

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
                    self.process_consumed_txo(block, &ctx, &consumed, output)?;
                }

                for (_, produced) in tx.produces() {
                    self.process_produced_txo(block, &produced, output)?;
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
