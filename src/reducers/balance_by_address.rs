use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{Asset, MultiEraBlock, OutputRef};
use serde::Deserialize;
use std::str::FromStr;

use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
    pub policy_id_hex: Option<String>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
}

impl Reducer {
    fn get_token_amount(&self, utxo: &MultiEraOutput) -> i64 {
        match &self.config.policy_id_hex {
            None => utxo.lovelace_amount() as i64,
            Some(policy_id_hex) => {
                let mut total: i64 = 0;
                for asset in utxo.non_ada_assets().iter() {
                    if let Asset::NativeAsset(asset_cs, _, amount) = asset {
                        if &hex::encode(asset_cs) == policy_id_hex {
                            total += *amount as i64;
                        }
                    }
                }
                total
            }
        }
    }

    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutputRef,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(()),
        };

        let address = utxo.address().map(|addr| addr.to_string()).or_panic()?;

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}.{}", prefix, address),
            None => format!("{}.{}", "balance_by_address".to_string(), address),
        };

        let crdt = model::CRDTCommand::PNCounter(key, -1 * self.get_token_amount(&utxo));

        output.send(gasket::messaging::Message::from(crdt))?;

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        tx_output: &MultiEraOutput,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let address = tx_output.address().map(|x| x.to_string()).or_panic()?;

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}.{}", prefix, address),
            None => format!("{}.{}", "balance_by_address".to_string(), address),
        };

        let crdt = model::CRDTCommand::PNCounter(key, self.get_token_amount(&tx_output));

        output.send(gasket::messaging::Message::from(crdt))?;

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
                    self.process_consumed_txo(&ctx, &consumed, output)?;
                }

                for (_, produced) in tx.produces() {
                    self.process_produced_txo(&produced, output)?;
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
