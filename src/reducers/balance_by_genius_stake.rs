use pallas::codec::utils::CborWrap;
use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::{
    Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
};
use pallas::ledger::primitives::alonzo::{Constr, PlutusData};
use pallas::ledger::primitives::babbage::DatumOption;
use pallas::ledger::traverse::ComputeHash;
use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{Asset, MultiEraBlock, OutputRef};
use pallas::network::miniprotocols::Point;
use serde::Deserialize;
use std::collections::HashMap;
use std::ops::Deref;

use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: String,
    pub filter: Option<crosscut::filters::Predicate>,
    pub policy_id_hex: Option<String>,
    pub script_address: String,
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

    // VERY naive
    fn datum_to_address(datum: &PlutusData) -> Option<Address> {
        let fields = get_constr(datum)?;
        // TODO: Check constr tags

        let addr_field = fields.fields.get(1)?;
        let addr_constr = get_constr(addr_field)?;

        // Spending

        let spending_field = addr_constr.fields.get(0)?;
        let spending_constr = get_constr(spending_field)?;

        let spending_bytes = get_bytes(spending_constr)?;
        let spending_part = mk_pub_key_spend_part(spending_bytes.clone())?;

        // Staking

        let maybe_staking_field = addr_constr.fields.get(1)?;
        let maybe_staking_constr = get_constr(maybe_staking_field)?;

        let staking_cred_field = maybe_staking_constr.fields.get(0)?;
        let staking_cred_constr = get_constr(staking_cred_field)?;

        let staking_field = staking_cred_constr.fields.get(0)?;
        let staking_constr = get_constr(staking_field)?;

        let staking_bytes = get_bytes(staking_constr)?;
        let stakeing_part = mk_pub_key_stake_part(staking_bytes.clone())?;

        // TODO: Don't hardcode network id
        let addr = ShelleyAddress::new(Network::Mainnet, spending_part, stakeing_part);
        Some(Address::Shelley(addr))
    }

    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        block: &MultiEraBlock,
        input: &OutputRef,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let point = Point::Specific(block.slot(), block.hash().to_vec());
        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(()),
        };

        let address = utxo.address().map(|addr| addr.to_string()).or_panic()?;
        if self.config.script_address != address {
            return Ok(());
        }

        let owner = match utxo.datum() {
            Some(DatumOption::Data(CborWrap(datum))) => Self::datum_to_address(&datum),
            Some(DatumOption::Hash(hash)) => {
                let datums = get_datums(block);
                let datum = datums.get(&hash);
                match datum {
                    Some(datum) => Self::datum_to_address(datum),
                    None => None,
                }
            }
            None => None,
        };
        log::debug!("Found Genius stake owner: {:?}", owner);

        match owner {
            Some(address) => {
                let owner = address.to_string();

                let prefix = self.config.key_prefix.clone();

                let delta = self.get_tokens_amount(&utxo);
                if delta != 0 {
                    let crdt =
                        model::CRDTCommand::voting_power_change(owner, prefix, -1 * delta, point);
                    output.send(gasket::messaging::Message::from(crdt))?;
                }
                return Ok(());
            }
            None => {
                return Ok(());
            }
        }
    }

    fn process_produced_txo(
        &mut self,
        block: &MultiEraBlock,
        tx_output: &MultiEraOutput,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let point = Point::Specific(block.slot(), block.hash().to_vec());

        let address = tx_output
            .address()
            .map(|addr| addr.to_string())
            .or_panic()?;
        if self.config.script_address != address {
            return Ok(());
        }

        let owner = match tx_output.datum() {
            Some(DatumOption::Data(CborWrap(datum))) => Self::datum_to_address(&datum),
            Some(DatumOption::Hash(hash)) => {
                let datums = get_datums(block);
                let datum = datums.get(&hash);
                match datum {
                    Some(datum) => Self::datum_to_address(datum),
                    None => None,
                }
            }
            None => None,
        };
        log::warn!("Found Genius stake owner: {:?}", owner);
        match owner {
            Some(address) => {
                let owner = address.to_string();

                let prefix = self.config.key_prefix.clone();

                let delta = self.get_tokens_amount(&tx_output);
                if delta != 0 {
                    let crdt = model::CRDTCommand::voting_power_change(owner, prefix, delta, point);
                    output.send(gasket::messaging::Message::from(crdt))?;
                }
            }
            None => {}
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
                    self.process_consumed_txo(&ctx, &block, &consumed, output)?;
                }

                for (_, produced) in tx.produces() {
                    self.process_produced_txo(&block, &produced, output)?;
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

        super::Reducer::BalanceByGeniusStake(reducer)
    }
}

fn get_datums(block: &MultiEraBlock) -> HashMap<Hash<32>, PlutusData> {
    let witnesses: Vec<PlutusData> = match &block {
        MultiEraBlock::Babbage(babbage) => babbage
            .transaction_witness_sets
            .iter()
            .map(|w| w.clone().unwrap())
            .flat_map(|w| w.plutus_data)
            .flat_map(|w| w)
            .map(|w| w.unwrap())
            .collect(),
        MultiEraBlock::AlonzoCompatible(alonzo, _) => alonzo
            .transaction_witness_sets
            .iter()
            .map(|w| w.clone().unwrap())
            .flat_map(|w| w.plutus_data)
            .flat_map(|w| w)
            .map(|w| w.unwrap())
            .collect(),
        _ => vec![],
    };
    let mut datums = HashMap::new();
    for witness in witnesses {
        let hash = witness.compute_hash();
        datums.insert(hash, witness);
    }
    datums
}

fn get_constr(data: &PlutusData) -> Option<&Constr<PlutusData>> {
    match data {
        PlutusData::Constr(c) => Some(c),
        _ => None,
    }
}

fn get_bytes(data: &Constr<PlutusData>) -> Option<&Vec<u8>> {
    match &data.fields[0] {
        PlutusData::BoundedBytes(b) => Some(b.deref()),
        _ => None,
    }
}

fn mk_pub_key_spend_part(bytes: Vec<u8>) -> Option<ShelleyPaymentPart> {
    let arr: [u8; 28] = bytes.try_into().ok()?;
    let hash: Hash<28> = Hash::new(arr);
    let payment_part = ShelleyPaymentPart::key_hash(hash);
    Some(payment_part)
}

fn mk_pub_key_stake_part(bytes: Vec<u8>) -> Option<ShelleyDelegationPart> {
    let arr: [u8; 28] = bytes.try_into().ok()?;
    let hash: Hash<28> = Hash::new(arr);
    let stake_part = ShelleyDelegationPart::Key(hash);
    Some(stake_part)
}
