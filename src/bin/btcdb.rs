extern crate error_chain;
#[macro_use]
extern crate log;

extern crate electrs;

use arrow::{
    array::{ArrayRef, BinaryArray, RecordBatch, UInt32Array},
    datatypes::{DataType, Field, Schema},
};
use bitcoin::{hashes::Hash, Transaction, Txid};
use error_chain::ChainedError;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use rustyline::{error::ReadlineError, DefaultEditor};
use std::process;
use std::sync::Arc;

use bitcoin::consensus::encode::deserialize;
use electrs::{
    chain,
    config::Config,
    daemon::Daemon,
    errors::*,
    metrics::Metrics,
    new_index::{compute_script_hash, ChainQuery, Store},
    signal::Waiter,
    util::{
        partitions::{block_batch, tx_batch, BtcPartition, BtcPartitionData, Partitioner},
        s3::CloudStorage,
    },
};
use hex::DisplayHex;
use std::str::FromStr;

fn split_line_to_cmds(line: &str) -> Vec<String> {
    let mut cmds = Vec::new();
    let mut cmd = String::new();
    let mut in_quote = false;
    for c in line.chars() {
        match c {
            ' ' if !in_quote => {
                if !cmd.is_empty() {
                    cmds.push(cmd);
                    cmd = String::new();
                }
            }
            '"' => {
                in_quote = !in_quote;
            }
            _ => {
                cmd.push(c);
            }
        }
    }
    if !cmd.is_empty() {
        cmds.push(cmd);
    }
    cmds
}

async fn switch_line(line: &str, query: &Arc<ChainQuery>) -> Result<()> {
    let cmds = split_line_to_cmds(line);
    if cmds.is_empty() {
        return Ok(());
    }
    match cmds[0].as_str() {
        "exit" => process::exit(0),
        "help" => {
            println!("Available commands:");
            println!("  exit");
            println!("  help");
            println!("  quit");
            println!("  stop");
            println!("  version");
        }
        "quit" => process::exit(0),
        "stop" => process::exit(0),
        "version" => println!("btcdb version {}", env!("CARGO_PKG_VERSION")),
        "height" => {
            let height = if cmds.len() > 1 {
                cmds[1]
                    .parse::<u32>()
                    .chain_err(|| "Invalid block height")?
            } else {
                query.best_height() as u32
            };
            println!("Block: {}", height);
        }
        "stats" => {
            if cmds.len() < 2 {
                return Err("Missing txid".into());
            }
            if let Ok(addr) = chain::address::Address::from_str(&cmds[1]) {
                let addr = addr.assume_checked();
                let script_hash = compute_script_hash(&addr.script_pubkey());
                let stats = query.stats(&script_hash);
                println!("Stats: {:?}", stats);
            } else {
                return Err("Invalid address".into());
            }
        }
        "rawtx" => {
            if cmds.len() < 2 {
                return Err("Missing txid".into());
            }
            if let Ok(txid) = Txid::from_str(&cmds[1]).chain_err(|| "Invalid txid") {
                if let Some(rawtx) = query.lookup_raw_txn(&txid, None) {
                    if let Ok(txn) = deserialize::<Transaction>(&rawtx) {
                        txn.input.iter().for_each(|i| {
                            i.witness.iter().for_each(|w| {
                                println!("Witness: {:?}", Vec::from(w).to_lower_hex_string())
                            });
                        });
                    } else {
                        println!("Failed to deserialize tx");
                    }
                }
            }
        }
        "tx" => {
            if cmds.len() < 2 {
                return Err("Missing txid".into());
            }
            if let Ok(txid) = Txid::from_str(&cmds[1]).chain_err(|| "Invalid txid") {
                if let Some(tx) = query.lookup_txn(&txid, None) {
                    println!("Tx: {:?}", tx);
                } else {
                    println!("Tx not found");
                }
            }
        }
        "block" => {
            if cmds.len() < 2 {
                return Err("Missing block hash".into());
            }
            if let Ok(height) = cmds[1].parse::<u32>() {
                if let Some(block_id) = query.blockid_by_height(height as usize) {
                    println!("Block: {:?}", block_id.hash);
                    if let Some(txids) = query.get_block_txids(&block_id.hash) {
                        txids.iter().for_each(|txid| {
                            println!("Txid: {}", txid);
                        });
                    }
                } else {
                    println!("Block not found");
                }
            } else if let Ok(blockhash) = chain::BlockHash::from_str(&cmds[1]) {
                let block = query.blockid_by_hash(&blockhash);
                println!("Block: {:?}", block);
            } else {
                return Err("Invalid block hash".into());
            }
        }
        "write-blocks" => {
            if cmds.len() < 3 {
                return Err("Missing block start, end and path".into());
            }
            let start = cmds[1]
                .parse::<u32>()
                .chain_err(|| "Invalid block height")?;
            let path = cmds[2].clone();
            write_blocks(query, start, &path).await?;
        }
        "write-txs" => {
            if cmds.len() < 4 {
                return Err("Missing tx start, end and path".into());
            }
            let start = cmds[1].parse::<u32>().chain_err(|| "Invalid tx height")?;
            let end = cmds[2].parse::<u32>().chain_err(|| "Invalid tx height")?;
            let path = cmds[3].clone();
            let client = CloudStorage::new()?;
            let mut partitioner = Partitioner::load_partitions(&client, &path, &path, 100).await?;
            for height in start..end {
                if let Some(block_id) = query.blockid_by_height(height as usize) {
                    let p: &mut BtcPartition =
                        if let Some(partition) = partitioner.get_partition(height).await? {
                            partition
                        } else {
                            let new_p = partitioner
                                .add_partition(height, BtcPartitionData::Tx)
                                .await?;
                            new_p
                        };
                    println!("Block: {:?}, partition {}", block_id.hash, p.filename());
                    if let Some(txids) = query.get_block_txids(&block_id.hash) {
                        let mut hashes = Vec::new();
                        txids.iter().for_each(|txid| {
                            hashes.push(txid.to_byte_array());
                        });
                        let batch = tx_batch(height, hashes)?;
                        p.write(batch)?;
                    }
                } else {
                    println!("Block not found");
                }
            }
            partitioner.close().await?;
        }
        _ => println!("Unknown command: {}", cmds[0]),
    }
    Ok(())
}

async fn run_script(config: Arc<Config>) -> Result<()> {
    let signal: Waiter = Waiter::start();

    let metrics = Metrics::new(config.monitoring_addr);
    metrics.start();

    let daemon = Arc::new(Daemon::new(
        &config.daemon_dir,
        &config.blocks_dir,
        config.daemon_rpc_addr,
        config.cookie_getter(),
        config.network_type,
        signal.clone(),
        &metrics,
    )?);

    let store = Arc::new(Store::open(&config.db_path.join("newindex"), &config, true));

    let chain = Arc::new(ChainQuery::new(
        Arc::clone(&store),
        Arc::clone(&daemon),
        &config,
        &metrics,
        true,
    ));

    let mut rl = DefaultEditor::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        match rl.readline(">> ") {
            Ok(line) => {
                println!("Line: {}", line);
                match switch_line(line.as_str(), &chain).await {
                    Ok(_) => {
                        rl.add_history_entry(line.as_str())?;
                    }
                    Err(e) => {
                        println!("Error: {}", e.display_chain());
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    rl.save_history("history.txt").unwrap();

    info!("btcdb completed");
    Ok(())
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let access_key = std::env::var("OCI_ACCESS_KEY").unwrap();
    println!("access key: {}", &access_key);
    let config = Arc::new(Config::from_args());
    if let Err(e) = run_script(config).await {
        error!("server failed: {}", e.display_chain());
        process::exit(1);
    }
}

async fn write_blocks(query: &Arc<ChainQuery>, start: u32, path: &str) -> Result<()> {
    let client = CloudStorage::new()?;
    let mut partitioner = Partitioner::load_partitions(&client, &path, &path, 100).await?;

    let mut heights = Vec::new();
    let mut hashes = Vec::new();
    let mut times = Vec::new();
    let mut sizes = Vec::new();
    let mut weights = Vec::new();
    let mut tx_counts = Vec::new();

    let p: &mut BtcPartition = partitioner
        .add_partition(start, BtcPartitionData::Block)
        .await?;

    let max_height = query.best_height() as u32;

    let mut count = 0;
    for height in start..max_height {
        if let Some(block_id) = query.blockid_by_height(height as usize) {
            if let Some(block_meta) = query.get_block_meta(&block_id.hash) {
                if let Some(block_header) = query.get_block_header(&block_id.hash) {
                    heights.push(height);
                    let hash = block_id.hash.as_raw_hash().to_byte_array();
                    hashes.push(hash);
                    times.push(block_header.time);
                    sizes.push(block_meta.size as u32);
                    weights.push(block_meta.weight as u32);
                    tx_counts.push(block_meta.tx_count as u32);
                    count += 1;
                }
            }
        }
    }
    println!("processed {} blocks", count);

    let batch = block_batch(heights, hashes, times, sizes, weights, tx_counts)?;
    p.write(batch)?;
    partitioner.close().await?;
    Ok(())
}
