extern crate error_chain;
#[macro_use]
extern crate log;

extern crate electrs;

use error_chain::ChainedError;
use rustyline::{error::ReadlineError, DefaultEditor, Editor};
use std::process;
use std::sync::Arc;

use electrs::{
    config::Config,
    daemon::Daemon,
    errors::*,
    metrics::Metrics,
    new_index::{ChainQuery, Store},
    signal::Waiter,
};

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

fn switch_line(line: &str, query: &Arc<ChainQuery>) -> Result<()> {
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
        _ => println!("Unknown command: {}", cmds[0]),
    }
    Ok(())
}

fn run_script(config: Arc<Config>) -> Result<()> {
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
    ));

    let mut rl = DefaultEditor::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        match rl.readline(">> ") {
            Ok(line) => {
                println!("Line: {}", line);
                match switch_line(line.as_str(), &chain) {
                    Ok(_) => {
                        rl.add_history_entry(line.as_str());
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

fn main() {
    let config = Arc::new(Config::from_args());
    if let Err(e) = run_script(config) {
        error!("server failed: {}", e.display_chain());
        process::exit(1);
    }
}
