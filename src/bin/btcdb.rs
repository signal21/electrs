extern crate error_chain;
#[macro_use]
extern crate log;

extern crate electrs;

use error_chain::ChainedError;
use std::process;
use std::sync::Arc;

use electrs::{config::Config, errors::*, new_index::Store};

fn run_script(config: Arc<Config>) -> Result<()> {
    let store = Arc::new(Store::open(&config.db_path.join("newindex"), &config, true));

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
