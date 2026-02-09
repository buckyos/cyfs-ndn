mod fs_daemon;

use log::error;
use std::env;
use std::path::PathBuf;

use crate::fs_daemon::{
    run_fs_daemon, FsDaemonRunOptions, DEFAULT_FS_DAEMON_CONFIG_PATH,
    DEFAULT_STORE_LAYOUT_CONFIG_PATH,
};

fn usage() -> String {
    format!(
        "usage: fs_daemon <mountpoint> [--store-config <path>] [--service-config <path>]\n\
         defaults:\n\
         --store-config {}\n\
         --service-config {}",
        DEFAULT_STORE_LAYOUT_CONFIG_PATH, DEFAULT_FS_DAEMON_CONFIG_PATH
    )
}

fn parse_args() -> Result<FsDaemonRunOptions, String> {
    let args = env::args().skip(1).collect::<Vec<String>>();
    if args.is_empty() || args[0] == "-h" || args[0] == "--help" {
        return Err(usage());
    }

    let mountpoint = PathBuf::from(args[0].clone());
    let mut store_config_path = PathBuf::from(DEFAULT_STORE_LAYOUT_CONFIG_PATH);
    let mut service_config_path = PathBuf::from(DEFAULT_FS_DAEMON_CONFIG_PATH);

    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--store-config" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "missing value for --store-config".to_string())?;
                store_config_path = PathBuf::from(value);
            }
            "--service-config" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "missing value for --service-config".to_string())?;
                service_config_path = PathBuf::from(value);
            }
            other => {
                return Err(format!("unknown argument: {}\n{}", other, usage()));
            }
        }
        i += 1;
    }

    Ok(FsDaemonRunOptions {
        mountpoint,
        store_config_path,
        service_config_path,
    })
}

fn main() {
    env_logger::init();
    let options = match parse_args() {
        Ok(v) => v,
        Err(msg) => {
            eprintln!("{}", msg);
            std::process::exit(1);
        }
    };

    if let Err(err) = run_fs_daemon(options) {
        error!("run fs_daemon failed: {}", err);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod fs_daemon_tests;
