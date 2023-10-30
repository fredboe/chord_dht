mod storage_rpc {
    tonic::include_proto!("storage");
}

mod dht_demo;
mod storage;

use crate::dht_demo::SimpleChordDHT;
use anyhow::{anyhow, Result};
use rand::Rng;
use std::error::Error;
use std::net::IpAddr;
use std::process::Command;
use std::time::Duration;

// replace u64 and 64 with specified types
// write tests
// later add a r successor list to mitigate node failures
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    log::info!("Own ip: {:?}", own_ip_from_command());

    let node_type = std::env::var("NODE_TYPE")?;
    log::info!("The node type is {}.", node_type);
    if node_type == "introducer" {
        demo_procedure_for_introducer_node().await?;
    } else if node_type == "normal" {
        demo_procedure_for_normal_node().await?;
    } else {
        log::warn!("An unkown node type was given (choose between 'introducer' and 'normal'.");
    }

    Ok(())
}

async fn demo_procedure_for_introducer_node() -> Result<()> {
    let own_ip = ip_from_env().or(own_ip_from_command())?;
    log::info!("Creating a new network with the start ip {}.", own_ip);
    let _dht_handle = SimpleChordDHT::new_network(own_ip).await?;

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn demo_procedure_for_normal_node() -> Result<()> {
    let introducer_ip = ip_from_env()?;
    log::info!("Joining the network through {}.", introducer_ip);
    let dht_handle = SimpleChordDHT::join(introducer_ip).await?;

    let keys: Vec<String> = std::iter::repeat_with(|| generate_3_letter_string())
        .map(|key| key.to_uppercase())
        .take(3)
        .collect();
    let values: Vec<String> = keys.iter().map(|key| key.to_lowercase()).collect();

    tokio::time::sleep(Duration::from_secs(2)).await;

    for (key, value) in keys.iter().zip(values.iter()) {
        log::info!("Putting ({}, {}) to the dht.", key, value);
        dht_handle.put(key.clone(), value.clone()).await?;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    for key in keys.iter() {
        let optional_value = dht_handle.retry_lookup(&key, 5).await;
        log::info!("The lookup for {} was {:?}.", key, optional_value);
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    log::info!("Leaving the network.");
    dht_handle.leave().await?;

    Ok(())
}

fn ip_from_env() -> Result<IpAddr> {
    let env_ip = std::env::var("IP")?;
    let ip = env_ip.parse()?;
    Ok(ip)
}

fn own_ip_from_command() -> Result<IpAddr> {
    let output = Command::new("hostname").arg("-I").output()?.stdout;
    let output_string = String::from_utf8_lossy(&output);

    let first_ip_str = output_string
        .trim()
        .split_whitespace()
        .next()
        .ok_or(anyhow!("There was no ip available."))?;
    let ip = first_ip_str.parse()?;

    Ok(ip)
}

fn generate_3_letter_string() -> String {
    std::iter::repeat_with(|| generate_random_letter())
        .take(3)
        .collect()
}

fn generate_random_letter() -> char {
    let num = rand::thread_rng().gen_range(0..52);
    match num {
        0..=25 => (b'A' + num) as char,
        26..=51 => (b'a' + num - 26) as char,
        _ => unreachable!(),
    }
}
