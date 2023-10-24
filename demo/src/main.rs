use anyhow::{anyhow, Result};
use chord::dht_demo::SimpleChordDHT;
use std::error::Error;
use std::net::IpAddr;
use std::process::Command;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Joining the network.");

    let own_ip = retrieve_own_ip()?;
    println!("own ip: {}", own_ip);
    let dht_handle = SimpleChordDHT::new_network(own_ip).await?;

    dht_handle.put("A".to_string(), "a".to_string()).await?;
    dht_handle.put("B".to_string(), "b".to_string()).await?;
    dht_handle.put("C".to_string(), "c".to_string()).await?;
    dht_handle.put("D".to_string(), "d".to_string()).await?;

    let value = dht_handle.lookup("B").await;
    println!("The value was: {:?}.", value);

    let value = dht_handle.lookup("D").await;
    println!("The value was: {:?}.", value);

    let value = dht_handle.lookup("a").await;
    println!("The value was: {:?}.", value);

    let value = dht_handle.lookup("Z").await;
    println!("The value was: {:?}.", value);

    println!("Leaving the network.");
    dht_handle.leave().await?;
    Ok(())
}

fn retrieve_own_ip() -> Result<IpAddr> {
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
