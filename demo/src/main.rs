use anyhow::{anyhow, Result};
use chord::dht_demo::SimpleChordDHT;
use rand::Rng;
use std::error::Error;
use std::net::IpAddr;
use std::process::Command;
use std::time::Duration;

// change the chord events
// r successor list
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Joining the network.");

    let dht_handle = if std::env::var("MODE") == Ok("join".to_string()) {
        let introducer_ip = ip_from_env()?;
        SimpleChordDHT::join(introducer_ip).await
    } else {
        let own_ip = ip_from_env().or(own_ip_from_command())?;
        SimpleChordDHT::new_network(own_ip).await
    }?;

    for _ in 0..6 {
        let letter = generate_random_uppercase_letter();
        println!("Put {}", letter);
        dht_handle
            .put(letter.to_string(), letter.to_string().to_ascii_lowercase())
            .await?;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    for _ in 0..6 {
        let letter = generate_random_uppercase_letter();
        println!(
            "The lookup for {} was {:?}.",
            letter,
            dht_handle.lookup(&letter.to_string()).await
        );
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("Leaving the network.");
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

fn generate_random_uppercase_letter() -> char {
    rand::thread_rng().gen_range(b'A'..b'Z' + 1) as char
}
