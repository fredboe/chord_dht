use chord::dht_demo::SimpleChordDHT;
use std::error::Error;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Joining the network.");

    let own_ip = std::env::var("OWN_IP").unwrap();
    println!("own ip: {}", own_ip);
    let dht_handle = SimpleChordDHT::new_network(own_ip.parse().unwrap()).await?;

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
    //dht_handle2.leave().await?;
    dht_handle.leave().await?;
    Ok(())
}
