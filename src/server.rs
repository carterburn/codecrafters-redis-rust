use anyhow::Result;
use tokio::net::TcpListener;

use crate::connection::RedisConnection;

pub struct Redis {
    /// TCP Listener on given port
    listener: TcpListener,
    // Clients connected -> should be join handles or arc of the clients?
}

impl Redis {
    pub async fn new(port: u16) -> Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(("127.0.0.1", port)).await?,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        println!("Serving clients");
        while let Ok((client_stream, client_addr)) = self.listener.accept().await {
            println!("New connection from: {client_addr}");

            let mut client = RedisConnection::new(client_stream, client_addr);

            tokio::spawn(async move { client.client_loop().await });
        }
        Ok(())
    }
}
