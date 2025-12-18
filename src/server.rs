use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use tokio::net::TcpListener;

use crate::{connection::RedisConnection, server::types::Value};

pub(crate) mod types;

pub struct Redis {
    /// TCP Listener on given port
    listener: TcpListener,
    // Clients connected -> should be join handles or arc of the clients?
    /// The global key/value store
    db: Arc<RwLock<HashMap<String, Value>>>,
}

impl Redis {
    pub async fn new(port: u16) -> Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(("127.0.0.1", port)).await?,
            db: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        println!("Serving clients");
        while let Ok((client_stream, client_addr)) = self.listener.accept().await {
            println!("New connection from: {client_addr}");

            let mut client = RedisConnection::new(client_stream, client_addr, self.db.clone());

            tokio::spawn(async move { client.client_loop().await });
        }
        Ok(())
    }
}
