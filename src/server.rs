use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc, time::Instant};

use anyhow::Result;
use dashmap::DashMap;
use tokio::{
    net::TcpListener,
    sync::mpsc::{Receiver, Sender},
    time::sleep_until,
};

use crate::{
    connection::RedisConnection,
    server::types::{Database, ExpiryEvent, RedisKey, Value, INITIAL_CAPACITY},
};

pub(crate) mod types;

pub struct Redis {
    /// TCP Listener on given port
    listener: TcpListener,
    // Clients connected -> should be join handles or arc of the clients?
    /// The global key/value store
    db: Arc<Database>,

    /// The channel to send expiration events on
    expiration_tx: Sender<ExpiryEvent>,
}

impl Redis {
    pub async fn new(port: u16) -> Result<Self> {
        let db = Arc::new(Database::new());

        // create task to expire keys
        let (tx, rx) = tokio::sync::mpsc::channel::<ExpiryEvent>(INITIAL_CAPACITY);
        tokio::spawn(Self::key_expirer(db.clone(), rx));

        Ok(Self {
            listener: TcpListener::bind(("127.0.0.1", port)).await?,
            db,
            expiration_tx: tx,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        tracing::info!("Serving clients");
        while let Ok((client_stream, client_addr)) = self.listener.accept().await {
            tracing::info!("New connection from: {client_addr}");

            let mut client = RedisConnection::new(
                client_stream,
                client_addr,
                self.db.clone(),
                self.expiration_tx.clone(),
            );

            tokio::spawn(async move { client.client_loop().await });
        }
        Ok(())
    }

    async fn key_expirer(db: Arc<Database>, mut expiry_rx: Receiver<ExpiryEvent>) {
        // binary min-heap to provide O(1) selection of next key to grab
        let mut expiry_queue: BinaryHeap<Reverse<(Instant, RedisKey)>> =
            BinaryHeap::with_capacity(INITIAL_CAPACITY);

        // loop over events received on channel for expiration updates or the timeout
        loop {
            let next_expiry = expiry_queue.peek().map(|Reverse((time, _))| *time);

            tokio::select! {
                Some(event) = expiry_rx.recv() => {
                    tracing::info!("Received new expiration event: {event:?}");
                    expiry_queue.push(Reverse(event));
                },
                _ = async {
                    if let Some(time) = next_expiry {
                        tracing::info!("Waiting until next expiration");
                        sleep_until(tokio::time::Instant::from_std(time)).await;
                    } else {
                        tracing::info!("No keys that will expire! Waiting forever");
                        std::future::pending::<()>().await;
                    }
                } => {
                    let now = Instant::now();
                    while let Some(Reverse(exp_evt)) = expiry_queue.peek() {
                        let expire_time = exp_evt.0;
                        if expire_time > now {
                            // done, we've processed all events
                            break;
                        }
                        // we know it is expired now, so remove the key if this event is one that
                        // matches the true value in the db
                        let key = expiry_queue.pop().unwrap().0.1;
                        let Some(true_exp) = db.get_key_expiration(&key) else {
                            continue;
                        };
                        if expire_time == true_exp {
                            // now we actually remove from the db, this is a real event
                            db.remove_key(&key);
                            tracing::info!("Expired key: {key:?}");
                        } else {
                            tracing::info!("Skipping key with stale expiration");
                        }
                    }
                }
            }
        }
    }
}
