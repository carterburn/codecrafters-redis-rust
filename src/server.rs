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
    server::types::{ExpiryEvent, RedisKey, Value},
};

pub(crate) mod types;

const INITIAL_CAPACITY: usize = 16;

pub struct Redis {
    /// TCP Listener on given port
    listener: TcpListener,
    // Clients connected -> should be join handles or arc of the clients?
    /// The global key/value store
    db: Arc<DashMap<RedisKey, Value>>,

    /// The channel to send expiration events on
    expiration_tx: Sender<ExpiryEvent>,
}

impl Redis {
    pub async fn new(port: u16) -> Result<Self> {
        let db = Arc::new(DashMap::with_capacity(INITIAL_CAPACITY));

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

    async fn key_expirer(db: Arc<DashMap<RedisKey, Value>>, mut expiry_rx: Receiver<ExpiryEvent>) {
        // binary min-heap to provide O(1) selection of next key to grab
        let mut expiry_queue: BinaryHeap<Reverse<(Instant, RedisKey)>> =
            BinaryHeap::with_capacity(INITIAL_CAPACITY);

        // loop over events received on channel for expiration updates or the timeout
        loop {
            let next_expiry = expiry_queue.peek().map(|Reverse((time, _))| *time);

            tokio::select! {
                Some(event) = expiry_rx.recv() => {
                    tracing::info!("Received new expiration event: {event:?}");
                    // we should clear out any instances of event.1 (the key) in the queue, then
                    // add a new one so we don't have a weird instance of expiring a key early from
                    // a previous set wait event
                    // Ex: if we SET foo bar EX 10 and wait 5.1 seconds then SET foo bar EX 5, it
                    // should expire 5 seconds after the last SET, not with the SET foo bar EX 10
                    // original timeline
                    expiry_queue.retain(|Reverse(evt)| evt.1 != event.1);
                    expiry_queue.push(Reverse(event));
                },
                _ = async {
                    if let Some(time) = next_expiry {
                        tracing::info!("Watching on a specific key");
                        sleep_until(tokio::time::Instant::from_std(time)).await;
                    } else {
                        tracing::info!("No keys that will expire! Waiting forever");
                        std::future::pending::<()>().await;
                    }
                } => {
                    let now = Instant::now();
                    while let Some(Reverse(exp_evt)) = expiry_queue.peek() {
                        if exp_evt.0 > now {
                            // done, we've processed all events
                            break;
                        }
                        // we know it is expired now, so remove the key
                        let key = expiry_queue.pop().unwrap().0.1;
                        db.remove(&key);
                        tracing::info!("Expired key: {key:?}");
                    }
                }
            }
        }
    }
}
