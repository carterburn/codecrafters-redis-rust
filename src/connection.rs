use futures::{SinkExt, StreamExt};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::{
    resp::{codec::RespFrame, RespValue},
    server::types::Value,
};

/// A type representing an active client connection
pub(crate) struct RedisConnection {
    /// Client address
    client_addr: SocketAddr,

    /// Frame to read and write data to the client
    frame: Framed<TcpStream, RespFrame>,

    /// Reference to the global key / value store
    db: Arc<RwLock<HashMap<String, Value>>>,
    // big question here is would it be better to have this serialized through channels? i.e. have
    // a single channel I ask for a key for...? we'll see
}

impl RedisConnection {
    pub(crate) fn new(
        stream: TcpStream,
        client_addr: SocketAddr,
        db: Arc<RwLock<HashMap<String, Value>>>,
    ) -> Self {
        Self {
            client_addr,
            frame: Framed::new(stream, RespFrame),
            db,
        }
    }

    pub(crate) async fn client_loop(&mut self) {
        while let Some(result) = self.frame.next().await {
            match result {
                Ok(message) => {
                    println!("Received RESP value: {message:?}");
                    self.handle_message(message).await;
                }
                Err(e) => {
                    println!("Received error while decoding message: {e:?}");
                    break;
                }
            }
        }
    }

    async fn handle_message(&mut self, message: RespValue) {
        let RespValue::Array(array) = message else {
            println!("Invalid message from client: {message:?}");
            return;
        };

        let Some(RespValue::BulkString(cmd)) = array.first() else {
            println!("Invalid initial type in array: {array:?}");
            return;
        };

        self.handle_cmd(cmd.to_ascii_uppercase(), &array[1..]).await
    }

    async fn handle_cmd(&mut self, cmd: String, args: &[RespValue]) {
        match cmd.as_str() {
            "PING" => {
                // respond to ping with simple string pong
                let _ = self
                    .frame
                    .send(RespValue::SimpleString("PONG".to_string()))
                    .await;
            }
            "ECHO" => {
                // respond to echo with the string sent by the client
                if args.is_empty() {
                    println!("No args for ECHO command!");
                    return;
                }
                let _ = self.frame.send(args[0].clone()).await;
            }
            "SET" => {
                if args.len() < 2 {
                    println!("Invalid number of args for SET command: {args:?}");
                    return;
                }
                let RespValue::BulkString(key) = &args[0] else {
                    println!("Invalid RESP type for SET command key: {:?}", args[0]);
                    return;
                };
                let RespValue::BulkString(value) = &args[1] else {
                    println!("Invalid RESP type for SET command value: {:?}", args[1]);
                    return;
                };

                // TODO: this is where we'd likely need to implement a parser for this specific
                // command as well, but for now we'll do it the bad way
                let expiration = if args.len() == 4 {
                    let RespValue::BulkString(expiry) = &args[2] else {
                        println!(
                            "Invalid RESP type for SET command (expiration): {:?}",
                            args[2]
                        );
                        return;
                    };
                    let RespValue::BulkString(duration) = &args[3] else {
                        println!(
                            "Invalid RESP type for SET command (expiration): {:?}",
                            args[3]
                        );
                        return;
                    };
                    let Ok(time) = duration.parse() else {
                        println!("Invalid number for duration time: {duration}");
                        return;
                    };
                    Some(match expiry.to_ascii_uppercase().as_str() {
                        "PX" => Instant::now() + Duration::from_millis(time),
                        "EX" => Instant::now() + Duration::from_secs(time),
                        _ => {
                            println!("Invalid key expiration argument: {expiry}");
                            return;
                        }
                    })
                } else {
                    None
                };

                {
                    let Ok(mut db) = self.db.write() else {
                        println!("DB lock poisoned!");
                        return;
                    };
                    // NOTE: for now, we overwrite no matter what! so use insert. entry API could be
                    // used if we have to start making decisions on whether or not we change up the
                    // insertion
                    let _ = db.insert(key.to_string(), Value::new(value.to_string(), expiration));
                }

                // reply with simple OK
                let _ = self
                    .frame
                    .send(RespValue::SimpleString("OK".to_string()))
                    .await;
            }
            "GET" => {
                if args.is_empty() {
                    println!("No args for GET command: {args:?}");
                    return;
                }
                let RespValue::BulkString(key) = &args[0] else {
                    println!("Invalid RESP type for GET command key: {:?}", args[0]);
                    return;
                };

                let reply = {
                    let Ok(db) = self.db.read() else {
                        println!("DB lock poisoned");
                        return;
                    };
                    match db.get(key) {
                        Some(v) => {
                            // TODO: we either need to grab a write handle to the DB or have a
                            // background task that will expire keys for us
                            if v.expired(Instant::now()) {
                                RespValue::NullBulkString
                            } else {
                                RespValue::BulkString(v.get_value())
                            }
                        }
                        None => RespValue::NullBulkString,
                    }
                };

                let _ = self.frame.send(reply).await;
            }
            _ => {
                println!("Unsupported command: {cmd:?}");
                let _ = self
                    .frame
                    .send(RespValue::SimpleError("Unsupported command".to_string()))
                    .await;
            }
        }
    }
}
