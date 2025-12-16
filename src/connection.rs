use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::resp::{codec::RespFrame, RespValue};

/// A type representing an active client connection
pub(crate) struct RedisConnection {
    /// Client address
    client_addr: SocketAddr,

    /// Frame to read and write data to the client
    frame: Framed<TcpStream, RespFrame>,
}

impl RedisConnection {
    pub(crate) fn new(stream: TcpStream, client_addr: SocketAddr) -> Self {
        Self {
            client_addr,
            frame: Framed::new(stream, RespFrame),
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
