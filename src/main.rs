use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buf = [0u8; 4096];
                while let Ok(n) = stream.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    println!("{:?}", str::from_utf8(&buf[..n]));
                    let _ = stream.write_all(b"+PONG\r\n");
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
