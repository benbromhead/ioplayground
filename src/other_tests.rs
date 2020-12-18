use tokio::net::{TcpListener, TcpStream};
use crate::protocols::redis1::RedisCodec;
use tokio_util::codec::{Framed, FramedWrite, FramedRead};
use futures::{SinkExt, StreamExt};
use anyhow::Result;
use tokio::signal;
use redis_protocol::types::Frame;
use crate::protocols::redis2::{RespParser, RedisValueRef};


//1 core == 257k (./src/redis-benchmark -n 100000000 -t get -P 4 -r 10000 -e)
//4 core == 520k (./src/redis-benchmark -n 100000000 -t get -P 4 -r 10000 -e) - Redis CPU maxed!
async fn async_mpsc() -> Result<()> {
    let mut listener = TcpListener::bind("127.0.0.1:6379".to_string()).await.unwrap();

    loop {
        let codec = RedisCodec::new(false, 1);
        let socket = tokio::select! {
             s = listener.accept() => {
                s?.0
            },
            _ = signal::ctrl_c() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
        };
        let mut connection = Framed::new(socket, codec.clone());
        let (in_tx, mut in_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Frame>>();
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Frame>>();

        tokio::spawn(async move {
            while let Some(Ok(frame)) = connection.next().await {
                out_tx.send(frame).unwrap();
                if let Some(frame) = in_rx.recv().await {
                    connection.send(frame).await.unwrap()
                }
            }
        });


        tokio::spawn(async move {
            let outbound_stream = TcpStream::connect("172.20.0.2:6379".to_string()).await.unwrap();
            let codec = RedisCodec::new(true, 1);
            let mut outbound_connection = Framed::new(outbound_stream, codec);

            while let Some(frame) = out_rx.recv().await {
                outbound_connection.send(frame).await.unwrap();
                if let Some(Ok(frame)) = outbound_connection.next().await {
                    in_tx.send(frame).unwrap();
                }
            }
        });
    }
}


//1 core == 238k (./src/redis-benchmark -n 100000000 -t get -P 4 -r 10000 -e)
async fn async_mpsc_select() -> Result<()> {
    let mut listener = TcpListener::bind("127.0.0.1:6379".to_string()).await.unwrap();

    loop {
        let codec = RedisCodec::new(false, 1);
        let socket = tokio::select! {
             s = listener.accept() => {
                s?.0
            },
            _ = signal::ctrl_c() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
        };
        let mut connection = Framed::new(socket, codec.clone());
        let (in_tx, mut in_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Frame>>();
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Frame>>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(Ok(frame)) = connection.next() => {
                        out_tx.send(frame).unwrap();
                    },
                    Some(frame) = in_rx.recv() => {
                        connection.send(frame).await.unwrap()
                    },
                    else => break,
                }
            }
        });

        tokio::spawn(async move {
            let outbound_stream = TcpStream::connect("172.20.0.2:6379".to_string()).await.unwrap();
            let codec = RedisCodec::new(true, 1);
            let mut outbound_connection = Framed::new(outbound_stream, codec);

            loop {
                tokio::select! {
                    Some(Ok(frame)) = outbound_connection.next() => {
                        in_tx.send(frame).unwrap();
                    },
                    Some(frame) = out_rx.recv() => {
                        outbound_connection.send(frame).await.unwrap()
                    },
                    else => break,
                }
            }
        });
    }
}


//1 core == 152k (./src/redis-benchmark -n 100000000 -t get -P 4 -r 10000 -e)
//1 core == 82k (./src/redis-benchmark -n 100000000 -t get -P 1 -r 10000 -e) - No pipelined
async fn async_mpsc_select_redis2() -> Result<()> {
    let mut listener = TcpListener::bind("127.0.0.1:6379".to_string()).await.unwrap();

    loop {
        let socket = tokio::select! {
             s = listener.accept() => {
                s?.0
            },
            _ = signal::ctrl_c() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
        };
        let mut connection = Framed::new(socket, RespParser::default());
        let (in_tx, mut in_rx) = tokio::sync::mpsc::unbounded_channel::<RedisValueRef>();
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<RedisValueRef>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(Ok(frame)) = connection.next() => {
                        out_tx.send(frame).unwrap();
                    },
                    Some(frame) = in_rx.recv() => {
                        connection.send(frame).await.unwrap()
                    },
                    else => break,
                }
            }
        });

        tokio::spawn(async move {
            let outbound_stream = TcpStream::connect("172.20.0.2:6379".to_string()).await.unwrap();
            let codec = RespParser::default();
            let mut outbound_connection = Framed::new(outbound_stream, codec);

            loop {
                tokio::select! {
                    Some(Ok(frame)) = outbound_connection.next() => {
                        in_tx.send(frame).unwrap();
                    },
                    Some(frame) = out_rx.recv() => {
                        outbound_connection.send(frame).await.unwrap()
                    },
                    else => break,
                }
            }
        });
    }
}

//1 core == 259k ~ 260k (./src/redis-benchmark -n 100000000 -t get -P 4 -r 10000 -e)
async fn async_main() -> Result<()> {
    let mut listener = TcpListener::bind("127.0.0.1:6379".to_string()).await.unwrap();

    loop {
        let codec = RedisCodec::new(false, 1);
        let socket: TcpStream = tokio::select! {
             s = listener.accept() => {
                s?.0
            },
            _ = signal::ctrl_c() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
        };
        let split = socket.into_split();
        // let mut connection = Framed::new(socket, codec.clone());
        tokio::spawn(async move {
            let (in_w, in_r) = (FramedWrite::new(split.1, codec.clone()), FramedRead::new(split.0, codec.clone()));
            let mut temp = TcpStream::connect("172.20.0.2:6379".to_string()).await.unwrap();
            let outbound_stream = temp.split();
            let codec = RedisCodec::new(true, 1);
            let (out_w, out_r) = (FramedWrite::new(outbound_stream.1, codec.clone()), FramedRead::new(outbound_stream.0, codec.clone()));

            let _ = tokio::join!(
               in_r.forward(out_w),
               out_r.forward(in_w)
            );
        });
    }
}