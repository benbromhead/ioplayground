use tokio::runtime;
use tokio::net::{TcpListener, TcpStream};
use ioplayground::protocols::redis1::RedisCodec;
use tokio_util::codec::{Framed};
use futures::{SinkExt, StreamExt};
use anyhow::Result;
use tokio::signal;
use glommio::LocalExecutorBuilder;
use futures::{AsyncRead, AsyncWrite};
use std::io;
use std::pin::Pin;
use futures::io::Error;
use futures::task::{Context, Poll};
use glommio::Task;
use clap::Clap;

use tokio::prelude::{AsyncRead as TAsyncRead, AsyncWrite as TAsyncWrite};


//1 core == 264k (./src/redis-benchmark -n 100000000 -t get -P 4 -r 10000 -e) - redis1 codec
//4 core == 500k (./src/redis-benchmark -n 100000000 -t get -P 4 -r 10000 -e) - redis1 codec Redis CPU maxed!
//1 core == 83k (./src/redis-benchmark -n 100000000 -t get -P 1 -r 10000 -e) - No pipelined
async fn async_basic(upstream_connection_string: String) -> Result<()> {
    let mut listener = TcpListener::bind("127.0.0.1:6379".to_string()).await.unwrap();

    loop {
        let codec = RedisCodec::new(false, 1);
        let upstream_connection_string= upstream_connection_string.clone();
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
        tokio::spawn(async move {
            let outbound_stream = TcpStream::connect(upstream_connection_string.clone()).await.unwrap();
            let codec = RedisCodec::new(true, 1);
            let mut outbound_connection = Framed::new(outbound_stream, codec);

            while let Some(Ok(frame)) = connection.next().await {
                outbound_connection.send(frame).await.unwrap();
                if let Some(Ok(frame)) = outbound_connection.next().await {
                    connection.send(frame).await.unwrap()
                }
            }
        });
    }
}


#[derive(Debug)]
pub struct TokioGlomStream {
    pub stream: glommio::net::TcpStream,
}

impl AsyncWrite for TokioGlomStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        // Pin::new(self.).stream.poll_write(cx, buf)
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.stream).poll_close(cx)
    }
}

impl AsyncRead for TokioGlomStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl TAsyncRead for TokioGlomStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl TAsyncWrite for TokioGlomStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.stream).poll_close(cx)
    }
}

async fn async_basic_glommio(upstream_connection_string: String) -> Result<()> {
    let listener = glommio::net::TcpListener::bind("127.0.0.1:6379".to_string()).unwrap();
    // let mut listener = glommioTcpListener::bind("127.0.0.1:6379".to_string()).await.unwrap();

    loop {
        let codec = RedisCodec::new(false, 1);
        let upstream_connection_string= upstream_connection_string.clone();

        let socket = listener.accept().await?;
        let mut connection = Framed::new(TokioGlomStream { stream: socket }, codec.clone());

        Task::local(async move {
            let outbound_stream = glommio::net::TcpStream::connect(upstream_connection_string).await.unwrap();
            let codec = RedisCodec::new(true, 1);
            let mut outbound_connection = Framed::new(TokioGlomStream { stream:outbound_stream}, codec);

            while let Some(Ok(frame)) = connection.next().await {
                outbound_connection.send(frame).await.unwrap();
                if let Some(Ok(frame)) = outbound_connection.next().await {
                    connection.send(frame).await.unwrap()
                }
            }
        }).detach();
    }
}


#[derive(Clap)]
#[clap(version = "0.0.1", author = "Ben")]
struct ConfigOpts {
    #[clap(short, long, default_value = "tokio")]
    pub executor: String,

    #[clap(short, long, default_value = "1")]
    pub cores: usize,

    #[clap(short, long)]
    pub upstream: String,
}


fn main() {
    let params = ConfigOpts::parse();

    let tokio = params.executor == "tokio".to_string();

    if tokio {
        println!("Starting Tokio!");
        let mut rt = runtime::Builder::new()
            .enable_all()
            .thread_name("RPProxy-Thread")
            .threaded_scheduler()
            .core_threads(params.cores)
            .build()
            .unwrap();
        rt.block_on(async_basic(params.upstream)).unwrap();

    } else {
        println!("Starting Glommio!");
        LocalExecutorBuilder::new()
            .pin_to_cpu(params.cores)
            .spawn(|| async move {
                async_basic_glommio(params.upstream).await
            })
            .unwrap()
            .join().unwrap();
    }

    println!("Done!");
}
