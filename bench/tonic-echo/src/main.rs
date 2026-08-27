use std::pin::Pin;
use std::time::Instant;

use futures::Stream;
use tokio::net::TcpListener;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

pub mod echo {
    tonic::include_proto!("echo");
}

use echo::echo_client::EchoClient;
use echo::echo_server::{Echo, EchoServer};
use echo::{EchoRequest, EchoResponse};

type ChatStream = Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;

#[derive(Default)]
struct EchoService;

#[tonic::async_trait]
impl Echo for EchoService {
    async fn say(
        &self,
        request: Request<EchoRequest>,
    ) -> Result<Response<EchoResponse>, Status> {
        Ok(Response::new(EchoResponse {
            message: request.into_inner().message,
        }))
    }

    type SplitStream = ChatStream;

    async fn split(
        &self,
        request: Request<EchoRequest>,
    ) -> Result<Response<Self::SplitStream>, Status> {
        let words = request.into_inner().message;
        let replies: Vec<Result<EchoResponse, Status>> = words
            .split_whitespace()
            .map(|word| {
                Ok(EchoResponse {
                    message: word.to_string(),
                })
            })
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(replies))))
    }

    async fn join(
        &self,
        request: Request<Streaming<EchoRequest>>,
    ) -> Result<Response<EchoResponse>, Status> {
        let mut inbound = request.into_inner();
        let mut message = String::new();
        while let Some(item) = inbound.message().await? {
            message.push_str(&item.message);
        }
        Ok(Response::new(EchoResponse { message }))
    }

    type ChatStream = ChatStream;

    async fn chat(
        &self,
        request: Request<Streaming<EchoRequest>>,
    ) -> Result<Response<Self::ChatStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if tx
                    .send(Ok(EchoResponse {
                        message: item.message,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn percentile_ns(mut samples: Vec<u128>, p: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let rank = ((p / 100.0) * samples.len() as f64).ceil() as usize;
    samples[rank.clamp(1, samples.len()) - 1]
}

fn mean_ns(samples: &[u128]) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    samples.iter().sum::<u128>() / samples.len() as u128
}

fn parse_iters() -> usize {
    let mut iters = 200usize;
    for arg in std::env::args().skip(1) {
        if arg == "--smoke" {
            return 5;
        }
        if let Some(value) = arg.strip_prefix("--iters=") {
            if let Ok(parsed) = value.parse() {
                iters = parsed;
            }
        }
    }
    iters
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let iters = parse_iters();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        Server::builder()
            .add_service(EchoServer::new(EchoService))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = EchoClient::connect(format!("http://{addr}")).await?;
    let small = EchoRequest {
        message: "hello bench".into(),
    };
    let big = EchoRequest {
        message: "x".repeat(65536),
    };
    client.say(small.clone()).await?;
    client.say(small.clone()).await?;

    let mut unary_small = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        client.say(small.clone()).await?;
        unary_small.push(start.elapsed().as_nanos());
    }
    let mut unary_big = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        client.say(big.clone()).await?;
        unary_big.push(start.elapsed().as_nanos());
    }
    let mut bidi = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut stream = client
            .chat(Request::new(ReceiverStream::new(rx)))
            .await?
            .into_inner();
        for i in 0..20 {
            tx.send(EchoRequest {
                message: i.to_string(),
            })
            .await?;
            if stream.message().await?.is_none() {
                return Err("tonic bidi ended early".into());
            }
        }
        drop(tx);
        bidi.push(start.elapsed().as_nanos());
    }

    println!(
        "{{\"impl\":\"tonic\",\"iters\":{iters},\"unary_11b\":{{\"mean_ns\":{},\"p99_ns\":{}}},\"unary_64kib\":{{\"mean_ns\":{},\"p99_ns\":{}}},\"bidi_x20\":{{\"mean_ns\":{},\"p99_ns\":{}}}}}",
        mean_ns(&unary_small),
        percentile_ns(unary_small, 99.0),
        mean_ns(&unary_big),
        percentile_ns(unary_big, 99.0),
        mean_ns(&bidi) / 20,
        percentile_ns(bidi, 99.0) / 20
    );
    Ok(())
}
