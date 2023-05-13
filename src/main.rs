use clap::Parser;
use clap::Subcommand;
use futures::StreamExt;
use rand::prelude::*;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Parser)]
struct CLI {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Write(Write),
    Read(Read),
}

#[derive(Debug, Parser)]
struct Write {
    #[clap(long)]
    store: usize,

    #[clap(long)]
    files: usize,
}

#[derive(Debug, Parser)]
struct Read {
    #[clap(long)]
    files: usize,
}

#[tokio::main]
async fn main() {
    match CLI::parse().command {
        Command::Write(Write { store, files }) => {
            println!("Creating {files} files with {store} entries");
            (0..files)
                .map(|i| async move {
                    let mut file = tokio::fs::OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .append(false)
                        .write(true)
                        .open(format!("{}.data", i))
                        .await
                        .unwrap();

                    let randv = (0..store)
                        .map(|_| {
                            let mut rng = rand::thread_rng();
                            let i: u16 = rng.gen();
                            i
                        })
                        .collect::<Vec<_>>();

                    let json = serde_json::to_string(&randv).unwrap();

                    file.write_all(json.as_bytes()).await.unwrap();
                    file.sync_all().await.unwrap();
                })
                .collect::<futures::stream::FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await;
        }

        Command::Read(Read { files }) => {
            let sum: u64 = (0..files)
                .map(|i| async move {
                    let mut file = tokio::fs::OpenOptions::new()
                        .create(false)
                        .create_new(false)
                        .read(true)
                        .open(format!("{}.data", i))
                        .await
                        .unwrap();

                    let mut buffer = String::new();
                    file.read_to_string(&mut buffer).await.unwrap();
                    serde_json::from_str::<Vec<u64>>(&buffer).unwrap()
                })
                .collect::<futures::stream::FuturesUnordered<_>>()
                .collect::<Vec<Vec<u64>>>()
                .await
                .into_iter()
                .map(|iter| iter.into_iter())
                .flatten()
                .sum();

            println!("{sum}");
        }
    }
}
