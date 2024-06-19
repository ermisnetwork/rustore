use anyhow::Result;
use clap::{arg, command, Parser};
use rustore_server::server::Server;
use rustore_storage::{fjall_database::FjallDatabase, sled_database::SledDatabase, Database};
use serde::Deserialize;

#[derive(Parser, Debug, Deserialize)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    listen_addr: String,
    #[arg(short, long)]
    root_path: String,
    #[arg(short, long)]
    secret: String,
    #[arg(short, long)]
    workers: Option<usize>,
}

fn main() -> Result<()> {
    let args = match std::fs::read_to_string("service.yaml") {
        Ok(content) => {
            let args: Args = serde_yaml::from_str(&content)?;
            args
        }
        Err(_) => {
            println!("service.yaml not found, using command line arguments");
            let args = Args::parse();
            args
        }
    };
    let workers = args.workers;
    let db = SledDatabase::open_or_create(args.root_path.clone())?;
    let piped_server = Server::new(
        args.listen_addr.parse()?,
        args.secret.as_str(),
        db
    )?;
    piped_server.serve(workers)?;
    Ok(())
}