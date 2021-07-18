use kvs::{KvStore, KvStoreError};
use std::process::exit;
use structopt::StructOpt;

fn main() {
    let opt: Opt = Opt::from_args();
    let mut store = KvStore::open(std::env::current_dir().unwrap()).unwrap();
    match opt.cmd {
        Command::Get { key } => {
            if let Some(value) = store.get(key).unwrap() {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Set { key, value } => {
            store.set(key, value).unwrap();
        }
        Command::Remove { key } => {
            if let Err(error) = store.remove(key) {
                if let KvStoreError::KeyNotFound { key: _ } = error {
                    println!("Key not found");
                }
                exit(1);
            };
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = env!("CARGO_PKG_NAME"),
    version = env!("CARGO_PKG_VERSION"),
    author = env!("CARGO_PKG_AUTHORS"),
    about = env!("CARGO_PKG_DESCRIPTION"),
)]
struct Opt {
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name = "set", about = "Set <value> in <key>")]
    Set { key: String, value: String },
    #[structopt(name = "get", about = "Get value in <key>")]
    Get { key: String },
    #[structopt(name = "rm", about = "Remove <key>")]
    Remove { key: String },
}
