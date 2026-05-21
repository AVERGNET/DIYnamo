use rocksdb::{Options, DB};

fn main() -> Result<(), rocksdb::Error> {
    let path = "./data/db";
    let db = DB::open_default(path)?;

    db.put(b"my-key", b"my-value")?;
    match db.get(b"my-key")? {
        Some(value) => println!("value: {:?}", value),
        None => println!("key not found"),
    }
    Ok(())
}