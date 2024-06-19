use rustore_client::piped_client::Client;
use futures::StreamExt;
use std::sync::{atomic::AtomicUsize, Arc, Barrier};
use tokio::runtime::Runtime;
// use datastore_client::pooled_client::Client;

#[tokio::main]
async fn main() {
    // increment_test("daibo".to_string()).await;
    let client = Client::new("127.0.0.1:5555", "Belochat123", false, false)
        .await
        .unwrap();
    //input loop
    // let mut input = String::new();
    // loop {
    //     println!("Enter command: ");
    //     input.clear();
    //     std::io::stdin().read_line(&mut input).unwrap();
    //     let input = input.trim();
    //     if input.starts_with("put") {
    //         let split = input.split_whitespace().collect::<Vec<&str>>();
    //         let key = split[1].as_bytes();
    //         let value = split[2].as_bytes();
    //         client.put("daibo".as_bytes(), key, value).await.unwrap();
    //     } 
    //     else if input.starts_with("get") {
    //         let split = input.split_whitespace().collect::<Vec<&str>>();
    //         let key = split[1].as_bytes();
    //         let value = client.get("daibo".as_bytes(), key).await.unwrap().unwrap();
    //         println!("{:?}", value);
    //     } 
    //     else if input.starts_with("iter") {
    //         let split = input.split_whitespace().collect::<Vec<&str>>();
    //         iter(&client, split[1]).await;
    //     }
    //     else {
    //         println!("Invalid command");
    //     }
    // }
    let num = 1;
    let mut v = vec![];
    let barrier = Arc::new(Barrier::new(num + 1));
    for i in 0..num {
        let barrier = barrier.clone();
        let client = Client::new("127.0.0.1:5555", "Belochat123", false, false)
            .await
            .unwrap();
        let t = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            barrier.wait();
            let table = "daibo".to_string();
            put(&client, table, 100000, &rt);
        });
        v.push(t);
    }
    barrier.wait();
    let now = std::time::Instant::now();
    for t in v {
        t.join().unwrap();
    }
    println!("put {} in {:?}", num * 100000, now.elapsed());
    println!(
        "throughput: {:?}",
        num as f64 * 100000.0 / (now.elapsed().as_micros() as f64 / 1000000.0)
    );
}

async fn increment_test(table: String) {
    let iters = 100000;
    let client = Client::new("127.0.0.1:5555", "Belochat123", true, false)
        .await
        .unwrap();
    let now = std::time::Instant::now();
    let mut set = futures::stream::FuturesUnordered::new();
    for i in 0u64..iters {
        let client = client.clone();
        let table = table.clone();
        let t = tokio::spawn(async move {
            client
                .increment(
                    table.as_bytes(),
                    [1,2,3].as_slice(),
                ).await.unwrap();
            }
        );
        set.push(t);
    }
    while let Some(_) = set.next().await {}
    println!("{:?}", now.elapsed());
    let cur = client.get(table.as_bytes(), [1,2,3].as_slice()).await.unwrap().unwrap().to_vec();
    println!("{}", u64::from_be_bytes(cur.try_into().unwrap()));
    for i in 0u64..iters {
        let client = client.clone();
        let table = table.clone();
        let t = tokio::spawn(async move {
            client
                .decrement(
                    table.as_bytes(),
                    [1,2,3].as_slice(),
                ).await.unwrap();
            }
        );
        set.push(t);
    }
    while let Some(_) = set.next().await {}
    let cur = client.get(table.as_bytes(), [1,2,3].as_slice()).await.unwrap().unwrap().to_vec();
    println!("{}", u64::from_be_bytes(cur.try_into().unwrap()));
}

async fn subscribe_test(table: String) {
    let client = Client::new("127.0.0.1:5555", "Belochat123", true, false)
        .await
        .unwrap();
    let mut set = futures::stream::FuturesUnordered::new();
    for i in 0u64..100 {
        let client = client.clone();
        let table = table.clone();
        let t = tokio::spawn(async move {
            client
                .put(
                    table.as_bytes(),
                    i.to_be_bytes().as_slice(),
                    chksum_hash_md5::hash(i.to_be_bytes()).as_bytes(),
                ).await.unwrap();
            }
        );
        set.push(t);
    }
    while let Some(_) = set.next().await {}
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    println!("{}", client.cache.unwrap().len());
}
async fn get_many(client: &Client, table: String, keys: u64) {
    let now = std::time::Instant::now();
    let mut set = tokio::task::JoinSet::new();
    let wrong_value = Arc::new(AtomicUsize::new(0));
    for t_id in 0u64..keys {
        let wrong_value = wrong_value.clone();
        let client = client.clone();
        let table = table.clone();
        set.spawn(async move {
            let res = client
                .get(&table.as_bytes(), t_id.to_be_bytes().as_slice())
                .await
                .unwrap()
                .unwrap();
            if res != chksum_hash_md5::hash(t_id.to_be_bytes()).as_bytes() {
                wrong_value.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });
    }
    while (set.join_next().await).is_some() {
        // println!("done: {}", done);
    }
    let elapsed = now.elapsed();
    println!("get: {:?}", elapsed);
    println!("get: {:?}", elapsed / keys as u32);
    if wrong_value.load(std::sync::atomic::Ordering::SeqCst) > 0 {
        println!("wrong_values: {:?}", wrong_value);
    }
}

fn put(client: &Client, table: String, keys: u64, rt: &Runtime) {
    let local = tokio::task::LocalSet::new();
    local.block_on(rt, async {
        let mut set = futures::stream::FuturesUnordered::new();
        for t_id in 0u64..keys {
            let table = table.clone();
            let client = client.clone();
            let t = local.spawn_local(async move {
                let key = t_id.to_be_bytes();
                let v = chksum_hash_md5::hash(key);
                // let v = [0u8; 1024];
                client
                    .put(
                        &table.as_bytes(),
                        key.as_slice(),
                        v.as_ref(),
                    )
                    .await
                    .unwrap();
            });
            set.push(t);
        }
        while let Some(_) = set.next().await {}
    });
}

async fn iter(client: &Client, table: &str) {
    let now = std::time::Instant::now();
    let iter = client.iter(table.as_bytes(), 1000000).await.unwrap();
    for (k, v) in iter.iter() {
        assert_eq!(chksum_hash_md5::hash(k).as_bytes(), v);
    }
    // let mut count = 0;
    // while let Some((k, v)) = iter.iter().next() {
    // println!("iter: {:?}, {:?}", u64::from_be_bytes(k.to_vec().try_into().unwrap()), u64::from_be_bytes(v.to_vec().try_into().unwrap()));
    // assert_eq!(chksum_hash_md5::hash(k).as_bytes(), v);
    // println!("{:?}, {:?}", k, v);
    // count += 1;
    // }
    println!("iter: {:?}, {}", now.elapsed(), iter.len());
}
