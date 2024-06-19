## Key-value store service based on the newest sled alpha (with some modifications)
## Opinionated API due to initial usecase

### check service.yaml for how to configure, then run server with:
```
cargo run -r --bin datastore-server
```
### client example:
```
use datastore_client::piped_client::Client;

let client = Client::new("127.0.0.1:5555", "secret", false, false).await.unwrap();
client.put("test".as_bytes(), "dai".as_bytes(), "bo".as_bytes()).await.unwrap();
assert_eq!(client.get("test".as_bytes(), "dai".as_bytes()).await.unwrap(), Some("bo".into()));
```