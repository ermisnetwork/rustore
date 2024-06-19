use rustore_protocol::{Command, Operation, PipedCommand};
fn main() {
    bench();
}

fn bench() {
    let client_command = Command {
        table: b"daibo",
        operation: Operation::WatchPrefix(b"key1"),
    };
    let piped_command = PipedCommand {
        id: 0,
        command: client_command,
    }
    .to_bytes()
    .unwrap();
    println!("{:?}", piped_command.as_ref());
}
