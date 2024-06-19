use std::ops::{Deref, DerefMut};

use bytes::Bytes;
use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct ReconnectingFramed {
    framed: Framed<TcpStream, LengthDelimitedCodec>,
    addr: String,
    handshake: Bytes,
}

impl ReconnectingFramed {
    pub async fn connect(addr: String, handshake: Bytes) -> std::io::Result<Self> {
        let stream = loop {
            if let Ok(stream) = TcpStream::connect(&addr).await {
                break stream;
            } else {
                println!("Failed to connect, retrying in 1 second");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        };
        let mut framed = Framed::new(
            stream,
            tokio_util::codec::length_delimited::LengthDelimitedCodec::new(),
        );
        framed.send(handshake.clone()).await?;
        Ok(Self {
            framed,
            addr,
            handshake,
        })
    }
    pub async fn reconnect(&mut self) -> std::io::Result<()> {
        loop {
            if let Ok(stream) = TcpStream::connect(&self.addr).await {
                self.framed = Framed::new(
                    stream,
                    tokio_util::codec::length_delimited::LengthDelimitedCodec::new(),
                );
                self.framed.send(self.handshake.clone()).await?;
                break;
            } else {
                println!("Failed to reconnect, retrying in 1 second");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
        Ok(())
    }
}

impl Deref for ReconnectingFramed {
    type Target = Framed<TcpStream, LengthDelimitedCodec>;

    fn deref(&self) -> &Self::Target {
        &self.framed
    }
}

impl DerefMut for ReconnectingFramed {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.framed
    }
}
