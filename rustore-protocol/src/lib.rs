use anyhow::Result;
use bytes::{Bytes, BytesMut};
use speedy::{BigEndian, Readable, Writable};
pub mod owned;

#[derive(PartialEq, Clone, Debug, Readable, Writable)]

pub struct HandShake<'a> {
    pub kind: HandShakeKind,
    pub secret: &'a str,
}

#[derive(PartialEq, Clone, Debug, Readable, Writable)]
#[speedy(tag_type = u8)]
#[repr(u8)]
pub enum HandShakeKind {
    Standard(bool) = 0,    // bool is for whether the client is a subscriber
    Piped(bool, bool) = 1, // bools are for whether the client is a subscriber/prefix watcher
    Subscriber = 2,
    // PrefixWatcher
}

impl<'a> HandShake<'a> {
    pub fn new_piped(secret: &'a str, subscribe: bool, watch_prefix: bool) -> Self {
        HandShake {
            kind: HandShakeKind::Piped(subscribe, watch_prefix),
            secret,
        }
    }
    pub fn new_standard(secret: &'a str, subscribe: bool) -> Self {
        HandShake {
            kind: HandShakeKind::Standard(subscribe),
            secret,
        }
    }
    pub fn new_subscriber(secret: &'a str) -> Self {
        HandShake {
            kind: HandShakeKind::Subscriber,
            secret,
        }
    }
    #[inline(always)]
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self> {
        let kind = Self::read_from_buffer_with_ctx(BigEndian::default(), bytes)?;
        Ok(kind)
    }
    #[inline(always)]
    pub fn to_bytes(&self) -> Result<Bytes> {
        let bytes = self.write_to_vec_with_ctx(BigEndian::default())?;
        Ok(bytes.into())
    }
}

#[derive(PartialEq, Clone, Debug, Readable, Writable)]
#[speedy(tag_type = u8)]
#[repr(u8)]
pub enum Operation<'a> {
    Get(&'a [u8]) = 0,
    Put(&'a [u8], &'a [u8]) = 1,
    PutIfAbsent(&'a [u8], &'a [u8]) = 2,
    Remove(&'a [u8]) = 3,
    Update(&'a [u8], &'a [u8]) = 4,
    Iter(u32) = 5,
    IterPrefix(&'a [u8], u32) = 6,
    IterPrefixFrom(&'a [u8], &'a [u8], u32) = 7,
    IterPrefixFromRev(&'a [u8], &'a [u8], u32) = 8,
    IterFrom(&'a [u8], u32) = 9,
    Increment(&'a [u8]) = 10,
    IterCount = 11,
    IterPrefixCount(&'a [u8]) = 12,
    IterFromCount(&'a [u8]) = 13,
    IterFromRev(&'a [u8], u32) = 14,
    IterFromRevCount(&'a [u8]) = 15,
    BatchedPut(Vec<(&'a [u8], &'a [u8])>) = 16,
    BatchedGet(Vec<&'a [u8]>) = 17,
    BatchedIncrement(Vec<&'a [u8]>) = 18,
    BatchedRemove(Vec<&'a [u8]>) = 19,
    WatchPrefix(&'a [u8]) = 20,
    PutAndWake(&'a [u8], &'a [u8], &'a [u8]) = 21,
    BatchedPutAndWake(Vec<(&'a [u8], &'a [u8])>, Vec<&'a [u8]>) = 22,
    GetDelayed(u32, &'a [u8]) = 23,
    DropTable = 24,
    Decrement(&'a [u8]) = 25,
    Range(&'a [u8], &'a [u8], u32) = 26,
    RangeCount(&'a [u8], &'a [u8]) = 27,
}

impl<'a> Operation<'a> {
    #[inline(always)]
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self> {
        let operation = Self::read_from_buffer_with_ctx(BigEndian::default(), bytes)?;
        Ok(operation)
    }
    #[inline(always)]
    pub fn to_bytes(&self) -> Result<Bytes> {
        let bytes = self.write_to_vec_with_ctx(BigEndian::default())?;
        Ok(bytes.into())
    }
}

#[derive(Debug, PartialEq, Readable, Writable)]
pub struct Command<'a> {
    pub table: &'a [u8],
    pub operation: Operation<'a>,
}
impl<'a> Command<'a> {
    #[inline(always)]
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self> {
        let command = Self::read_from_buffer_with_ctx(BigEndian::default(), bytes)?;
        Ok(command)
    }
    #[inline(always)]
    pub fn to_bytes(&self) -> Result<Bytes> {
        let bytes = self.write_to_vec_with_ctx(BigEndian::default())?;
        Ok(bytes.into())
    }
}

#[derive(Debug, PartialEq, Readable, Writable)]
pub struct PipedCommand<'a> {
    pub id: u32,
    pub command: Command<'a>,
}

impl<'a> PipedCommand<'a> {
    #[inline(always)]
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self> {
        let id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let command = Command::read_from_buffer_with_ctx(BigEndian::default(), &bytes[4..])?;
        Ok(PipedCommand { id, command })
    }
    #[inline(always)]
    pub fn to_bytes(&self) -> Result<Bytes> {
        let mut bytes_mut = BytesMut::zeroed(
            4 + <Command as Writable<BigEndian>>::bytes_needed(&self.command).unwrap(),
        );
        bytes_mut[0..4].copy_from_slice(&self.id.to_be_bytes());
        self.command
            .write_to_buffer_with_ctx(BigEndian::default(), &mut bytes_mut[4..])?;
        Ok(bytes_mut.freeze())
    }
}

#[derive(PartialEq, Clone, Debug, Readable, Writable)]
#[speedy(tag_type = u8)]
#[repr(u8)]
pub enum Response<'a> {
    Ok = 0,
    PutError = 1,
    NoMatchingKey = 2,
    KeyExists = 3,
    Value(&'a [u8]) = 4,
    Entry(&'a [u8], &'a [u8]) = 5,
    Entries(Vec<(&'a [u8], &'a [u8])>) = 6,
    IterEnd = 7,
    InvalidCommand = 8,
    BatchFailed = 9,
    WakePrefix(&'a [u8], &'a [u8]) = 10,
    WatchPrefixUnsupported = 11,
}
impl<'a> Response<'a> {
    #[inline(always)]
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self> {
        let response = Self::read_from_buffer_with_ctx(BigEndian::default(), bytes)?;
        Ok(response)
    }
    #[inline(always)]
    pub fn to_bytes(&self) -> Bytes {
        let bytes = self.write_to_vec_with_ctx(BigEndian::default()).unwrap();
        bytes.into()
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct PipedResponse<'a> {
    pub id: u32,
    pub response: Response<'a>,
}

impl<'a> PipedResponse<'a> {
    #[inline(always)]
    pub fn to_bytes(self) -> Bytes {
        let needed = <Response as Writable<BigEndian>>::bytes_needed(&self.response).unwrap();
        let mut bytes_mut = BytesMut::zeroed(4 + needed);
        let PipedResponse { id, response } = self;
        bytes_mut[0..4].copy_from_slice(&id.to_be_bytes());
        response
            .write_to_buffer_with_ctx(BigEndian::default(), &mut bytes_mut[4..])
            .unwrap();
        bytes_mut.freeze()
    }
}
