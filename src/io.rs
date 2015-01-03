use buf::{Buf, MutBuf};
use error::MioResult;
use self::NonBlock::{Ready, WouldBlock};
use error::MioErrorKind as mek;
use os;

pub use os::IoDesc;

/// The result of a non-blocking operation.
#[derive(Show)]
pub enum NonBlock<T> {
    Ready(T),
    WouldBlock
}

impl<T> NonBlock<T> {
    pub fn would_block(&self) -> bool {
        match *self {
            WouldBlock => true,
            _ => false
        }
    }

    pub fn unwrap(self) -> T {
        match self {
            Ready(v) => v,
            _ => panic!("would have blocked, no result to take")
        }
    }
}

pub trait IoHandle {
    fn desc(&self) -> &IoDesc;
}

pub trait IoReader {
    fn read(&self, buf: &mut MutBuf) -> MioResult<NonBlock<uint>>;
    fn read_slice(&self, buf: &mut [u8]) -> MioResult<NonBlock<uint>>;
}

pub trait IoWriter {
    fn write(&self, buf: &mut Buf) -> MioResult<NonBlock<uint>>;
    fn write_slice(&self, buf: &[u8]) -> MioResult<NonBlock<uint>>;
}

pub trait IoAcceptor<T> {
    fn accept(&mut self) -> MioResult<NonBlock<T>>;
}

pub fn pipe() -> MioResult<(PipeReader, PipeWriter)> {
    let (rd, wr) = try!(os::pipe());
    Ok((PipeReader { desc: rd }, PipeWriter { desc: wr }))
}

pub struct PipeReader {
    desc: os::IoDesc
}

impl IoHandle for PipeReader {
    fn desc(&self) -> &os::IoDesc {
        &self.desc
    }
}

pub struct PipeWriter {
    desc: os::IoDesc
}

impl IoHandle for PipeWriter {
    fn desc(&self) -> &os::IoDesc {
        &self.desc
    }
}

impl IoReader for PipeReader {
    fn read(&self, buf: &mut MutBuf) -> MioResult<NonBlock<uint>> {
        read(self, buf)
    }

    fn read_slice(&self, buf: &mut [u8]) -> MioResult<NonBlock<uint>> {
        read_slice(self, buf)
    }
}

impl IoWriter for PipeWriter {
    fn write(&self, buf: &mut Buf) -> MioResult<NonBlock<uint>> {
        write(self, buf)
    }

    fn write_slice(&self, buf: &[u8]) -> MioResult<NonBlock<uint>> {
        write_slice(self, buf)
    }
}

/// Reads the length of the slice supplied by buf.mut_bytes into the buffer
/// This is not guaranteed to consume an entire datagram or segment.
/// If your protocol is msg based (instead of continuous stream) you should
/// ensure that your buffer is large enough to hold an entire segment (1532 bytes if not jumbo
/// frames)
#[inline]
pub fn read<I: IoHandle>(io: &I, buf: &mut MutBuf) -> MioResult<NonBlock<uint>> {

    let res = read_slice(io, buf.mut_bytes());
    match res {
        // Successfully read some bytes, advance the cursor
        Ok(Ready(cnt)) => { buf.advance(cnt); },
        _              => {}
    }
    res
}

///writes the length of the slice supplied by Buf.bytes into the socket
///then advances the buffer that many bytes
#[inline]
pub fn write<O: IoHandle>(io: &O, buf: &mut Buf) -> MioResult<NonBlock<uint>> {
    let res = write_slice(io, buf.bytes());
    match res {
        Ok(Ready(cnt)) => buf.advance(cnt),
        _              => {}
    }
    res
}

///reads the length of the supplied slice from the socket into the slice
#[inline]
pub fn read_slice<I: IoHandle>(io: & I, buf: &mut [u8]) -> MioResult<NonBlock<uint>> {
    match os::read(io.desc(), buf) {
        Ok(cnt) => {
            Ok(Ready(cnt))
        }
        Err(e) => {
            match e.kind {
                mek::WouldBlock => Ok(WouldBlock),
                _ => Err(e)
            }
        }
    }
}

///writes the length of the supplied slice into the socket from the slice
#[inline]
pub fn write_slice<I: IoHandle>(io: & I, buf: & [u8]) -> MioResult<NonBlock<uint>> {
    match os::write(io.desc(), buf) {
        Ok(cnt) => { Ok(Ready(cnt)) }
        Err(e) => {
            match e.kind {
                mek::WouldBlock => Ok(WouldBlock),
                _               => Err(e)
            }
        }
    }
}
