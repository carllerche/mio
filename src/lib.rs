#![crate_name = "mio"]
#![feature(globs)]
#![feature(phase)]
#![feature(unsafe_destructor)]
// While in active dev
#![allow(dead_code)]

extern crate alloc;
extern crate nix;
extern crate time;

#[phase(plugin, link)]
extern crate log;

pub use buf::{
    Buf,
    MutBuf,
};
pub use error::{
    MioResult,
    MioError,
};
pub use handler::{
    Handler,
    ReadHint,
    DATAHINT,
    HUPHINT,
    ERRORHINT,
};
pub use io::{
    pipe,
    NonBlock,
    IoReader,
    IoWriter,
    IoAcceptor,
    PipeReader,
    PipeWriter,
};
pub use poll::{
    Poll,
    IoEvent,
    IoEventKind,
    IOREADABLE,
    IOWRITABLE,
    IOERROR,
    IOHUPHINT,
    IOHINTED,
};
pub use event_loop::{
    EventLoop,
    EventLoopConfig,
    EventLoopResult,
    EventLoopSender,
};
pub use timer::{
    Timeout,
};
pub use token::{
    Token,
};

pub mod buf;
pub mod net;
pub mod util;

mod error;
mod event_loop;
mod handler;
mod io;
mod notify;
mod os;
mod poll;
mod timer;
mod token;
