macro_rules! os_required {
    () => {
        panic!("mio must be compiled with `os-poll` to run.")
    };
}

mod selector;
pub(crate) use self::selector::{event, Event, Events, Selector};

mod waker;
pub(crate) use self::waker::Waker;

cfg_tcp! {
    pub mod tcp;
    pub(crate) use self::tcp::TcpListener;
}

cfg_udp! {
    pub mod udp;
}

#[cfg(unix)]
cfg_uds! {
    mod uds;
    pub(crate) use self::uds::{UnixDatagram, UnixListener, UnixStream};
}

cfg_net! {
    use std::io;
    #[cfg(windows)]
    use std::os::windows::io::RawSocket;

    #[cfg(windows)]
    use crate::{Registry, Token, Interest};

    pub struct IoSourceState;

    impl IoSourceState {
        pub fn new() -> IoSourceState {
            IoSourceState
        }

        pub fn do_io<T, F, R>(&self, f: F, io: &T) -> io::Result<R>
        where
            F: FnOnce(&T) -> io::Result<R>,
        {
            // We don't hold state, so we can just call the function and
            // return.
            f(io)
        }
    }

    #[cfg(windows)]
    impl IoSourceState {
         pub fn register(
            &mut self,
            _: &Registry,
            _: Token,
            _: Interest,
            _: RawSocket,
        ) -> io::Result<()> {
            os_required!()
        }

        pub fn reregister(
            &mut self,
            _: &Registry,
            _: Token,
            _: Interest,
        ) -> io::Result<()> {
           os_required!()
        }

        pub fn deregister(&mut self) -> io::Result<()> {
            os_required!()
        }
    }
}
