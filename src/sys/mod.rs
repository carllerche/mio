//! Module with system specific types.
//!
//! Required types:
//!
//! * `Event`: a type alias for the system specific event, e.g. `kevent` or
//!            `epoll_event`.
//! * `event`: a module with various helper functions for `Event`, see
//!            [`crate::event::Event`] for the required functions.
//! * `Events`: collection of `Event`s, see [`crate::Events`].
//! * `IoSourceState`: state for the `IoSource` type.
//! * `Selector`: selector used to register event sources and poll for events,
//!               see [`crate::Poll`] and [`crate::Registry`] for required
//!               methods.
//! * `tcp` and `udp` modules: see the [`crate::net`] module.
//! * `Waker`: see [`crate::Waker`].

cfg_os_poll! {
    macro_rules! debug_detail {
        (
            $type: ident ($event_type: ty), $test: path,
            $($(#[$target: meta])* $libc: ident :: $flag: ident),+ $(,)*
        ) => {
            struct $type($event_type);

            impl fmt::Debug for $type {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    let mut written_one = false;
                    $(
                        $(#[$target])*
                        #[allow(clippy::bad_bit_mask)] // Apparently some flags are zero.
                        {
                            // Windows doesn't use `libc` but the `afd` module.
                            if $test(&self.0, &$libc :: $flag) {
                                if !written_one {
                                    write!(f, "{}", stringify!($flag))?;
                                    written_one = true;
                                } else {
                                    write!(f, "|{}", stringify!($flag))?;
                                }
                            }
                        }
                    )+
                    if !written_one {
                        write!(f, "(empty)")
                    } else {
                        Ok(())
                    }
                }
            }
        };
    }
}

#[cfg(unix)]
cfg_os_poll! {
    mod unix;
    pub use self::unix::SourceFd;

    pub(crate) use self::unix::{event, Event, Events, Selector, Waker};

    cfg_tcp! {
        pub(crate) use self::unix::tcp;
    }

    cfg_udp! {
        pub(crate) use self::unix::udp;
    }

    cfg_uds! {
        pub use self::unix::SocketAddr;

        pub(crate) use self::unix::uds;
    }

    cfg_net! {
        pub(crate) use self::unix::IoSourceState;
    }
}

#[cfg(windows)]
cfg_os_poll! {
    mod windows;
    pub(crate) use self::windows::{Waker, Event, Events, event, Selector};
    cfg_any_os_util! {
        pub use self::windows::{
            Binding,
            CompletionCallback,
            Overlapped,
            Readiness,
        };
    }

    cfg_net! {
        pub(crate) use self::windows::IoSourceState;
    }

    cfg_tcp! {
        pub(crate) use self::windows::tcp;
    }

    cfg_udp! {
        pub(crate) use self::windows::udp;
    }
}

cfg_not_os_poll! {
    mod shell;
    pub(crate) use self::shell::*;

    #[cfg(unix)]
    cfg_any_os_util! {
        mod unix;
        pub use self::unix::SourceFd;
    }

    #[cfg(unix)]
    cfg_uds! {
        pub use self::unix::SocketAddr;
    }
}
