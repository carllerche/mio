#[cfg(any(feature = "tcp", feature = "udp"))]
use crate::sys::unix::net::from_socket_addr;
#[cfg(feature = "tcp")]
use crate::sys::unix::net::to_socket_addr;
use std::io::Result;
#[cfg(any(feature = "tcp", feature = "udp"))]
use std::mem;
#[cfg(feature = "tcp")]
use std::mem::MaybeUninit;
#[cfg(any(feature = "tcp", feature = "udp"))]
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};

#[derive(Debug)]
pub(crate) struct Socket {
    fd: libc::c_int,
}

impl Socket {
    pub(crate) fn new(
        domain: libc::c_int,
        socket_type: libc::c_int,
        protocol: libc::c_int,
    ) -> Result<Self> {
        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "linux",
            target_os = "netbsd",
            target_os = "openbsd"
        ))]
        let socket_type = socket_type | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;

        // Gives a warning for platforms without SOCK_NONBLOCK.
        #[allow(clippy::let_and_return)]
        let socket = syscall!(socket(domain, socket_type, protocol));

        // Darwin doesn't have SOCK_NONBLOCK or SOCK_CLOEXEC. Not sure about
        // Solaris, couldn't find anything online.
        #[cfg(any(target_os = "ios", target_os = "macos", target_os = "solaris"))]
        let socket = socket.and_then(|socket| {
            // For platforms that don't support flags in socket, we need to
            // set the flags ourselves.
            syscall!(fcntl(socket, libc::F_SETFL, libc::O_NONBLOCK))
                .and_then(|_| {
                    syscall!(fcntl(socket, libc::F_SETFD, libc::FD_CLOEXEC)).map(|_| socket)
                })
                .map_err(|e| {
                    // If either of the `fcntl` calls failed, close the
                    // socket. Ignore the error from closing since we can't
                    // pass back two errors.
                    let _ = syscall!(close(socket));
                    e
                })
        });

        socket.map(|socket| Socket { fd: socket })
    }

    #[cfg(any(feature = "tcp", feature = "udp"))]
    pub(crate) fn from_addr(addr: SocketAddr, socket_type: libc::c_int) -> Result<Self> {
        let domain = match addr {
            SocketAddr::V4(..) => libc::AF_INET,
            SocketAddr::V6(..) => libc::AF_INET6,
        };
        Self::new(domain, socket_type, 0)
    }

    #[cfg(feature = "tcp")]
    pub(crate) fn connect(&self, addr: SocketAddr) -> Result<i32> {
        let (storage, len) = from_socket_addr(&addr);
        let res = syscall!(connect(self.fd, storage, len));
        match res {
            Ok(res) => Ok(res),
            Err(ref err) if err.raw_os_error() == Some(libc::EINPROGRESS) => {
                // Connect hasn't finished, but that is fine.
                Ok(0)
            }
            Err(err) => {
                // Close the socket if we hit an error, ignoring the error
                // from closing since we can't pass back two errors.
                let _ = unsafe { libc::close(self.fd) };
                Err(err)
            }
        }
    }

    #[cfg(any(feature = "tcp", feature = "udp"))]
    pub(crate) fn bind(&self, addr: SocketAddr) -> Result<i32> {
        let (storage, len) = from_socket_addr(&addr);
        syscall!(bind(self.fd, storage, len)).map_err(|err| {
            // Close the socket if we hit an error, ignoring the error
            // from closing since we can't pass back two errors.
            let _ = unsafe { libc::close(self.fd) };
            err
        })
    }

    #[cfg(feature = "tcp")]
    pub(crate) fn listen(&self, backlog: i32) -> Result<i32> {
        syscall!(listen(self.fd, backlog))
    }

    #[cfg(feature = "tcp")]
    pub(crate) fn accept(&self) -> Result<(Self, SocketAddr)> {
        let mut storage: MaybeUninit<libc::sockaddr_storage> = MaybeUninit::uninit();
        let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

        // On platforms that support it we can use `accept4(2)` to set `NONBLOCK`
        // and `CLOEXEC` in the call to accept the connection.
        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "linux",
            target_os = "openbsd"
        ))]
        let socket = syscall!(accept4(
            self.fd,
            storage.as_mut_ptr() as *mut _,
            &mut len,
            libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
        ))?;

        // But not all platforms have the `accept4(2)` call. Luckily BSD (derived)
        // OSes inherit the non-blocking flag from the listener, so we just have to
        // set `CLOEXEC`.
        #[cfg(any(
            target_os = "ios",
            target_os = "macos",
            // NetBSD 8.0 actually has `accept4(2)`, but libc doesn't expose it
            // (yet). See https://github.com/rust-lang/libc/issues/1636.
            target_os = "netbsd",
            target_os = "solaris",
        ))]
        let socket = {
            let socket = syscall!(accept(self.fd, storage.as_mut_ptr() as *mut _, &mut len))?;
            syscall!(fcntl(socket, libc::F_SETFD, libc::FD_CLOEXEC))?;
            socket
        };

        // This is safe because `accept` calls above ensures the address
        // initialised.
        let socket_addr = unsafe { to_socket_addr(storage.as_ptr())? };

        Ok((Socket { fd: socket }, socket_addr))
    }

    #[cfg(feature = "tcp")]
    pub(crate) fn set_reuse_address(&self) -> Result<i32> {
        syscall!(setsockopt(
            self.fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &1 as *const libc::c_int as *const libc::c_void,
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        ))
    }

    #[cfg(feature = "udp")]
    pub(crate) fn set_no_sigpipe(&self) -> Result<i32> {
        syscall!(setsockopt(
            self.fd,
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            &1 as *const libc::c_int as *const libc::c_void,
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        ))
    }
}

impl AsRawFd for Socket {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl FromRawFd for Socket {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Socket { fd }
    }
}

impl IntoRawFd for Socket {
    fn into_raw_fd(self) -> RawFd {
        self.fd
    }
}
