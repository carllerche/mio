use super::{socket_addr, SocketAddr};
use crate::sys::Socket;

use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::net;
use std::path::Path;

pub(crate) fn connect(path: &Path) -> io::Result<net::UnixStream> {
    let socket = Socket::new(libc::AF_UNIX, libc::SOCK_STREAM, 0)?;
    let (sockaddr, socklen) = socket_addr(path)?;
    let sockaddr = &sockaddr as *const libc::sockaddr_un as *const libc::sockaddr;

    // temp: Most of the below can be moved into `Socket` methods. Create a
    // `RawFd` for now until those are added.
    let socket = socket.as_raw_fd();

    match syscall!(connect(socket, sockaddr, socklen)) {
        Ok(_) => {}
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
        Err(e) => {
            // Close the socket if we hit an error, ignoring the error
            // from closing since we can't pass back two errors.
            let _ = unsafe { libc::close(socket) };

            return Err(e);
        }
    }

    Ok(unsafe { net::UnixStream::from_raw_fd(socket) })
}

pub(crate) fn pair() -> io::Result<(net::UnixStream, net::UnixStream)> {
    super::pair(libc::SOCK_STREAM)
}

pub(crate) fn local_addr(socket: &net::UnixStream) -> io::Result<SocketAddr> {
    super::local_addr(socket.as_raw_fd())
}

pub(crate) fn peer_addr(socket: &net::UnixStream) -> io::Result<SocketAddr> {
    super::peer_addr(socket.as_raw_fd())
}
