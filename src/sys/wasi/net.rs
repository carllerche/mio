use std::io;

/// A lot of function are not support on Wasi, this function returns a
/// consistent error when calling those functions.
fn unsupported() -> io::Error {
    io::Error::new(io::ErrorKind::Other, "not supported on wasi")
}

pub(crate) mod tcp {
    use std::io;
    use std::net::{self, SocketAddr};
    use std::time::Duration;

    use super::unsupported;

    pub type TcpSocket = wasi::Fd;

    pub(crate) fn new_v4_socket() -> io::Result<TcpSocket> {
        Err(unsupported())
    }

    pub(crate) fn new_v6_socket() -> io::Result<TcpSocket> {
        Err(unsupported())
    }

    pub(crate) fn bind(_: TcpSocket, _: SocketAddr) -> io::Result<()> {
        Err(unsupported())
    }

    pub(crate) fn connect(_: TcpSocket, _: SocketAddr) -> io::Result<net::TcpStream> {
        Err(unsupported())
    }

    pub(crate) fn listen(_: TcpSocket, _: u32) -> io::Result<net::TcpListener> {
        Err(unsupported())
    }

    pub(crate) fn close(socket: TcpSocket) {
        let _ = unsafe { wasi::fd_close(socket) };
    }

    pub(crate) fn set_reuseaddr(_: TcpSocket, _: bool) -> io::Result<()> {
        Err(unsupported())
    }

    pub(crate) fn get_reuseaddr(_: TcpSocket) -> io::Result<bool> {
        Err(unsupported())
    }

    pub(crate) fn get_localaddr(_: TcpSocket) -> io::Result<SocketAddr> {
        Err(unsupported())
    }

    pub(crate) fn set_linger(_: TcpSocket, _: Option<Duration>) -> io::Result<()> {
        Err(unsupported())
    }

    pub(crate) fn get_linger(_: TcpSocket) -> io::Result<Option<Duration>> {
        Err(unsupported())
    }

    pub(crate) fn set_recv_buffer_size(_: TcpSocket, _: u32) -> io::Result<()> {
        Err(unsupported())
    }

    pub(crate) fn get_recv_buffer_size(_: TcpSocket) -> io::Result<u32> {
        Err(unsupported())
    }

    pub(crate) fn set_send_buffer_size(_: TcpSocket, _: u32) -> io::Result<()> {
        Err(unsupported())
    }

    pub(crate) fn get_send_buffer_size(_: TcpSocket) -> io::Result<u32> {
        Err(unsupported())
    }

    pub(crate) fn accept(_: &net::TcpListener) -> io::Result<(net::TcpStream, SocketAddr)> {
        Err(unsupported())
    }
}

pub(crate) mod udp {
    use std::io;
    use std::net::{self, SocketAddr};

    use super::unsupported;

    pub(crate) fn bind(_: SocketAddr) -> io::Result<net::UdpSocket> {
        Err(unsupported())
    }
}
