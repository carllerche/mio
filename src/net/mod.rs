//! Networking primitives.
//!
//! The types provided in this module are non-blocking by default and are
//! designed to be portable across all supported Mio platforms. As long as the
//! [portability guidelines] are followed, the behavior should be identical no
//! matter the target platform.
//!
//! [portability guidelines]: ../struct.Poll.html#portability

use std::{io, net};

use socket2::SockAddr;

mod tcp;
pub use self::tcp::{TcpKeepalive, TcpListener, TcpSocket, TcpStream};

mod udp;
pub use self::udp::UdpSocket;

#[cfg(unix)]
mod uds;
#[cfg(unix)]
pub use self::uds::{SocketAddr, UnixDatagram, UnixListener, UnixStream};

/// Convert a `socket2:::SockAddr` into a `std::net::SocketAddr`.
fn convert_address(address: SockAddr) -> io::Result<net::SocketAddr> {
    match address.as_socket() {
        Some(address) => Ok(address),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid address family (not IPv4 or IPv6)",
        )),
    }
}
