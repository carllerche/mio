use {io};
use sys::unix::{nix, Io};
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
pub use net::tcp::Shutdown;

pub fn socket(family: nix::AddressFamily, ty: nix::SockType, nonblock: bool) -> io::Result<RawFd> {
    let opts = if nonblock {
        nix::SOCK_NONBLOCK | nix::SOCK_CLOEXEC
    } else {
        nix::SOCK_CLOEXEC
    };

    nix::socket(family, ty, opts)
        .map_err(super::from_nix_error)
}

pub fn connect(io: &Io, addr: &nix::SockAddr) -> io::Result<bool> {
    match nix::connect(io.as_raw_fd(), addr) {
        Ok(_) => Ok(true),
        Err(e) => {
            match e {
                nix::Error::Sys(nix::EINPROGRESS) => Ok(false),
                _ => Err(super::from_nix_error(e))
            }
        }
    }
}

pub fn bind(io: &Io, addr: &nix::SockAddr) -> io::Result<()> {
    nix::bind(io.as_raw_fd(), addr)
        .map_err(super::from_nix_error)
}

pub fn listen(io: &Io, backlog: usize) -> io::Result<()> {
    nix::listen(io.as_raw_fd(), backlog)
        .map_err(super::from_nix_error)
}

pub fn accept(io: &Io, nonblock: bool) -> io::Result<RawFd> {
    let opts = if nonblock {
        nix::SOCK_NONBLOCK | nix::SOCK_CLOEXEC
    } else {
        nix::SOCK_CLOEXEC
    };

    nix::accept4(io.as_raw_fd(), opts)
        .map_err(super::from_nix_error)
}

pub fn shutdown(io: &Io, how: Shutdown) -> io::Result<()> {
    let how: nix::Shutdown = match how {
        Shutdown::Read  => nix::Shutdown::Read,
        Shutdown::Write => nix::Shutdown::Write,
        Shutdown::Both  => nix::Shutdown::Both,
    };
    nix::shutdown(io.as_raw_fd(), how)
        .map_err(super::from_nix_error)
}

pub fn take_socket_error(io: &Io) -> io::Result<()> {
    let code = try!(nix::getsockopt(io.as_raw_fd(), nix::sockopt::SocketError)
                            .map_err(super::from_nix_error));
    if code != 0 {
        Err(io::Error::from_raw_os_error(code))
    } else {
        Ok(())
    }
}

pub fn set_nodelay(io: &Io, delay: bool) -> io::Result<()> {
    nix::setsockopt(io.as_raw_fd(), nix::sockopt::TcpNoDelay, &delay)
        .map_err(super::from_nix_error)
}

pub fn set_keepalive(io: &Io, keepalive: bool) -> io::Result<()> {
    nix::setsockopt(io.as_raw_fd(), nix::sockopt::KeepAlive, &keepalive)
        .map_err(super::from_nix_error)
}

#[cfg(any(target_os = "macos",
          target_os = "ios"))]
pub fn set_tcp_keepalive(io: &Io, seconds: u32) -> io::Result<()> {
    nix::setsockopt(io.as_raw_fd(), nix::sockopt::TcpKeepAlive, &seconds)
        .map_err(super::from_nix_error)
}

#[cfg(any(target_os = "freebsd",
          target_os = "dragonfly",
          target_os = "linux"))]
pub fn set_tcp_keepalive(io: &Io, seconds: u32) -> io::Result<()> {
    nix::setsockopt(io.as_raw_fd(), nix::sockopt::TcpKeepIdle, &seconds)
        .map_err(super::from_nix_error)
}

#[cfg(not(any(target_os = "freebsd",
              target_os = "dragonfly",
              target_os = "linux",
              target_os = "macos",
              target_os = "ios")))]
pub fn set_tcp_keepalive(io: &Io, _seconds: u32) -> io::Result<()> {
    Ok(())
}

// UDP & UDS
#[inline]
pub fn recvfrom(io: &Io, buf: &mut [u8]) -> io::Result<(usize, nix::SockAddr)> {
    nix::recvfrom(io.as_raw_fd(), buf)
        .map_err(super::from_nix_error)
}

// UDP & UDS
#[inline]
pub fn sendto(io: &Io, buf: &[u8], target: &nix::SockAddr) -> io::Result<usize> {
    nix::sendto(io.as_raw_fd(), buf, target, nix::MSG_DONTWAIT)
        .map_err(super::from_nix_error)
}

pub fn getpeername(io: &Io) -> io::Result<nix::SockAddr> {
    nix::getpeername(io.as_raw_fd())
        .map_err(super::from_nix_error)
}

pub fn getsockname(io: &Io) -> io::Result<nix::SockAddr> {
    nix::getsockname(io.as_raw_fd())
        .map_err(super::from_nix_error)
}

#[inline]
pub fn dup(io: &Io) -> io::Result<Io> {
    nix::dup(io.as_raw_fd())
        .map_err(super::from_nix_error)
        .map(|fd| Io::from_raw_fd(fd))
}

/*
 *
 * ===== Helpers =====
 *
 */

pub fn to_nix_addr(addr: &SocketAddr) -> nix::SockAddr {
    nix::SockAddr::Inet(nix::InetAddr::from_std(addr))
}

pub fn to_std_addr(addr: nix::SockAddr) -> SocketAddr {
    match addr {
        nix::SockAddr::Inet(ref addr) => addr.to_std(),
        _ => panic!("unexpected unix socket address"),
    }
}
