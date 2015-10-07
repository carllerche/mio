use {io, Evented, EventSet, Io, IpAddr, PollOpt, Selector, Token};
use io::MapNonBlock;
use sys::unix::{net, nix, Socket};
use std::net::SocketAddr;
use std::os::unix::io::{RawFd, AsRawFd, FromRawFd};

#[derive(Debug)]
pub struct UdpSocket {
    io: Io,
}

impl UdpSocket {
    /// Returns a new, unbound, non-blocking, IPv4 UDP socket
    pub fn v4() -> io::Result<UdpSocket> {
        net::socket(nix::AddressFamily::Inet, nix::SockType::Datagram, true)
            .map(|fd| UdpSocket { io: Io::from_raw_fd(fd) })
    }

    /// Returns a new, unbound, non-blocking, IPv6 UDP socket
    pub fn v6() -> io::Result<UdpSocket> {
        net::socket(nix::AddressFamily::Inet6, nix::SockType::Datagram, true)
            .map(|fd| UdpSocket { io: Io::from_raw_fd(fd) })
    }

    pub fn bind(&self, addr: &SocketAddr) -> io::Result<()> {
        net::bind(&self.io, &net::to_nix_addr(addr))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        net::getsockname(&self.io)
            .map(net::to_std_addr)
    }

    pub fn try_clone(&self) -> io::Result<UdpSocket> {
        net::dup(&self.io)
            .map(From::from)
    }

    pub fn send_to(&self, buf: &[u8], target: &SocketAddr)
                   -> io::Result<Option<usize>> {
        net::sendto(&self.io, buf, &net::to_nix_addr(target))
            .map_non_block()
    }

    pub fn recv_from(&self, buf: &mut [u8])
                     -> io::Result<Option<(usize, SocketAddr)>> {
        net::recvfrom(&self.io, buf)
            .map(|(cnt, addr)| (cnt, net::to_std_addr(addr)))
            .map_non_block()
    }

    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        nix::setsockopt(self.as_raw_fd(), nix::sockopt::Broadcast, &on)
            .map_err(super::from_nix_error)
    }

    pub fn set_multicast_loop(&self, on: bool) -> io::Result<()> {
        nix::setsockopt(self.as_raw_fd(), nix::sockopt::IpMulticastLoop, &on)
            .map_err(super::from_nix_error)
    }

    pub fn join_multicast(&self, multi: &IpAddr) -> io::Result<()> {
        match *multi {
            IpAddr::V4(ref addr) => {
                // Create the request
                let req = nix::ip_mreq::new(nix::Ipv4Addr::from_std(addr), None);

                // Set the socket option
                nix::setsockopt(self.as_raw_fd(), nix::sockopt::IpAddMembership, &req)
                    .map_err(super::from_nix_error)
            }
            IpAddr::V6(ref addr) => {
                // Create the request
                let req = nix::ipv6_mreq::new(nix::Ipv6Addr::from_std(addr));

                // Set the socket option
                nix::setsockopt(self.as_raw_fd(), nix::sockopt::Ipv6AddMembership, &req)
                    .map_err(super::from_nix_error)
            }
        }
    }

    pub fn leave_multicast(&self, multi: &IpAddr) -> io::Result<()> {
        match *multi {
            IpAddr::V4(ref addr) => {
                // Create the request
                let req = nix::ip_mreq::new(nix::Ipv4Addr::from_std(addr), None);

                // Set the socket option
                nix::setsockopt(self.as_raw_fd(), nix::sockopt::IpDropMembership, &req)
                    .map_err(super::from_nix_error)
            }
            IpAddr::V6(ref addr) => {
                // Create the request
                let req = nix::ipv6_mreq::new(nix::Ipv6Addr::from_std(addr));

                // Set the socket option
                nix::setsockopt(self.as_raw_fd(), nix::sockopt::Ipv6DropMembership, &req)
                    .map_err(super::from_nix_error)
            }
        }
    }

    pub fn set_multicast_time_to_live(&self, ttl: i32) -> io::Result<()> {
        let v = if ttl < 0 {
            0
        } else if ttl > 255 {
            255
        } else {
            ttl as u8
        };

        nix::setsockopt(self.as_raw_fd(), nix::sockopt::IpMulticastTtl, &v)
            .map_err(super::from_nix_error)
    }
}

impl Evented for UdpSocket {
    fn register(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        self.io.register(selector, token, interest, opts)
    }

    fn reregister(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        self.io.reregister(selector, token, interest, opts)
    }

    fn deregister(&self, selector: &mut Selector) -> io::Result<()> {
        self.io.deregister(selector)
    }
}

impl Socket for UdpSocket {
}

impl From<Io> for UdpSocket {
    fn from(io: Io) -> UdpSocket {
        UdpSocket { io: io }
    }
}

impl FromRawFd for UdpSocket {
    unsafe fn from_raw_fd(fd: RawFd) -> UdpSocket {
        UdpSocket { io: Io::from_raw_fd(fd) }
    }
}

impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.io.as_raw_fd()
    }
}
