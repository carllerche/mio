use {io, Evented, Ready, Poll, PollOpt, Token};
use sys::redox::EventedFd;
use std::net::{self, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::unix::io::{RawFd, IntoRawFd, AsRawFd, FromRawFd};

#[derive(Debug)]
pub struct UdpSocket {
    io: net::UdpSocket,
}

impl UdpSocket {
    pub fn new(socket: net::UdpSocket) -> io::Result<UdpSocket> {
        socket.set_nonblocking(true)?;
        Ok(UdpSocket {
            io: socket,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    pub fn try_clone(&self) -> io::Result<UdpSocket> {
        self.io.try_clone().map(|io| {
            UdpSocket {
                io: io,
            }
        })
    }

    pub fn send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<usize> {
        self.io.send_to(buf, target)
    }

    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.io.recv_from(buf)
    }

    pub fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.io.send(buf)
    }

    pub fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.io.recv(buf)
    }

    pub fn connect(&self, addr: SocketAddr)
                     -> io::Result<()> {
        self.io.connect(addr)
    }

    pub fn broadcast(&self) -> io::Result<bool> {
        self.io.broadcast()
    }

    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        self.io.set_broadcast(on)
    }

    pub fn multicast_loop_v4(&self) -> io::Result<bool> {
        self.io.multicast_loop_v4()
    }

    pub fn set_multicast_loop_v4(&self, on: bool) -> io::Result<()> {
        self.io.set_multicast_loop_v4(on)
    }

    pub fn multicast_ttl_v4(&self) -> io::Result<u32> {
        self.io.multicast_ttl_v4()
    }

    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.io.set_multicast_ttl_v4(ttl)
    }

    pub fn multicast_loop_v6(&self) -> io::Result<bool> {
        self.io.multicast_loop_v6()
    }

    pub fn set_multicast_loop_v6(&self, on: bool) -> io::Result<()> {
        self.io.set_multicast_loop_v6(on)
    }

    pub fn ttl(&self) -> io::Result<u32> {
        self.io.ttl()
    }

    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.set_ttl(ttl)
    }

    pub fn join_multicast_v4(&self,
                             multiaddr: &Ipv4Addr,
                             interface: &Ipv4Addr) -> io::Result<()> {
        self.io.join_multicast_v4(multiaddr, interface)
    }

    pub fn join_multicast_v6(&self,
                             multiaddr: &Ipv6Addr,
                             interface: u32) -> io::Result<()> {
        self.io.join_multicast_v6(multiaddr, interface)
    }

    pub fn leave_multicast_v4(&self,
                              multiaddr: &Ipv4Addr,
                              interface: &Ipv4Addr) -> io::Result<()> {
        self.io.leave_multicast_v4(multiaddr, interface)
    }

    pub fn leave_multicast_v6(&self,
                              multiaddr: &Ipv6Addr,
                              interface: u32) -> io::Result<()> {
        self.io.leave_multicast_v6(multiaddr, interface)
    }

    pub fn set_only_v6(&self, _only_v6: bool) -> io::Result<()> {
        //self.io.set_only_v6(only_v6)
        Err(io::Error::new(io::ErrorKind::Other, "UdpSocket::set_only_v6 is unimplemented on redox"))
    }

    pub fn only_v6(&self) -> io::Result<bool> {
        //self.io.only_v6()
        Err(io::Error::new(io::ErrorKind::Other, "UdpSocket::only_v6 is unimplemented on redox"))
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.io.take_error()
    }
}

impl Evented for UdpSocket {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        EventedFd(&self.as_raw_fd()).register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        EventedFd(&self.as_raw_fd()).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        EventedFd(&self.as_raw_fd()).deregister(poll)
    }
}

impl FromRawFd for UdpSocket {
    unsafe fn from_raw_fd(fd: RawFd) -> UdpSocket {
        UdpSocket {
            io: net::UdpSocket::from_raw_fd(fd),
        }
    }
}

impl IntoRawFd for UdpSocket {
    fn into_raw_fd(self) -> RawFd {
        self.io.into_raw_fd()
    }
}

impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.io.as_raw_fd()
    }
}
