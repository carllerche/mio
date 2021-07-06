use crate::sys::unix::net::{new_ip_socket, socket_addr};

use std::convert::TryInto;
use std::io;
use std::mem::{self, MaybeUninit};
use std::net::{self, SocketAddr};
use std::os::unix::io::{AsRawFd, FromRawFd};

pub fn bind(addr: SocketAddr) -> io::Result<net::UdpSocket> {
    // Gives a warning for non Apple platforms.
    #[allow(clippy::let_and_return)]
    let socket = new_ip_socket(addr, libc::SOCK_DGRAM);

    socket.and_then(|socket| {
        let (raw_addr, raw_addr_length) = socket_addr(&addr);
        syscall!(bind(socket, raw_addr.as_ptr(), raw_addr_length))
            .map_err(|err| {
                // Close the socket if we hit an error, ignoring the error
                // from closing since we can't pass back two errors.
                let _ = unsafe { libc::close(socket) };
                err
            })
            .map(|_| unsafe { net::UdpSocket::from_raw_fd(socket) })
    })
}

pub(crate) fn only_v6(socket: &net::UdpSocket) -> io::Result<bool> {
    let mut optval: libc::c_int = 0;
    let mut optlen = mem::size_of::<libc::c_int>() as libc::socklen_t;

    syscall!(getsockopt(
        socket.as_raw_fd(),
        libc::IPPROTO_IPV6,
        libc::IPV6_V6ONLY,
        &mut optval as *mut _ as *mut _,
        &mut optlen,
    ))?;

    Ok(optval != 0)
}

pub(crate) fn set_recv_buffer_size(socket: &net::UdpSocket, size: u32) -> io::Result<()> {
    let size = size.try_into().ok().unwrap_or_else(i32::max_value);

    syscall!(setsockopt(
        socket.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_RCVBUF,
        &size as *const _ as *const _,
        mem::size_of::<libc::c_int>() as libc::socklen_t
    ))?;

    Ok(())
}

pub(crate) fn recv_buffer_size(socket: &net::UdpSocket) -> io::Result<u32> {
    let mut optval: MaybeUninit<libc::c_int> = MaybeUninit::uninit();
    let mut optlen = mem::size_of::<libc::c_int>() as libc::socklen_t;

    syscall!(getsockopt(
        socket.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_RCVBUF,
        &mut optval as *mut _ as *mut _,
        &mut optlen,
    ))?;

    debug_assert_eq!(optlen as usize, mem::size_of::<libc::c_int>());
    // Safety: `getsockopt` initialised `optval` for us.
    let optval = unsafe { optval.assume_init() };
    Ok(optval as u32)
}

pub(crate) fn set_send_buffer_size(socket: &net::UdpSocket, size: u32) -> io::Result<()> {
    let size = size.try_into().ok().unwrap_or_else(i32::max_value);

    syscall!(setsockopt(
        socket.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_SNDBUF,
        &size as *const _ as *const _,
        mem::size_of::<libc::c_int>() as libc::socklen_t
    ))?;

    Ok(())
}

pub(crate) fn send_buffer_size(socket: &net::UdpSocket) -> io::Result<u32> {
    let mut optval: MaybeUninit<libc::c_int> = MaybeUninit::uninit();
    let mut optlen = mem::size_of::<libc::c_int>() as libc::socklen_t;

    syscall!(getsockopt(
        socket.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_SNDBUF,
        &mut optval as *mut _ as *mut _,
        &mut optlen,
    ))?;

    debug_assert_eq!(optlen as usize, mem::size_of::<libc::c_int>());
    // Safety: `getsockopt` initialised `optval` for us.
    let optval = unsafe { optval.assume_init() };
    Ok(optval as u32)
}
