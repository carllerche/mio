use std::io;
use std::net::{self, SocketAddr};

pub fn bind(_: SocketAddr) -> io::Result<net::UdpSocket> {
    os_required!()
}

pub(crate) fn only_v6(_: &net::UdpSocket) -> io::Result<bool> {
    os_required!()
}

pub(crate) fn set_recv_buffer_size(_: &net::UdpSocket, _: u32) -> io::Result<()> {
    os_required!()
}

pub(crate) fn recv_buffer_size(_: &net::UdpSocket) -> io::Result<u32> {
    os_required!()
}

pub(crate) fn set_send_buffer_size(_: &net::UdpSocket, _: u32) -> io::Result<()> {
    os_required!()
}

pub(crate) fn send_buffer_size(_: &net::UdpSocket) -> io::Result<u32> {
    os_required!()
}
