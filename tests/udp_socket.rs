#![cfg(all(feature = "os-poll", feature = "net"))]

use std::mem::MaybeUninit;
use std::net::{self, IpAddr, SocketAddr};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
use std::sync::{Arc, Barrier};
use std::time::Duration;
use std::{slice, str, thread};

use log::{debug, info};

use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Registry, Token};

#[macro_use]
mod util;
use util::{
    any_local_address, any_local_ipv6_address, assert_error, assert_send,
    assert_socket_close_on_exec, assert_socket_non_blocking, assert_sync, assert_would_block,
    expect_events, expect_no_events, init, init_with_poll, ExpectEvent,
};

const DATA1: &[u8] = b"Hello world!";
const DATA2: &[u8] = b"Hello mars!";

const LISTENER: Token = Token(0);
const SENDER: Token = Token(1);
const ID1: Token = Token(2);
const ID2: Token = Token(3);
const ID3: Token = Token(4);

#[test]
#[cfg(all(unix, not(debug_assertions)))]
fn assert_size() {
    use mio::net::*;
    use std::mem::size_of;

    // Without debug assertions enabled `TcpListener`, `TcpStream` and
    // `UdpSocket` should have the same size as the system specific socket, i.e.
    // just a file descriptor on Unix platforms.
    assert_eq!(size_of::<UdpSocket>(), size_of::<std::net::UdpSocket>());
}

#[test]
fn send_empty_datagram() {
    const EMPTY: &[u8] = b"";

    let (mut poll, mut events) = init_with_poll();
    let mut s1 = UdpSocket::bind(any_local_address()).unwrap();
    let mut s2 = UdpSocket::bind(any_local_address()).unwrap();

    poll.registry()
        .register(&mut s1, ID1, Interest::WRITABLE)
        .unwrap();
    poll.registry()
        .register(&mut s2, ID2, Interest::READABLE)
        .unwrap();

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::WRITABLE)],
    );

    checked_write!(s1.send_to(EMPTY, s2.local_addr().unwrap()));

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID2, Interest::READABLE)],
    );
    let mut buf = [MaybeUninit::uninit(); 10];
    expect_read!(s2.recv_from(&mut buf), EMPTY, s1.local_addr().unwrap());
}

#[test]
fn is_send_and_sync() {
    assert_send::<UdpSocket>();
    assert_sync::<UdpSocket>();
}

#[test]
fn unconnected_udp_socket_ipv4() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();
    let socket2 = UdpSocket::bind(any_local_address()).unwrap();
    smoke_test_unconnected_udp_socket(socket1, socket2);
}

#[test]
fn unconnected_udp_socket_ipv6() {
    let socket1 = UdpSocket::bind(any_local_ipv6_address()).unwrap();
    let socket2 = UdpSocket::bind(any_local_ipv6_address()).unwrap();
    smoke_test_unconnected_udp_socket(socket1, socket2);
}

#[test]
fn unconnected_udp_socket_std() {
    let socket1 = net::UdpSocket::bind(any_local_address()).unwrap();
    let socket2 = net::UdpSocket::bind(any_local_address()).unwrap();

    // `std::net::UdpSocket`s are blocking by default, so make sure they are
    // in non-blocking mode before wrapping in a Mio equivalent.
    socket1.set_nonblocking(true).unwrap();
    socket2.set_nonblocking(true).unwrap();

    let socket1 = UdpSocket::from_std(socket1);
    let socket2 = UdpSocket::from_std(socket2);
    smoke_test_unconnected_udp_socket(socket1, socket2);
}

fn smoke_test_unconnected_udp_socket(mut socket1: UdpSocket, mut socket2: UdpSocket) {
    let (mut poll, mut events) = init_with_poll();

    assert_socket_non_blocking(&socket1);
    assert_socket_close_on_exec(&socket1);
    assert_socket_non_blocking(&socket2);
    assert_socket_close_on_exec(&socket2);

    let address1 = socket1.local_addr().unwrap();
    let address2 = socket2.local_addr().unwrap();

    poll.registry()
        .register(
            &mut socket1,
            ID1,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .expect("unable to register UDP socket");
    poll.registry()
        .register(
            &mut socket2,
            ID2,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .expect("unable to register UDP socket");

    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(ID1, Interest::WRITABLE),
            ExpectEvent::new(ID2, Interest::WRITABLE),
        ],
    );

    let mut buf = [MaybeUninit::uninit(); 20];
    assert_would_block(socket1.peek_from(&mut buf));
    assert_would_block(socket1.recv_from(&mut buf));

    checked_write!(socket1.send_to(DATA1, address2));
    checked_write!(socket2.send_to(DATA2, address1));

    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(ID1, Interest::READABLE),
            ExpectEvent::new(ID2, Interest::READABLE),
        ],
    );

    expect_read!(socket1.peek_from(&mut buf), DATA2, address2);
    expect_read!(socket2.peek_from(&mut buf), DATA1, address1);

    expect_read!(socket1.recv_from(&mut buf), DATA2, address2);
    expect_read!(socket2.recv_from(&mut buf), DATA1, address1);

    assert!(socket1.take_error().unwrap().is_none());
    assert!(socket2.take_error().unwrap().is_none());
}

#[test]
fn set_get_ttl() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();

    // set TTL, get TTL, make sure it has the expected value
    const TTL: u32 = 10;
    socket1.set_ttl(TTL).unwrap();
    assert_eq!(socket1.ttl().unwrap(), TTL);
    assert!(socket1.take_error().unwrap().is_none());
}

#[test]
fn get_ttl_without_previous_set() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();

    // expect a get TTL to work w/o any previous set_ttl
    socket1.ttl().expect("unable to get TTL for UDP socket");
}

#[test]
fn set_get_broadcast() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();

    socket1.set_broadcast(true).unwrap();
    assert_eq!(socket1.broadcast().unwrap(), true);

    socket1.set_broadcast(false).unwrap();
    assert_eq!(socket1.broadcast().unwrap(), false);

    assert!(socket1.take_error().unwrap().is_none());
}

#[test]
fn get_broadcast_without_previous_set() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();

    socket1
        .broadcast()
        .expect("unable to get broadcast for UDP socket");
}

#[test]
fn set_get_multicast_loop_v4() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();

    socket1.set_multicast_loop_v4(true).unwrap();
    assert_eq!(socket1.multicast_loop_v4().unwrap(), true);

    socket1.set_multicast_loop_v4(false).unwrap();
    assert_eq!(socket1.multicast_loop_v4().unwrap(), false);

    assert!(socket1.take_error().unwrap().is_none());
}

#[test]
fn get_multicast_loop_v4_without_previous_set() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();

    socket1
        .multicast_loop_v4()
        .expect("unable to get multicast_loop_v4 for UDP socket");
}

#[test]
fn set_get_multicast_ttl_v4() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();

    const TTL: u32 = 10;
    socket1.set_multicast_ttl_v4(TTL).unwrap();
    assert_eq!(socket1.multicast_ttl_v4().unwrap(), TTL);

    assert!(socket1.take_error().unwrap().is_none());
}

#[test]
fn get_multicast_ttl_v4_without_previous_set() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();

    socket1
        .multicast_ttl_v4()
        .expect("unable to get multicast_ttl_v4 for UDP socket");
}

#[test]
fn set_get_multicast_loop_v6() {
    let socket1 = UdpSocket::bind(any_local_ipv6_address()).unwrap();

    socket1.set_multicast_loop_v6(true).unwrap();
    assert_eq!(socket1.multicast_loop_v6().unwrap(), true);

    socket1.set_multicast_loop_v6(false).unwrap();
    assert_eq!(socket1.multicast_loop_v6().unwrap(), false);

    assert!(socket1.take_error().unwrap().is_none());
}

#[test]
fn get_multicast_loop_v6_without_previous_set() {
    let socket1 = UdpSocket::bind(any_local_ipv6_address()).unwrap();

    socket1
        .multicast_loop_v6()
        .expect("unable to get multicast_loop_v6 for UDP socket");
}

#[test]
fn connected_udp_socket_ipv4() {
    let socket1 = UdpSocket::bind(any_local_address()).unwrap();
    let address1 = socket1.local_addr().unwrap();

    let socket2 = UdpSocket::bind(any_local_address()).unwrap();
    let address2 = socket2.local_addr().unwrap();

    socket1.connect(address2).unwrap();
    socket2.connect(address1).unwrap();

    smoke_test_connected_udp_socket(socket1, socket2);
}

#[test]
fn connected_udp_socket_ipv6() {
    let socket1 = UdpSocket::bind(any_local_ipv6_address()).unwrap();
    let address1 = socket1.local_addr().unwrap();

    let socket2 = UdpSocket::bind(any_local_ipv6_address()).unwrap();
    let address2 = socket2.local_addr().unwrap();

    socket1.connect(address2).unwrap();
    socket2.connect(address1).unwrap();

    smoke_test_connected_udp_socket(socket1, socket2);
}

#[test]
fn connected_udp_socket_std() {
    let socket1 = net::UdpSocket::bind(any_local_address()).unwrap();
    let address1 = socket1.local_addr().unwrap();

    let socket2 = net::UdpSocket::bind(any_local_address()).unwrap();
    let address2 = socket2.local_addr().unwrap();

    socket1.connect(address2).unwrap();
    socket2.connect(address1).unwrap();

    // `std::net::UdpSocket`s are blocking by default, so make sure they are
    // in non-blocking mode before wrapping in a Mio equivalent.
    socket1.set_nonblocking(true).unwrap();
    socket2.set_nonblocking(true).unwrap();

    let socket1 = UdpSocket::from_std(socket1);
    let socket2 = UdpSocket::from_std(socket2);

    smoke_test_connected_udp_socket(socket1, socket2);
}

fn smoke_test_connected_udp_socket(mut socket1: UdpSocket, mut socket2: UdpSocket) {
    let (mut poll, mut events) = init_with_poll();

    assert_socket_non_blocking(&socket1);
    assert_socket_close_on_exec(&socket1);
    assert_socket_non_blocking(&socket2);
    assert_socket_close_on_exec(&socket2);

    poll.registry()
        .register(
            &mut socket1,
            ID1,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .expect("unable to register UDP socket");
    poll.registry()
        .register(
            &mut socket2,
            ID2,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .expect("unable to register UDP socket");

    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(ID1, Interest::WRITABLE),
            ExpectEvent::new(ID2, Interest::WRITABLE),
        ],
    );

    let mut buf = [MaybeUninit::uninit(); 20];
    assert_would_block(socket1.peek(&mut buf));
    assert_would_block(socket1.recv(&mut buf));

    checked_write!(socket1.send(DATA1));
    checked_write!(socket2.send(DATA2));

    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(ID1, Interest::READABLE),
            ExpectEvent::new(ID2, Interest::READABLE),
        ],
    );

    expect_read!(socket1.peek(&mut buf), DATA2);
    expect_read!(socket2.peek(&mut buf), DATA1);

    expect_read!(socket1.recv(&mut buf), DATA2);
    expect_read!(socket2.recv(&mut buf), DATA1);

    assert!(socket1.take_error().unwrap().is_none());
    assert!(socket2.take_error().unwrap().is_none());
}

#[test]
fn reconnect_udp_socket_sending() {
    let (mut poll, mut events) = init_with_poll();

    let mut socket1 = UdpSocket::bind(any_local_address()).unwrap();
    let mut socket2 = UdpSocket::bind(any_local_address()).unwrap();
    let mut socket3 = UdpSocket::bind(any_local_address()).unwrap();

    let address1 = socket1.local_addr().unwrap();
    let address2 = socket2.local_addr().unwrap();
    let address3 = socket3.local_addr().unwrap();

    socket1.connect(address2).unwrap();
    socket2.connect(address1).unwrap();
    socket3.connect(address1).unwrap();

    poll.registry()
        .register(
            &mut socket1,
            ID1,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .unwrap();
    poll.registry()
        .register(&mut socket2, ID2, Interest::READABLE)
        .unwrap();
    poll.registry()
        .register(&mut socket3, ID3, Interest::READABLE)
        .unwrap();

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::WRITABLE)],
    );

    checked_write!(socket1.send(DATA1));

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID2, Interest::READABLE)],
    );

    let mut buf = [MaybeUninit::uninit(); 20];
    expect_read!(socket2.recv(&mut buf), DATA1);

    socket1.connect(address3).unwrap();
    checked_write!(socket1.send(DATA2));

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID3, Interest::READABLE)],
    );

    expect_read!(socket3.recv(&mut buf), DATA2);

    assert!(socket1.take_error().unwrap().is_none());
    assert!(socket2.take_error().unwrap().is_none());
    assert!(socket3.take_error().unwrap().is_none());
}

#[test]
fn reconnect_udp_socket_receiving() {
    let (mut poll, mut events) = init_with_poll();

    let mut socket1 = UdpSocket::bind(any_local_address()).unwrap();
    let mut socket2 = UdpSocket::bind(any_local_address()).unwrap();
    let mut socket3 = UdpSocket::bind(any_local_address()).unwrap();

    let address1 = socket1.local_addr().unwrap();
    let address2 = socket2.local_addr().unwrap();
    let address3 = socket3.local_addr().unwrap();

    socket1.connect(address2).unwrap();
    socket2.connect(address1).unwrap();
    socket3.connect(address1).unwrap();

    poll.registry()
        .register(&mut socket1, ID1, Interest::READABLE)
        .unwrap();
    poll.registry()
        .register(&mut socket2, ID2, Interest::WRITABLE)
        .unwrap();
    poll.registry()
        .register(&mut socket3, ID3, Interest::WRITABLE)
        .unwrap();

    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(ID2, Interest::WRITABLE),
            ExpectEvent::new(ID3, Interest::WRITABLE),
        ],
    );

    checked_write!(socket2.send(DATA1));

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::READABLE)],
    );

    let mut buf = [MaybeUninit::uninit(); 20];
    expect_read!(socket1.recv(&mut buf), DATA1);

    //this will reregister socket1 resetting the interests
    assert_would_block(socket1.recv(&mut buf));

    socket1.connect(address3).unwrap();

    checked_write!(socket3.send(DATA2));

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::READABLE)],
    );

    // Read all data.
    // On Windows, reading part of data returns error WSAEMSGSIZE (10040).
    expect_read!(socket1.recv(&mut buf), DATA2);

    //this will reregister socket1 resetting the interests
    assert_would_block(socket1.recv(&mut buf));

    // Now connect back to socket 2.
    socket1.connect(address2).unwrap();

    checked_write!(socket2.send(DATA2));

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::READABLE)],
    );

    expect_read!(socket1.recv(&mut buf), DATA2);

    assert!(socket1.take_error().unwrap().is_none());
    assert!(socket2.take_error().unwrap().is_none());
    assert!(socket3.take_error().unwrap().is_none());
}

#[test]
fn unconnected_udp_socket_connected_methods() {
    let (mut poll, mut events) = init_with_poll();

    let mut socket1 = UdpSocket::bind(any_local_address()).unwrap();
    let mut socket2 = UdpSocket::bind(any_local_address()).unwrap();
    let address2 = socket2.local_addr().unwrap();

    poll.registry()
        .register(&mut socket1, ID1, Interest::WRITABLE)
        .unwrap();
    poll.registry()
        .register(&mut socket2, ID2, Interest::READABLE)
        .unwrap();

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::WRITABLE)],
    );

    // Socket is unconnected, but we're using an connected method.
    if cfg!(not(target_os = "windows")) {
        assert_error(socket1.send(DATA1), "address required");
    }
    if cfg!(target_os = "windows") {
        assert_error(
            socket1.send(DATA1),
            "no address was supplied. (os error 10057)",
        );
    }

    // Now send some actual data.
    checked_write!(socket1.send_to(DATA1, address2));

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID2, Interest::READABLE)],
    );

    // Receive methods don't require the socket to be connected, you just won't
    // know the sender.
    let mut buf = [MaybeUninit::uninit(); 20];
    expect_read!(socket2.peek(&mut buf), DATA1);
    expect_read!(socket2.recv(&mut buf), DATA1);

    assert!(socket1.take_error().unwrap().is_none());
    assert!(socket2.take_error().unwrap().is_none());
}

#[test]
fn connected_udp_socket_unconnected_methods() {
    let (mut poll, mut events) = init_with_poll();

    let mut socket1 = UdpSocket::bind(any_local_address()).unwrap();
    let mut socket2 = UdpSocket::bind(any_local_address()).unwrap();
    let mut socket3 = UdpSocket::bind(any_local_address()).unwrap();

    let address2 = socket2.local_addr().unwrap();
    let address3 = socket3.local_addr().unwrap();

    socket1.connect(address3).unwrap();
    socket3.connect(address2).unwrap();

    poll.registry()
        .register(&mut socket1, ID1, Interest::WRITABLE)
        .unwrap();
    poll.registry()
        .register(&mut socket2, ID2, Interest::WRITABLE)
        .unwrap();
    poll.registry()
        .register(&mut socket3, ID3, Interest::READABLE)
        .unwrap();

    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(ID1, Interest::WRITABLE),
            ExpectEvent::new(ID2, Interest::WRITABLE),
        ],
    );

    // Can't use `send_to`.
    // Linux (and Android) and Windows actually allow `send_to` even if the
    // socket is connected.
    #[cfg(not(any(target_os = "android", target_os = "linux", target_os = "windows")))]
    assert_error(socket1.send_to(DATA1, address2), "already connected");
    // Even if the address is the same.
    #[cfg(not(any(target_os = "android", target_os = "linux", target_os = "windows")))]
    assert_error(socket1.send_to(DATA1, address3), "already connected");

    checked_write!(socket2.send_to(DATA2, address3));

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID3, Interest::READABLE)],
    );

    let mut buf = [MaybeUninit::uninit(); 20];
    expect_read!(socket3.peek_from(&mut buf), DATA2, address2);
    expect_read!(socket3.recv_from(&mut buf), DATA2, address2);

    assert!(socket1.take_error().unwrap().is_none());
    assert!(socket2.take_error().unwrap().is_none());
    assert!(socket3.take_error().unwrap().is_none());
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn recv_from_empty_buffer() {
    let socket = UdpSocket::bind(any_local_address()).unwrap();
    socket.recv_from(&mut []).unwrap();
}

#[cfg(unix)]
#[test]
fn udp_socket_raw_fd() {
    init();

    let socket = UdpSocket::bind(any_local_address()).unwrap();
    let address = socket.local_addr().unwrap();

    let raw_fd1 = socket.as_raw_fd();
    let raw_fd2 = socket.into_raw_fd();
    assert_eq!(raw_fd1, raw_fd2);

    let socket = unsafe { UdpSocket::from_raw_fd(raw_fd2) };
    assert_eq!(socket.as_raw_fd(), raw_fd1);
    assert_eq!(socket.local_addr().unwrap(), address);
}

#[test]
fn udp_socket_register() {
    let (mut poll, mut events) = init_with_poll();

    let mut socket = UdpSocket::bind(any_local_address()).unwrap();
    poll.registry()
        .register(&mut socket, ID1, Interest::READABLE)
        .expect("unable to register UDP socket");

    expect_no_events(&mut poll, &mut events);

    // NOTE: more tests are done in the smoke tests above.
}

#[test]
fn udp_socket_reregister() {
    let (mut poll, mut events) = init_with_poll();

    let mut socket = UdpSocket::bind(any_local_address()).unwrap();
    let address = socket.local_addr().unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let thread_handle = send_packets(address, 1, barrier.clone());

    poll.registry()
        .register(&mut socket, ID1, Interest::WRITABLE)
        .unwrap();
    // Let the first packet be send.
    barrier.wait();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::WRITABLE)], // Not readable!
    );

    poll.registry()
        .reregister(&mut socket, ID2, Interest::READABLE)
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID2, Interest::READABLE)],
    );

    let mut buf = [MaybeUninit::uninit(); 20];
    expect_read!(socket.recv_from(&mut buf), DATA1, __anywhere);

    thread_handle.join().expect("unable to join thread");
}

#[test]
fn udp_socket_no_events_after_deregister() {
    let (mut poll, mut events) = init_with_poll();

    let mut socket = UdpSocket::bind(any_local_address()).unwrap();
    let address = socket.local_addr().unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let thread_handle = send_packets(address, 1, barrier.clone());

    poll.registry()
        .register(&mut socket, ID1, Interest::READABLE)
        .unwrap();

    // Let the packet be send.
    barrier.wait();

    poll.registry().deregister(&mut socket).unwrap();

    expect_no_events(&mut poll, &mut events);

    // But we do expect a packet to be send.
    let mut buf = [MaybeUninit::uninit(); 20];
    expect_read!(socket.recv_from(&mut buf), DATA1, __anywhere);

    thread_handle.join().expect("unable to join thread");
}

/// Sends `n_packets` packets to `address`, over UDP, after the `barrier` is
/// waited (before each send) on in another thread.
fn send_packets(
    address: SocketAddr,
    n_packets: usize,
    barrier: Arc<Barrier>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let socket = net::UdpSocket::bind(any_local_address()).unwrap();
        for _ in 0..n_packets {
            barrier.wait();
            checked_write!(socket.send_to(DATA1, address));
        }
    })
}

pub struct UdpHandlerSendRecv {
    tx: UdpSocket,
    rx: UdpSocket,
    msg: &'static str,
    buf: Vec<u8>,
    rx_buf: Vec<u8>,
    connected: bool,
    shutdown: bool,
}

impl UdpHandlerSendRecv {
    fn new(tx: UdpSocket, rx: UdpSocket, connected: bool, msg: &'static str) -> UdpHandlerSendRecv {
        UdpHandlerSendRecv {
            tx,
            rx,
            msg,
            buf: msg.as_bytes().to_vec(),
            rx_buf: vec![0; 1024],
            connected,
            shutdown: false,
        }
    }
}

fn send_recv_udp(mut tx: UdpSocket, mut rx: UdpSocket, connected: bool) {
    init();

    debug!("Starting TEST_UDP_SOCKETS");
    let mut poll = Poll::new().unwrap();

    // ensure that the sockets are non-blocking
    let mut buf = [MaybeUninit::uninit(); 128];
    assert_would_block(rx.recv_from(&mut buf));

    info!("Registering SENDER");
    poll.registry()
        .register(&mut tx, SENDER, Interest::WRITABLE)
        .unwrap();

    info!("Registering LISTENER");
    poll.registry()
        .register(&mut rx, LISTENER, Interest::READABLE)
        .unwrap();

    let mut events = Events::with_capacity(1024);

    info!("Starting event loop to test with...");
    let mut handler = UdpHandlerSendRecv::new(tx, rx, connected, "hello world");

    while !handler.shutdown {
        poll.poll(&mut events, None).unwrap();

        for event in &events {
            if event.is_readable() {
                if let LISTENER = event.token() {
                    debug!("We are receiving a datagram now...");
                    // TODO: replace with `Vec::spare_capacity_mut` once stable.
                    let rx_buf = unsafe {
                        slice::from_raw_parts_mut(
                            handler.rx_buf.as_mut_ptr() as *mut MaybeUninit<u8>,
                            handler.rx_buf.capacity(),
                        )
                    };
                    let cnt = if !handler.connected {
                        handler.rx.recv_from(rx_buf).unwrap().0
                    } else {
                        handler.rx.recv(rx_buf).unwrap()
                    };

                    unsafe { handler.rx_buf.set_len(cnt) };
                    assert_eq!(
                        str::from_utf8(handler.rx_buf.as_ref()).unwrap(),
                        handler.msg
                    );
                    handler.shutdown = true;
                }
            }

            if event.is_writable() {
                if let SENDER = event.token() {
                    let cnt = if !handler.connected {
                        let addr = handler.rx.local_addr().unwrap();
                        handler.tx.send_to(&handler.buf, addr).unwrap()
                    } else {
                        handler.tx.send(&handler.buf).unwrap()
                    };

                    // Advance the buffer.
                    drop(handler.buf.drain(..cnt));
                }
            }
        }
    }
}

/// Returns the sender and the receiver
fn connected_sockets() -> (UdpSocket, UdpSocket) {
    let tx = UdpSocket::bind(any_local_address()).unwrap();
    let rx = UdpSocket::bind(any_local_address()).unwrap();

    let tx_addr = tx.local_addr().unwrap();
    let rx_addr = rx.local_addr().unwrap();

    assert!(tx.connect(rx_addr).is_ok());
    assert!(rx.connect(tx_addr).is_ok());

    (tx, rx)
}

#[test]
pub fn udp_socket() {
    init();

    let tx = UdpSocket::bind(any_local_address()).unwrap();
    let rx = UdpSocket::bind(any_local_address()).unwrap();

    send_recv_udp(tx, rx, false);
}

#[test]
pub fn udp_socket_send_recv() {
    init();

    let (tx, rx) = connected_sockets();

    send_recv_udp(tx, rx, true);
}

#[test]
pub fn udp_socket_discard() {
    init();

    let mut tx = UdpSocket::bind(any_local_address()).unwrap();
    let mut rx = UdpSocket::bind(any_local_address()).unwrap();
    let udp_outside = UdpSocket::bind(any_local_address()).unwrap();

    let tx_addr = tx.local_addr().unwrap();
    let rx_addr = rx.local_addr().unwrap();

    assert!(tx.connect(rx_addr).is_ok());
    assert!(udp_outside.connect(rx_addr).is_ok());
    assert!(rx.connect(tx_addr).is_ok());

    let mut poll = Poll::new().unwrap();

    checked_write!(udp_outside.send(b"hello world"));

    poll.registry()
        .register(&mut rx, LISTENER, Interest::READABLE)
        .unwrap();
    poll.registry()
        .register(&mut tx, SENDER, Interest::WRITABLE)
        .unwrap();

    let mut events = Events::with_capacity(1024);

    poll.poll(&mut events, Some(Duration::from_secs(5)))
        .unwrap();

    for event in &events {
        if event.is_readable() {
            if let LISTENER = event.token() {
                panic!("Expected to no receive a packet but got something")
            }
        }
    }
}

pub struct UdpHandler {
    tx: UdpSocket,
    rx: UdpSocket,
    msg: &'static str,
    buf: Vec<u8>,
    rx_buf: Vec<u8>,
    localhost: IpAddr,
    shutdown: bool,
}

impl UdpHandler {
    fn new(tx: UdpSocket, rx: UdpSocket, msg: &'static str) -> UdpHandler {
        let sock = UdpSocket::bind(any_local_address()).unwrap();
        UdpHandler {
            tx,
            rx,
            msg,
            buf: msg.as_bytes().to_vec(),
            rx_buf: Vec::with_capacity(1024),
            localhost: sock.local_addr().unwrap().ip(),
            shutdown: false,
        }
    }

    fn handle_read(&mut self, _: &Registry, token: Token) {
        if let LISTENER = token {
            debug!("We are receiving a datagram now...");
            unsafe { self.rx_buf.set_len(self.rx_buf.capacity()) };
            // TODO: replace with `Vec::spare_capacity_mut` once stable.
            let rx_buf = unsafe {
                slice::from_raw_parts_mut(
                    self.rx_buf.as_mut_ptr() as *mut MaybeUninit<u8>,
                    self.rx_buf.capacity(),
                )
            };
            match self.rx.recv_from(rx_buf) {
                Ok((cnt, addr)) => {
                    unsafe { self.rx_buf.set_len(cnt) };
                    assert_eq!(addr.ip(), self.localhost);
                }
                res => panic!("unexpected result: {:?}", res),
            }
            assert_eq!(str::from_utf8(&self.rx_buf).unwrap(), self.msg);
            self.shutdown = true;
        }
    }

    fn handle_write(&mut self, _: &Registry, token: Token) {
        if let SENDER = token {
            let addr = self.rx.local_addr().unwrap();
            let cnt = self.tx.send_to(self.buf.as_ref(), addr).unwrap();
            self.buf.drain(..cnt);
        }
    }
}

// TODO: This doesn't pass on android 64bit CI...
// Figure out why!
#[cfg_attr(
    target_os = "android",
    ignore = "Multicast doesn't work on Android 64bit"
)]
#[test]
pub fn multicast() {
    init();

    debug!("Starting TEST_UDP_CONNECTIONLESS");
    let mut poll = Poll::new().unwrap();

    let mut tx = UdpSocket::bind(any_local_address()).unwrap();
    let mut rx = UdpSocket::bind(any_local_address()).unwrap();

    info!("Joining group 227.1.1.100");
    let any = &"0.0.0.0".parse().unwrap();
    rx.join_multicast_v4(&"227.1.1.100".parse().unwrap(), any)
        .unwrap();

    info!("Joining group 227.1.1.101");
    rx.join_multicast_v4(&"227.1.1.101".parse().unwrap(), any)
        .unwrap();

    info!("Registering SENDER");
    poll.registry()
        .register(&mut tx, SENDER, Interest::WRITABLE)
        .unwrap();

    info!("Registering LISTENER");
    poll.registry()
        .register(&mut rx, LISTENER, Interest::READABLE)
        .unwrap();

    let mut events = Events::with_capacity(1024);

    let mut handler = UdpHandler::new(tx, rx, "hello world");

    info!("Starting event loop to test with...");

    while !handler.shutdown {
        poll.poll(&mut events, None).unwrap();

        for event in &events {
            if event.is_readable() {
                handler.handle_read(poll.registry(), event.token());
            }

            if event.is_writable() {
                handler.handle_write(poll.registry(), event.token());
            }
        }
    }
}

#[test]
fn et_behavior_recv() {
    let (mut poll, mut events) = init_with_poll();

    let mut socket1 = UdpSocket::bind(any_local_address()).unwrap();
    let mut socket2 = UdpSocket::bind(any_local_address()).unwrap();

    let address2 = socket2.local_addr().unwrap();

    poll.registry()
        .register(&mut socket1, ID1, Interest::WRITABLE)
        .expect("unable to register UDP socket");
    poll.registry()
        .register(
            &mut socket2,
            ID2,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .expect("unable to register UDP socket");

    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(ID1, Interest::WRITABLE),
            ExpectEvent::new(ID2, Interest::WRITABLE),
        ],
    );

    socket1.connect(address2).unwrap();

    let mut buf = [MaybeUninit::uninit(); 20];
    checked_write!(socket1.send(DATA1));
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID2, Interest::READABLE)],
    );

    expect_read!(socket2.recv(&mut buf), DATA1);

    // this will reregister the socket2, resetting the interests
    assert_would_block(socket2.recv(&mut buf));
    checked_write!(socket1.send(DATA1));
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID2, Interest::READABLE)],
    );

    expect_read!(socket2.recv(&mut buf), DATA1);
}

#[test]
fn et_behavior_recv_from() {
    let (mut poll, mut events) = init_with_poll();

    let mut socket1 = UdpSocket::bind(any_local_address()).unwrap();
    let mut socket2 = UdpSocket::bind(any_local_address()).unwrap();

    let address1 = socket1.local_addr().unwrap();
    let address2 = socket2.local_addr().unwrap();

    poll.registry()
        .register(
            &mut socket1,
            ID1,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .expect("unable to register UDP socket");
    poll.registry()
        .register(
            &mut socket2,
            ID2,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .expect("unable to register UDP socket");

    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(ID1, Interest::WRITABLE),
            ExpectEvent::new(ID2, Interest::WRITABLE),
        ],
    );

    checked_write!(socket1.send_to(DATA1, address2));

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID2, Interest::READABLE)],
    );

    let mut buf = [MaybeUninit::uninit(); 20];
    expect_read!(socket2.recv_from(&mut buf), DATA1, address1);

    // this will reregister the socket2, resetting the interests
    assert_would_block(socket2.recv_from(&mut buf));
    checked_write!(socket1.send_to(DATA1, address2));
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID2, Interest::READABLE)],
    );

    expect_read!(socket2.recv_from(&mut buf), DATA1, address1);

    assert!(socket1.take_error().unwrap().is_none());
    assert!(socket2.take_error().unwrap().is_none());
}
