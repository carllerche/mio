#![cfg(all(feature = "os-poll", feature = "net"))]

use mio::net::TcpListener;
use mio::{Interest, Token};
use std::io::{self, Read};
use std::net::{self, SocketAddr};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
use std::sync::{Arc, Barrier};
use std::thread;

mod util;
use util::{
    any_local_address, any_local_ipv6_address, assert_send, assert_socket_close_on_exec,
    assert_socket_non_blocking, assert_sync, assert_would_block, expect_events, expect_no_events,
    init, init_with_poll, ExpectEvent,
};

const ID1: Token = Token(0);
const ID2: Token = Token(1);

#[test]
fn is_send_and_sync() {
    assert_send::<TcpListener>();
    assert_sync::<TcpListener>();
}

#[test]
fn tcp_listener() {
    smoke_test_tcp_listener(any_local_address(), TcpListener::bind);
}

#[test]
fn tcp_listener_ipv6() {
    smoke_test_tcp_listener(any_local_ipv6_address(), TcpListener::bind);
}

#[test]
fn tcp_listener_std() {
    smoke_test_tcp_listener(any_local_address(), |addr| {
        let listener = net::TcpListener::bind(addr).unwrap();
        // `std::net::TcpListener`s are blocking by default, so make sure it is in
        // non-blocking mode before wrapping in a Mio equivalent.
        listener.set_nonblocking(true).unwrap();
        Ok(TcpListener::from_std(listener))
    });
}

fn smoke_test_tcp_listener<F>(addr: SocketAddr, make_listener: F)
where
    F: FnOnce(SocketAddr) -> io::Result<TcpListener>,
{
    let (mut poll, mut events) = init_with_poll();

    let mut listener = make_listener(addr).unwrap();
    let address = listener.local_addr().unwrap();

    assert_socket_non_blocking(&listener);
    assert_socket_close_on_exec(&listener);

    poll.registry()
        .register(&mut listener, ID1, Interest::READABLE)
        .expect("unable to register TCP listener");

    let barrier = Arc::new(Barrier::new(2));
    let thread_handle = start_connections(address, 1, barrier.clone());

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::READABLE)],
    );

    // Expect a single connection.
    let (mut stream, peer_address) = listener.accept().expect("unable to accept connection");
    assert!(peer_address.ip().is_loopback());
    assert_eq!(stream.peer_addr().unwrap(), peer_address);
    assert_eq!(stream.local_addr().unwrap(), address);

    // Expect the stream to be non-blocking.
    let mut buf = [0; 20];
    assert_would_block(stream.read(&mut buf));

    // Expect no more connections.
    assert_would_block(listener.accept());

    assert!(listener.take_error().unwrap().is_none());

    barrier.wait();
    thread_handle.join().expect("unable to join thread");
}

#[test]
fn set_get_ttl() {
    init();

    let listener = TcpListener::bind(any_local_address()).unwrap();

    // set TTL, get TTL, make sure it has the expected value
    const TTL: u32 = 10;
    listener.set_ttl(TTL).unwrap();
    assert_eq!(listener.ttl().unwrap(), TTL);
    assert!(listener.take_error().unwrap().is_none());
}

#[test]
fn get_ttl_without_previous_set() {
    init();

    let listener = TcpListener::bind(any_local_address()).unwrap();

    // expect a get TTL to work w/o any previous set_ttl
    listener.ttl().expect("unable to get TTL for TCP listener");
    assert!(listener.take_error().unwrap().is_none());
}

#[cfg(unix)]
#[test]
fn raw_fd() {
    init();

    let listener = TcpListener::bind(any_local_address()).unwrap();
    let address = listener.local_addr().unwrap();

    let raw_fd1 = listener.as_raw_fd();
    let raw_fd2 = listener.into_raw_fd();
    assert_eq!(raw_fd1, raw_fd2);

    let listener = unsafe { TcpListener::from_raw_fd(raw_fd2) };
    assert_eq!(listener.as_raw_fd(), raw_fd1);
    assert_eq!(listener.local_addr().unwrap(), address);
}

#[test]
fn registering() {
    let (mut poll, mut events) = init_with_poll();

    let mut stream = TcpListener::bind(any_local_address()).unwrap();

    poll.registry()
        .register(&mut stream, ID1, Interest::READABLE)
        .expect("unable to register TCP listener");

    expect_no_events(&mut poll, &mut events);

    // NOTE: more tests are done in the smoke tests above.
}

#[test]
fn reregister() {
    let (mut poll, mut events) = init_with_poll();

    let mut listener = TcpListener::bind(any_local_address()).unwrap();
    let address = listener.local_addr().unwrap();

    poll.registry()
        .register(&mut listener, ID1, Interest::READABLE)
        .unwrap();
    poll.registry()
        .reregister(&mut listener, ID2, Interest::READABLE)
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let thread_handle = start_connections(address, 1, barrier.clone());

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(ID2, Interest::READABLE)],
    );

    let (stream, peer_address) = listener.accept().expect("unable to accept connection");
    assert!(peer_address.ip().is_loopback());
    assert_eq!(stream.peer_addr().unwrap(), peer_address);
    assert_eq!(stream.local_addr().unwrap(), address);

    assert_would_block(listener.accept());

    assert!(listener.take_error().unwrap().is_none());

    barrier.wait();
    thread_handle.join().expect("unable to join thread");
}

#[test]
fn no_events_after_deregister() {
    let (mut poll, mut events) = init_with_poll();

    let mut listener = TcpListener::bind(any_local_address()).unwrap();
    let address = listener.local_addr().unwrap();

    poll.registry()
        .register(&mut listener, ID1, Interest::READABLE)
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let thread_handle = start_connections(address, 1, barrier.clone());

    poll.registry().deregister(&mut listener).unwrap();

    expect_no_events(&mut poll, &mut events);

    // Should still be able to accept the connection.
    let (stream, peer_address) = listener.accept().expect("unable to accept connection");
    assert!(peer_address.ip().is_loopback());
    assert_eq!(stream.peer_addr().unwrap(), peer_address);
    assert_eq!(stream.local_addr().unwrap(), address);

    assert_would_block(listener.accept());

    assert!(listener.take_error().unwrap().is_none());

    barrier.wait();
    thread_handle.join().expect("unable to join thread");
}

/// This tests reregister on successful accept works
#[test]
fn tcp_listener_two_streams() {
    let (mut poll1, mut events) = init_with_poll();

    let mut listener = TcpListener::bind(any_local_address()).unwrap();
    let address = listener.local_addr().unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let thread_handle1 = start_connections(address, 1, barrier.clone());

    poll1
        .registry()
        .register(&mut listener, ID1, Interest::READABLE)
        .unwrap();

    expect_events(
        &mut poll1,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::READABLE)],
    );

    {
        let (stream, peer_address) = listener.accept().expect("unable to accept connection");
        assert!(peer_address.ip().is_loopback());
        assert_eq!(stream.peer_addr().unwrap(), peer_address);
        assert_eq!(stream.local_addr().unwrap(), address);
    }

    assert_would_block(listener.accept());

    let thread_handle2 = start_connections(address, 1, barrier.clone());

    expect_events(
        &mut poll1,
        &mut events,
        vec![ExpectEvent::new(ID1, Interest::READABLE)],
    );

    {
        let (stream, peer_address) = listener.accept().expect("unable to accept connection");
        assert!(peer_address.ip().is_loopback());
        assert_eq!(stream.peer_addr().unwrap(), peer_address);
        assert_eq!(stream.local_addr().unwrap(), address);
    }

    expect_no_events(&mut poll1, &mut events);

    barrier.wait();
    thread_handle1.join().expect("unable to join thread");
    thread_handle2.join().expect("unable to join thread");
}

// Only `kqueue(2)` supports hints in event.
#[test]
#[cfg(any(
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "ios",
    target_os = "macos",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn event_hint() {
    use std::time::Duration;

    use mio::event::Hint;

    let (mut poll, mut events) = init_with_poll();

    let mut listener = TcpListener::bind(any_local_address()).unwrap();
    let address = listener.local_addr().unwrap();

    const N: usize = 5;
    let barrier = Arc::new(Barrier::new(N + 1));
    let thread_handles = (0..N)
        .map(|_| start_connections(address, 1, barrier.clone()))
        .collect::<Vec<_>>();

    poll.registry()
        .register(&mut listener, ID1, Interest::READABLE)
        .unwrap();

    poll.poll(&mut events, Some(Duration::from_secs(1)))
        .unwrap();
    assert_eq!(events.iter().count(), 1);

    // Expect the hint to contain the number of connections in the backlog.
    let event = events.iter().nth(0).unwrap();
    assert_eq!(event.token(), ID1);
    assert_eq!(
        event.hint(),
        Some(Hint::Readable(N)),
        "missing hint: {:#?}",
        event
    );

    barrier.wait();
    for handle in thread_handles {
        handle.join().expect("unable to join thread");
    }
}

/// Start `n_connections` connections to `address`. If a `barrier` is provided
/// it will wait on it after each connection is made before it is dropped.
fn start_connections(
    address: SocketAddr,
    n_connections: usize,
    barrier: Arc<Barrier>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for _ in 0..n_connections {
            let conn = net::TcpStream::connect(address).unwrap();
            barrier.wait();
            drop(conn);
        }
    })
}
