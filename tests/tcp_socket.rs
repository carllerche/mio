#![cfg(all(feature = "os-poll", feature = "net"))]

use mio::net::{TcpKeepalive, TcpSocket};
use std::io;
use std::time::Duration;

#[test]
fn is_send_and_sync() {
    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    is_send::<TcpSocket>();
    is_sync::<TcpSocket>();
}

#[test]
fn set_reuseaddr() {
    let addr = "127.0.0.1:0".parse().unwrap();

    let socket = TcpSocket::new_v4().unwrap();
    socket.set_reuseaddr(true).unwrap();
    assert!(socket.get_reuseaddr().unwrap());

    socket.bind(addr).unwrap();

    let _ = socket.listen(128).unwrap();
}

#[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
#[test]
fn set_reuseport() {
    let addr = "127.0.0.1:0".parse().unwrap();

    let socket = TcpSocket::new_v4().unwrap();
    socket.set_reuseport(true).unwrap();
    assert!(socket.get_reuseport().unwrap());

    socket.bind(addr).unwrap();

    let _ = socket.listen(128).unwrap();
}

#[test]
fn set_keepalive() {
    let addr = "127.0.0.1:0".parse().unwrap();

    let socket = TcpSocket::new_v4().unwrap();
    socket.set_keepalive(false).unwrap();
    assert!(!socket.get_keepalive().unwrap());

    socket.set_keepalive(true).unwrap();
    assert!(socket.get_keepalive().unwrap());

    socket.bind(addr).unwrap();

    let _ = socket.listen(128).unwrap();
}

#[test]
fn set_keepalive_time() {
    let dur = Duration::from_secs(4); // Chosen by fair dice roll, guaranteed to be random
    let addr = "127.0.0.1:0".parse().unwrap();

    let socket = TcpSocket::new_v4().unwrap();
    socket
        .set_keepalive_params(TcpKeepalive::default().with_time(dur))
        .unwrap();

    // It's not possible to access keepalive parameters on Windows...
    #[cfg(not(target_os = "windows"))]
    assert_eq!(Some(dur), socket.get_keepalive_time().unwrap());

    socket.bind(addr).unwrap();

    let _ = socket.listen(128).unwrap();
}

#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "windows"
))]
#[test]
fn set_keepalive_interval() {
    let dur = Duration::from_secs(4); // Chosen by fair dice roll, guaranteed to be random
    let addr = "127.0.0.1:0".parse().unwrap();

    let socket = TcpSocket::new_v4().unwrap();
    socket
        .set_keepalive_params(TcpKeepalive::default().with_interval(dur))
        .unwrap();
    // It's not possible to access keepalive parameters on Windows...
    #[cfg(not(target_os = "windows"))]
    assert_eq!(Some(dur), socket.get_keepalive_interval().unwrap());

    socket.bind(addr).unwrap();

    let _ = socket.listen(128).unwrap();
}

#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "netbsd",
))]
#[test]
fn set_keepalive_retries() {
    let addr = "127.0.0.1:0".parse().unwrap();

    let socket = TcpSocket::new_v4().unwrap();
    socket
        .set_keepalive_params(TcpKeepalive::default().with_retries(16))
        .unwrap();
    assert_eq!(Some(16), socket.get_keepalive_retries().unwrap());

    socket.bind(addr).unwrap();

    let _ = socket.listen(128).unwrap();
}

#[test]
fn get_localaddr() {
    let expected_addr = "127.0.0.1:0".parse().unwrap();
    let socket = TcpSocket::new_v4().unwrap();

    //Windows doesn't support calling getsockname before calling `bind`
    #[cfg(not(windows))]
    assert_eq!("0.0.0.0:0", socket.get_localaddr().unwrap().to_string());

    socket.bind(expected_addr).unwrap();

    let actual_addr = socket.get_localaddr().unwrap();

    assert_eq!(expected_addr.ip(), actual_addr.ip());
    assert!(actual_addr.port() > 0);

    let _ = socket.listen(128).unwrap();
}

#[test]
fn set_linger() {
    let addr = "127.0.0.1:0".parse().unwrap();

    let socket = TcpSocket::new_v4().unwrap();
    socket.set_linger(Some(Duration::from_secs(1))).unwrap();
    assert_eq!(socket.get_linger().unwrap().unwrap().as_secs(), 1);

    let _ = socket.set_linger(None);
    assert_eq!(socket.get_linger().unwrap(), None);

    socket.bind(addr).unwrap();

    let _ = socket.listen(128).unwrap();
}

#[test]
fn send_buffer_size_roundtrips() {
    test_buffer_sizes(
        TcpSocket::set_send_buffer_size,
        TcpSocket::get_send_buffer_size,
    )
}

#[test]
fn recv_buffer_size_roundtrips() {
    test_buffer_sizes(
        TcpSocket::set_recv_buffer_size,
        TcpSocket::get_recv_buffer_size,
    )
}

// Helper for testing send/recv buffer size.
fn test_buffer_sizes(
    set: impl Fn(&TcpSocket, u32) -> io::Result<()>,
    get: impl Fn(&TcpSocket) -> io::Result<u32>,
) {
    let test = |size: u32| {
        println!("testing buffer size: {}", size);
        let socket = TcpSocket::new_v4().unwrap();
        set(&socket, size).unwrap();
        // Note that this doesn't assert that the values are equal: on Linux,
        // the kernel doubles the requested buffer size, and returns the doubled
        // value from `getsockopt`. As per `man socket(7)`:
        // > Sets or gets the maximum socket send/receive buffer in bytes. The
        // > kernel doubles this value (to allow space for bookkeeping
        // > overhead) when it is set using setsockopt(2), and this doubled
        // > value is returned by getsockopt(2).
        //
        // Additionally, the buffer size may be clamped above a minimum value,
        // and this minimum value is OS-dependent.
        let actual = get(&socket).unwrap();
        assert!(actual >= size, "\tactual: {}\n\texpected: {}", actual, size);
    };

    test(256);
    test(4096);
    test(65512);
}
