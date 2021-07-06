#![cfg(all(unix, feature = "os-poll", feature = "net"))]

use mio::net::UnixDatagram;
use mio::{Interest, Token};
use std::io;
use std::net::Shutdown;
use std::os::unix::net;

#[macro_use]
mod util;
use util::{
    assert_send, assert_socket_close_on_exec, assert_socket_non_blocking, assert_sync,
    assert_would_block, expect_events, expect_no_events, init, init_with_poll, temp_file,
    ExpectEvent, Readiness,
};

const DATA1: &[u8] = b"Hello same host!";
const DATA2: &[u8] = b"Why hello mio!";
const DEFAULT_BUF_SIZE: usize = 64;
const TOKEN_1: Token = Token(0);
const TOKEN_2: Token = Token(1);

#[test]
fn is_send_and_sync() {
    assert_send::<UnixDatagram>();
    assert_sync::<UnixDatagram>();
}

#[test]
fn unix_datagram_smoke_unconnected() {
    init();
    let path1 = temp_file("unix_datagram_smoke_unconnected1");
    let path2 = temp_file("unix_datagram_smoke_unconnected2");

    let datagram1 = UnixDatagram::bind(&path1).unwrap();
    let datagram2 = UnixDatagram::bind(&path2).unwrap();
    smoke_test_unconnected(datagram1, datagram2);
}

#[test]
fn unix_datagram_smoke_connected() {
    init();
    let path1 = temp_file("unix_datagram_smoke_connected1");
    let path2 = temp_file("unix_datagram_smoke_connected2");

    let datagram1 = UnixDatagram::bind(&path1).unwrap();
    let datagram2 = UnixDatagram::bind(&path2).unwrap();

    datagram1.connect(&path2).unwrap();
    datagram2.connect(&path1).unwrap();
    smoke_test_connected(datagram1, datagram2);
}

#[test]
fn unix_datagram_smoke_unconnected_from_std() {
    init();
    let path1 = temp_file("unix_datagram_smoke_unconnected_from_std1");
    let path2 = temp_file("unix_datagram_smoke_unconnected_from_std2");

    let datagram1 = net::UnixDatagram::bind(&path1).unwrap();
    let datagram2 = net::UnixDatagram::bind(&path2).unwrap();

    datagram1.set_nonblocking(true).unwrap();
    datagram2.set_nonblocking(true).unwrap();

    let datagram1 = UnixDatagram::from_std(datagram1);
    let datagram2 = UnixDatagram::from_std(datagram2);
    smoke_test_unconnected(datagram1, datagram2);
}

#[test]
fn unix_datagram_smoke_connected_from_std() {
    init();
    let path1 = temp_file("unix_datagram_smoke_connected_from_std1");
    let path2 = temp_file("unix_datagram_smoke_connected_from_std2");

    let datagram1 = net::UnixDatagram::bind(&path1).unwrap();
    let datagram2 = net::UnixDatagram::bind(&path2).unwrap();

    datagram1.connect(&path2).unwrap();
    datagram2.connect(&path1).unwrap();

    datagram1.set_nonblocking(true).unwrap();
    datagram2.set_nonblocking(true).unwrap();

    let datagram1 = UnixDatagram::from_std(datagram1);
    let datagram2 = UnixDatagram::from_std(datagram2);
    smoke_test_connected(datagram1, datagram2);
}

#[test]
fn unix_datagram_connect() {
    init();
    let path1 = temp_file("unix_datagram_connect1");
    let path2 = temp_file("unix_datagram_connect2");

    let datagram1 = UnixDatagram::bind(&path1).unwrap();
    let datagram1_local = datagram1.local_addr().unwrap();
    let datagram2 = UnixDatagram::bind(&path2).unwrap();
    let datagram2_local = datagram2.local_addr().unwrap();

    datagram1
        .connect(
            datagram1_local
                .as_pathname()
                .expect("failed to get pathname"),
        )
        .unwrap();
    datagram2
        .connect(
            datagram2_local
                .as_pathname()
                .expect("failed to get pathname"),
        )
        .unwrap();
}

#[test]
fn unix_datagram_pair() {
    let (mut poll, mut events) = init_with_poll();

    let (mut datagram1, mut datagram2) = UnixDatagram::pair().unwrap();
    poll.registry()
        .register(
            &mut datagram1,
            TOKEN_1,
            Interest::READABLE | Interest::WRITABLE,
        )
        .unwrap();
    poll.registry()
        .register(
            &mut datagram2,
            TOKEN_2,
            Interest::READABLE | Interest::WRITABLE,
        )
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(TOKEN_1, Interest::WRITABLE),
            ExpectEvent::new(TOKEN_2, Interest::WRITABLE),
        ],
    );

    let mut buf = [0; DEFAULT_BUF_SIZE];
    assert_would_block(datagram1.recv(&mut buf));
    assert_would_block(datagram2.recv(&mut buf));

    checked_write!(datagram1.send(DATA1));
    checked_write!(datagram2.send(DATA2));
    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(TOKEN_1, Interest::READABLE),
            ExpectEvent::new(TOKEN_2, Interest::READABLE),
        ],
    );

    expect_read!(datagram1.recv(&mut buf), DATA2);
    expect_read!(datagram2.recv(&mut buf), DATA1);

    assert!(datagram1.take_error().unwrap().is_none());
    assert!(datagram2.take_error().unwrap().is_none());
}

#[test]
fn unix_datagram_shutdown() {
    let (mut poll, mut events) = init_with_poll();
    let path1 = temp_file("unix_datagram_shutdown1");
    let path2 = temp_file("unix_datagram_shutdown2");

    let mut datagram1 = UnixDatagram::bind(&path1).unwrap();
    let mut datagram2 = UnixDatagram::bind(&path2).unwrap();

    poll.registry()
        .register(
            &mut datagram1,
            TOKEN_1,
            Interest::WRITABLE.add(Interest::READABLE),
        )
        .unwrap();
    poll.registry()
        .register(
            &mut datagram2,
            TOKEN_2,
            Interest::WRITABLE.add(Interest::READABLE),
        )
        .unwrap();

    datagram1.connect(&path2).unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );

    checked_write!(datagram1.send(DATA1));
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_2, Interest::READABLE)],
    );

    datagram1.shutdown(Shutdown::Read).unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Readiness::READ_CLOSED)],
    );

    datagram1.shutdown(Shutdown::Write).unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Readiness::WRITE_CLOSED)],
    );

    let err = datagram1.send(DATA2).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);

    assert!(datagram1.take_error().unwrap().is_none());
}

#[test]
fn unix_datagram_register() {
    let (mut poll, mut events) = init_with_poll();
    let path = temp_file("unix_datagram_register");

    let mut datagram = UnixDatagram::bind(path).unwrap();
    poll.registry()
        .register(&mut datagram, TOKEN_1, Interest::READABLE)
        .unwrap();
    expect_no_events(&mut poll, &mut events);
}

#[test]
fn unix_datagram_reregister() {
    let (mut poll, mut events) = init_with_poll();
    let path1 = temp_file("unix_datagram_reregister1");
    let path2 = temp_file("unix_datagram_reregister2");

    let mut datagram1 = UnixDatagram::bind(&path1).unwrap();
    poll.registry()
        .register(&mut datagram1, TOKEN_1, Interest::READABLE)
        .unwrap();

    let datagram2 = UnixDatagram::bind(&path2).unwrap();
    datagram2.connect(&path1).unwrap();
    poll.registry()
        .reregister(&mut datagram1, TOKEN_1, Interest::WRITABLE)
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );
}

#[test]
fn unix_datagram_deregister() {
    let (mut poll, mut events) = init_with_poll();
    let path1 = temp_file("unix_datagram_deregister1");
    let path2 = temp_file("unix_datagram_deregister2");

    let mut datagram1 = UnixDatagram::bind(&path1).unwrap();
    poll.registry()
        .register(&mut datagram1, TOKEN_1, Interest::WRITABLE)
        .unwrap();

    let datagram2 = UnixDatagram::bind(&path2).unwrap();
    datagram2.connect(&path1).unwrap();
    poll.registry().deregister(&mut datagram1).unwrap();
    expect_no_events(&mut poll, &mut events);
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

    let path1 = temp_file("unix_datagram_event_hint1");
    let path2 = temp_file("unix_datagram_event_hint2");

    let mut datagram1 = UnixDatagram::bind(&path1).unwrap();

    poll.registry()
        .register(
            &mut datagram1,
            TOKEN_1,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .unwrap();

    poll.poll(&mut events, Some(Duration::from_secs(1)))
        .unwrap();
    assert!(events.iter().count() >= 1);

    let mut checked = false;
    for event in events.iter() {
        if !event.is_writable() {
            continue;
        }
        assert_eq!(event.token(), TOKEN_1);

        // Expect the hint to contain the number of bytes send.
        if !matches!(event.hint(), Some(Hint::Writable(..))) {
            panic!("missing hint: {:#?}", event);
        }
        checked = true;
    }
    assert!(checked, "missing writable event: {:?}", events);

    let datagram2 = net::UnixDatagram::bind(&path2).unwrap();
    datagram2.send_to(DATA1, path1).unwrap();

    poll.poll(&mut events, Some(Duration::from_secs(1)))
        .unwrap();
    assert!(events.iter().count() >= 1);

    let mut checked = false;
    for event in events.iter() {
        if !event.is_readable() {
            continue;
        }
        assert_eq!(event.token(), TOKEN_1);

        // Expect the hint to contain the number of bytes send.
        assert_eq!(
            event.hint(),
            Some(Hint::Readable(DATA1.len())),
            "missing hint: {:#?}",
            event
        );
        checked = true;
    }
    assert!(checked, "missing readable event: {:?}", events);

    let mut buf = [0; 20];
    let n = datagram1.recv(&mut buf).unwrap();
    assert_eq!(n, DATA1.len());
    assert_eq!(&buf[..n], &*DATA1);
}

fn smoke_test_unconnected(mut datagram1: UnixDatagram, mut datagram2: UnixDatagram) {
    let (mut poll, mut events) = init_with_poll();

    assert_socket_non_blocking(&datagram1);
    assert_socket_close_on_exec(&datagram1);
    assert_socket_non_blocking(&datagram2);
    assert_socket_close_on_exec(&datagram2);

    let addr1 = datagram1.local_addr().unwrap();
    let addr2 = datagram2.local_addr().unwrap();
    let path1 = addr1.as_pathname().expect("failed to get pathname");
    let path2 = addr2.as_pathname().expect("failed to get pathname");

    poll.registry()
        .register(
            &mut datagram1,
            TOKEN_1,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .unwrap();
    poll.registry()
        .register(
            &mut datagram2,
            TOKEN_2,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(TOKEN_1, Interest::WRITABLE),
            ExpectEvent::new(TOKEN_2, Interest::WRITABLE),
        ],
    );

    let mut buf = [0; DEFAULT_BUF_SIZE];
    assert_would_block(datagram1.recv_from(&mut buf));
    assert_would_block(datagram2.recv_from(&mut buf));

    checked_write!(datagram1.send_to(DATA1, path2));
    checked_write!(datagram2.send_to(DATA2, path1));
    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(TOKEN_1, Interest::READABLE),
            ExpectEvent::new(TOKEN_2, Interest::READABLE),
        ],
    );

    expect_read!(datagram1.recv_from(&mut buf), DATA2, path: path2);
    expect_read!(datagram2.recv_from(&mut buf), DATA1, path: path1);

    assert!(datagram1.take_error().unwrap().is_none());
    assert!(datagram2.take_error().unwrap().is_none());
}

fn smoke_test_connected(mut datagram1: UnixDatagram, mut datagram2: UnixDatagram) {
    let (mut poll, mut events) = init_with_poll();

    assert_socket_non_blocking(&datagram1);
    assert_socket_close_on_exec(&datagram1);
    assert_socket_non_blocking(&datagram2);
    assert_socket_close_on_exec(&datagram2);

    let local_addr1 = datagram1.local_addr().unwrap();
    let peer_addr1 = datagram1.peer_addr().unwrap();
    let local_addr2 = datagram2.local_addr().unwrap();
    let peer_addr2 = datagram2.peer_addr().unwrap();
    assert_eq!(
        local_addr1.as_pathname().expect("failed to get pathname"),
        peer_addr2.as_pathname().expect("failed to get pathname")
    );
    assert_eq!(
        local_addr2.as_pathname().expect("failed to get pathname"),
        peer_addr1.as_pathname().expect("failed to get pathname")
    );

    poll.registry()
        .register(
            &mut datagram1,
            TOKEN_1,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .unwrap();
    poll.registry()
        .register(
            &mut datagram2,
            TOKEN_2,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(TOKEN_1, Interest::WRITABLE),
            ExpectEvent::new(TOKEN_2, Interest::WRITABLE),
        ],
    );

    let mut buf = [0; DEFAULT_BUF_SIZE];
    assert_would_block(datagram1.recv(&mut buf));
    assert_would_block(datagram2.recv(&mut buf));

    checked_write!(datagram1.send(DATA1));
    checked_write!(datagram2.send(DATA2));
    expect_events(
        &mut poll,
        &mut events,
        vec![
            ExpectEvent::new(TOKEN_1, Interest::READABLE),
            ExpectEvent::new(TOKEN_2, Interest::READABLE),
        ],
    );

    expect_read!(datagram1.recv(&mut buf), DATA2);
    expect_read!(datagram2.recv(&mut buf), DATA1);

    assert!(datagram1.take_error().unwrap().is_none());
    assert!(datagram2.take_error().unwrap().is_none());
}
