#![cfg(all(unix, feature = "os-poll", feature = "net"))]

use mio::net::UnixStream;
use mio::{Interest, Token};
use std::io::{self, IoSlice, IoSliceMut, Read, Write};
use std::net::Shutdown;
use std::os::unix::net;
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::{Arc, Barrier};
use std::thread;

#[macro_use]
mod util;
use util::{
    assert_send, assert_socket_close_on_exec, assert_socket_non_blocking, assert_sync,
    assert_would_block, expect_events, expect_no_events, init, init_with_poll, temp_file,
    ExpectEvent, Readiness,
};

const DATA1: &[u8] = b"Hello same host!";
const DATA2: &[u8] = b"Why hello mio!";
const DATA1_LEN: usize = 16;
const DATA2_LEN: usize = 14;
const DEFAULT_BUF_SIZE: usize = 64;
const TOKEN_1: Token = Token(0);
const TOKEN_2: Token = Token(1);

#[test]
fn unix_stream_send_and_sync() {
    assert_send::<UnixStream>();
    assert_sync::<UnixStream>();
}

#[test]
fn unix_stream_smoke() {
    #[allow(clippy::redundant_closure)]
    smoke_test(|path| UnixStream::connect(path), "unix_stream_smoke");
}

#[test]
fn unix_stream_connect() {
    let (mut poll, mut events) = init_with_poll();
    let barrier = Arc::new(Barrier::new(2));
    let path = temp_file("unix_stream_connect");

    let listener = net::UnixListener::bind(path.clone()).unwrap();
    let mut stream = UnixStream::connect(path).unwrap();

    let barrier_clone = barrier.clone();
    let handle = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        barrier_clone.wait();
        drop(stream);
    });

    poll.registry()
        .register(
            &mut stream,
            TOKEN_1,
            Interest::READABLE | Interest::WRITABLE,
        )
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );

    barrier.wait();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::READABLE)],
    );

    handle.join().unwrap();
}

#[test]
fn unix_stream_from_std() {
    smoke_test(
        |path| {
            let local = net::UnixStream::connect(path).unwrap();
            // `std::os::unix::net::UnixStream`s are blocking by default, so make sure
            // it is in non-blocking mode before wrapping in a Mio equivalent.
            local.set_nonblocking(true).unwrap();
            Ok(UnixStream::from_std(local))
        },
        "unix_stream_from_std",
    )
}

#[test]
fn unix_stream_pair() {
    let (mut poll, mut events) = init_with_poll();

    let (mut s1, mut s2) = UnixStream::pair().unwrap();
    poll.registry()
        .register(&mut s1, TOKEN_1, Interest::READABLE | Interest::WRITABLE)
        .unwrap();
    poll.registry()
        .register(&mut s2, TOKEN_2, Interest::READABLE | Interest::WRITABLE)
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );

    let mut buf = [0; DEFAULT_BUF_SIZE];
    assert_would_block(s1.read(&mut buf));

    checked_write!(s1.write(&DATA1));
    s1.flush().unwrap();

    expect_read!(s2.read(&mut buf), DATA1);
    assert_would_block(s2.read(&mut buf));

    checked_write!(s2.write(&DATA2));
    s2.flush().unwrap();

    expect_read!(s1.read(&mut buf), DATA2);
    assert_would_block(s2.read(&mut buf));
}

#[test]
fn unix_stream_peer_addr() {
    init();
    let (handle, expected_addr) = new_echo_listener(1, "unix_stream_peer_addr");
    let expected_path = expected_addr.as_pathname().expect("failed to get pathname");

    let stream = UnixStream::connect(expected_path).unwrap();

    assert_eq!(
        stream.peer_addr().unwrap().as_pathname().unwrap(),
        expected_path
    );
    assert!(stream.local_addr().unwrap().as_pathname().is_none());

    // Close the connection to allow the remote to shutdown
    drop(stream);
    handle.join().unwrap();
}

#[test]
fn unix_stream_shutdown_read() {
    let (mut poll, mut events) = init_with_poll();
    let (handle, remote_addr) = new_echo_listener(1, "unix_stream_shutdown_read");
    let path = remote_addr.as_pathname().expect("failed to get pathname");

    let mut stream = UnixStream::connect(path).unwrap();
    poll.registry()
        .register(
            &mut stream,
            TOKEN_1,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );

    checked_write!(stream.write(&DATA1));
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::READABLE)],
    );

    stream.shutdown(Shutdown::Read).unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Readiness::READ_CLOSED)],
    );

    // Shutting down the reading side is different on each platform. For example
    // on Linux based systems we can still read.
    #[cfg(any(
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "ios",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        let mut buf = [0; DEFAULT_BUF_SIZE];
        expect_read!(stream.read(&mut buf), &[]);
    }

    // Close the connection to allow the remote to shutdown
    drop(stream);
    handle.join().unwrap();
}

#[test]
fn unix_stream_shutdown_write() {
    let (mut poll, mut events) = init_with_poll();
    let (handle, remote_addr) = new_echo_listener(1, "unix_stream_shutdown_write");
    let path = remote_addr.as_pathname().expect("failed to get pathname");

    let mut stream = UnixStream::connect(path).unwrap();
    poll.registry()
        .register(
            &mut stream,
            TOKEN_1,
            Interest::WRITABLE.add(Interest::READABLE),
        )
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );

    checked_write!(stream.write(&DATA1));
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::READABLE)],
    );

    stream.shutdown(Shutdown::Write).unwrap();

    #[cfg(any(
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "ios",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Readiness::WRITE_CLOSED)],
    );

    let err = stream.write(DATA2).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);

    // Read should be ok
    let mut buf = [0; DEFAULT_BUF_SIZE];
    expect_read!(stream.read(&mut buf), DATA1);

    // Close the connection to allow the remote to shutdown
    drop(stream);
    handle.join().unwrap();
}

#[test]
fn unix_stream_shutdown_both() {
    let (mut poll, mut events) = init_with_poll();
    let (handle, remote_addr) = new_echo_listener(1, "unix_stream_shutdown_both");
    let path = remote_addr.as_pathname().expect("failed to get pathname");

    let mut stream = UnixStream::connect(path).unwrap();
    poll.registry()
        .register(
            &mut stream,
            TOKEN_1,
            Interest::WRITABLE.add(Interest::READABLE),
        )
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );

    checked_write!(stream.write(&DATA1));
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::READABLE)],
    );

    stream.shutdown(Shutdown::Both).unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Readiness::WRITE_CLOSED)],
    );

    // Shutting down the reading side is different on each platform. For example
    // on Linux based systems we can still read.
    #[cfg(any(
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "ios",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        let mut buf = [0; DEFAULT_BUF_SIZE];
        expect_read!(stream.read(&mut buf), &[]);
    }

    let err = stream.write(DATA2).unwrap_err();
    #[cfg(unix)]
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    #[cfg(window)]
    assert_eq!(err.kind(), io::ErrorKind::ConnectionAbroted);

    // Close the connection to allow the remote to shutdown
    drop(stream);
    handle.join().unwrap();
}

#[test]
fn unix_stream_shutdown_listener_write() {
    let (mut poll, mut events) = init_with_poll();
    let barrier = Arc::new(Barrier::new(2));
    let (handle, remote_addr) =
        new_noop_listener(1, barrier.clone(), "unix_stream_shutdown_listener_write");
    let path = remote_addr.as_pathname().expect("failed to get pathname");

    let mut stream = UnixStream::connect(path).unwrap();
    poll.registry()
        .register(
            &mut stream,
            TOKEN_1,
            Interest::READABLE.add(Interest::WRITABLE),
        )
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );

    barrier.wait();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Readiness::READ_CLOSED)],
    );

    barrier.wait();
    handle.join().unwrap();
}

#[test]
fn unix_stream_register() {
    let (mut poll, mut events) = init_with_poll();
    let (handle, remote_addr) = new_echo_listener(1, "unix_stream_register");
    let path = remote_addr.as_pathname().expect("failed to get pathname");

    let mut stream = UnixStream::connect(path).unwrap();
    poll.registry()
        .register(&mut stream, TOKEN_1, Interest::READABLE)
        .unwrap();
    expect_no_events(&mut poll, &mut events);

    // Close the connection to allow the remote to shutdown
    drop(stream);
    handle.join().unwrap();
}

#[test]
fn unix_stream_reregister() {
    let (mut poll, mut events) = init_with_poll();
    let (handle, remote_addr) = new_echo_listener(1, "unix_stream_reregister");
    let path = remote_addr.as_pathname().expect("failed to get pathname");

    let mut stream = UnixStream::connect(path).unwrap();
    poll.registry()
        .register(&mut stream, TOKEN_1, Interest::READABLE)
        .unwrap();
    poll.registry()
        .reregister(&mut stream, TOKEN_1, Interest::WRITABLE)
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );

    // Close the connection to allow the remote to shutdown
    drop(stream);
    handle.join().unwrap();
}

#[test]
fn unix_stream_deregister() {
    let (mut poll, mut events) = init_with_poll();
    let (handle, remote_addr) = new_echo_listener(1, "unix_stream_deregister");
    let path = remote_addr.as_pathname().expect("failed to get pathname");

    let mut stream = UnixStream::connect(path).unwrap();
    poll.registry()
        .register(&mut stream, TOKEN_1, Interest::WRITABLE)
        .unwrap();
    poll.registry().deregister(&mut stream).unwrap();
    expect_no_events(&mut poll, &mut events);

    // Close the connection to allow the remote to shutdown
    drop(stream);
    handle.join().unwrap();
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

    let (handle, remote_addr) = new_echo_listener(1, "unix_stream_event_hint");
    let path = remote_addr.as_pathname().expect("failed to get pathname");
    let mut stream = UnixStream::connect(path).unwrap();

    poll.registry()
        .register(
            &mut stream,
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

    let n = stream.write(DATA1).unwrap();
    assert_eq!(n, DATA1.len());

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
    let n = stream.read(&mut buf).unwrap();
    assert_eq!(n, DATA1.len());
    assert_eq!(&buf[..n], &*DATA1);

    drop(stream);
    handle.join().expect("failed to join thread");
}

fn smoke_test<F>(connect_stream: F, test_name: &'static str)
where
    F: FnOnce(&Path) -> io::Result<UnixStream>,
{
    let (mut poll, mut events) = init_with_poll();
    let (handle, remote_addr) = new_echo_listener(1, test_name);
    let path = remote_addr.as_pathname().expect("failed to get pathname");

    let mut stream = connect_stream(path).unwrap();

    assert_socket_non_blocking(&stream);
    assert_socket_close_on_exec(&stream);

    poll.registry()
        .register(
            &mut stream,
            TOKEN_1,
            Interest::WRITABLE.add(Interest::READABLE),
        )
        .unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::WRITABLE)],
    );

    let mut buf = [0; DEFAULT_BUF_SIZE];
    assert_would_block(stream.read(&mut buf));

    checked_write!(stream.write(&DATA1));
    stream.flush().unwrap();
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::READABLE)],
    );

    expect_read!(stream.read(&mut buf), DATA1);

    assert!(stream.take_error().unwrap().is_none());

    let bufs = [IoSlice::new(&DATA1), IoSlice::new(&DATA2)];
    let wrote = stream.write_vectored(&bufs).unwrap();
    assert_eq!(wrote, DATA1_LEN + DATA2_LEN);
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(TOKEN_1, Interest::READABLE)],
    );

    let mut buf1 = [1; DATA1_LEN];
    let mut buf2 = [2; DATA2_LEN + 1];
    let mut bufs = [IoSliceMut::new(&mut buf1), IoSliceMut::new(&mut buf2)];
    let read = stream.read_vectored(&mut bufs).unwrap();
    assert_eq!(read, DATA1_LEN + DATA2_LEN);
    assert_eq!(&buf1, DATA1);
    assert_eq!(&buf2[..DATA2.len()], DATA2);

    // Last byte should be unchanged
    assert_eq!(buf2[DATA2.len()], 2);

    // Close the connection to allow the remote to shutdown
    drop(stream);
    handle.join().unwrap();
}

fn new_echo_listener(
    connections: usize,
    test_name: &'static str,
) -> (thread::JoinHandle<()>, net::SocketAddr) {
    let (addr_sender, addr_receiver) = channel();
    let handle = thread::spawn(move || {
        let path = temp_file(test_name);
        let listener = net::UnixListener::bind(path).unwrap();
        let local_addr = listener.local_addr().unwrap();
        addr_sender.send(local_addr).unwrap();

        for _ in 0..connections {
            let (mut stream, _) = listener.accept().unwrap();

            // On Linux based system it will cause a connection reset
            // error when the reading side of the peer connection is
            // shutdown, we don't consider it an actual here.
            let (mut read, mut written) = (0, 0);
            let mut buf = [0; DEFAULT_BUF_SIZE];
            loop {
                let n = match stream.read(&mut buf) {
                    Ok(amount) => {
                        read += amount;
                        amount
                    }
                    Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(ref err) if err.kind() == io::ErrorKind::ConnectionReset => break,
                    Err(err) => panic!("{}", err),
                };
                if n == 0 {
                    break;
                }
                match stream.write(&buf[..n]) {
                    Ok(amount) => written += amount,
                    Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(ref err) if err.kind() == io::ErrorKind::BrokenPipe => break,
                    Err(err) => panic!("{}", err),
                };
            }
            assert_eq!(read, written, "unequal reads and writes");
        }
    });
    (handle, addr_receiver.recv().unwrap())
}

fn new_noop_listener(
    connections: usize,
    barrier: Arc<Barrier>,
    test_name: &'static str,
) -> (thread::JoinHandle<()>, net::SocketAddr) {
    let (sender, receiver) = channel();
    let handle = thread::spawn(move || {
        let path = temp_file(test_name);
        let listener = net::UnixListener::bind(path).unwrap();
        let local_addr = listener.local_addr().unwrap();
        sender.send(local_addr).unwrap();

        for _ in 0..connections {
            let (stream, _) = listener.accept().unwrap();
            barrier.wait();
            stream.shutdown(Shutdown::Write).unwrap();
            barrier.wait();
            drop(stream);
        }
    });
    (handle, receiver.recv().unwrap())
}
