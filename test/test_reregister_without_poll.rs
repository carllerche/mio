use mio::net::{TcpListener, TcpStream};
use mio::*;
use sleep_ms;
use std::time::Duration;

const MS: u64 = 1_000;

#[test]
pub fn test_reregister_different_without_poll() {
    let mut events = Events::with_capacity(1024);
    let mut poll = Poll::new().unwrap();

    // Create the listener
    let l = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();

    // Register the listener with `Poll`
    poll.registry()
        .register(
            &l,
            Token(0),
            Ready::readable(),
            PollOpt::edge() | PollOpt::oneshot(),
        )
        .unwrap();

    let s1 = TcpStream::connect(&l.local_addr().unwrap()).unwrap();
    poll.registry()
        .register(&s1, Token(2), Ready::readable(), PollOpt::edge())
        .unwrap();

    sleep_ms(MS);

    poll.registry()
        .reregister(
            &l,
            Token(0),
            Ready::writable(),
            PollOpt::edge() | PollOpt::oneshot(),
        )
        .unwrap();

    poll.poll(&mut events, Some(Duration::from_millis(MS)))
        .unwrap();
    assert!(events.iter().next().is_none());
}
