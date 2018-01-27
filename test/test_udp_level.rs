use mio::{Events, Poll, PollOpt, Ready, Token};
use mio::event::Event;
use mio::net::UdpSocket;
use {expect_events, sleep_ms};

#[test]
pub fn test_udp_level_triggered() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let events = &mut events;

    // Create the listener
    let tx = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let rx = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();

    poll.register().register(&tx, Token(0), Ready::readable() | Ready::writable(), PollOpt::level()).unwrap();
    poll.register().register(&rx, Token(1), Ready::readable() | Ready::writable(), PollOpt::level()).unwrap();


    for _ in 0..2 {
        expect_events(&mut poll, events, 2, vec![
            Event::new(Ready::writable(), Token(0)),
            Event::new(Ready::writable(), Token(1)),
        ]);
    }

    tx.send_to(b"hello world!", &rx.local_addr().unwrap()).unwrap();

    sleep_ms(250);

    for _ in 0..2 {
        expect_events(&mut poll, events, 2, vec![
            Event::new(Ready::readable() | Ready::writable(), Token(1))
        ]);
    }

    let mut buf = [0; 200];
    while rx.recv_from(&mut buf).is_ok() {}

    for _ in 0..2 {
        expect_events(&mut poll, events, 4, vec![Event::new(Ready::writable(), Token(1))]);
    }

    tx.send_to(b"hello world!", &rx.local_addr().unwrap()).unwrap();
    sleep_ms(250);

    expect_events(&mut poll, events, 10,
                  vec![Event::new(Ready::readable() | Ready::writable(), Token(1))]);

    drop(rx);
}
