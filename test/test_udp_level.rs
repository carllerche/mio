use mio::*;
use mio::udp::*;
use {expect_events, sleep_ms};

#[test]
pub fn test_udp_level_triggered() {
    let poll = Poll::new().unwrap();
    let poll = &poll;
    let mut events = Events::with_capacity(1024);
    let events = &mut events;

    // Create the listener
    let tx = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let rx = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();

    poll.register(&tx, Token(0), Ready::all(), PollOpt::level()).unwrap();
    poll.register(&rx, Token(1), Ready::all(), PollOpt::level()).unwrap();


    for _ in 0..2 {
        expect_events(poll, events, 2, vec![
            Event::new(Ready::WRITABLE, Token(0)),
            Event::new(Ready::WRITABLE, Token(1)),
        ]);
    }

    tx.send_to(b"hello world!", &rx.local_addr().unwrap()).unwrap();

    sleep_ms(250);

    for _ in 0..2 {
        expect_events(poll, events, 2, vec![
            Event::new(Ready::READABLE | Ready::WRITABLE, Token(1))
        ]);
    }

    let mut buf = [0; 200];
    while rx.recv_from(&mut buf).unwrap().is_some() {}

    for _ in 0..2 {
        expect_events(poll, events, 4, vec![Event::new(Ready::WRITABLE, Token(1))]);
    }

    tx.send_to(b"hello world!", &rx.local_addr().unwrap()).unwrap();
    sleep_ms(250);

    expect_events(poll, events, 10,
                  vec![Event::new(Ready::READABLE | Ready::WRITABLE, Token(1))]);

    drop(rx);
}
