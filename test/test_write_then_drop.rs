use std::io::{Write, Read};

use mio::event::Evented;
use mio::net::{TcpListener, TcpStream};
use mio::{Poll, Events, Ready, PollOpt, Token};

#[test]
fn write_then_drop() {
    drop(::env_logger::init());

    let a = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = a.local_addr().unwrap();
    let mut s = TcpStream::connect(&addr).unwrap();

    let mut poll = Poll::new().unwrap();

    a.register(poll.register(),
               Token(1),
               Ready::READABLE,
               PollOpt::EDGE).unwrap();
    s.register(poll.register(),
               Token(3),
               Ready::EMPTY,
               PollOpt::EDGE).unwrap();

    let mut events = Events::with_capacity(1024);
    while events.iter().next().is_none() {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.iter().count(), 1);
    assert_eq!(events.iter().next().unwrap().token(), Token(1));

    let mut s2 = a.accept().unwrap().0;

    s2.register(poll.register(),
                Token(2),
                Ready::WRITABLE,
                PollOpt::EDGE).unwrap();

    let mut events = Events::with_capacity(1024);
    while events.iter().count() == 0 {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.iter().count(), 1);
    assert_eq!(events.iter().next().unwrap().token(), Token(2));

    s2.write(&[1, 2, 3, 4]).unwrap();
    drop(s2);

    s.reregister(poll.register(),
                 Token(3),
                 Ready::READABLE,
                 PollOpt::EDGE).unwrap();
    let mut events = Events::with_capacity(1024);
    while events.iter().count() == 0 {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.iter().count(), 1);
    assert_eq!(events.iter().next().unwrap().token(), Token(3));

    let mut buf = [0; 10];
    assert_eq!(s.read(&mut buf).unwrap(), 4);
    assert_eq!(&buf[0..4], &[1, 2, 3, 4]);
}

#[test]
fn write_then_deregister() {
    drop(::env_logger::init());

    let a = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = a.local_addr().unwrap();
    let mut s = TcpStream::connect(&addr).unwrap();

    let mut poll = Poll::new().unwrap();

    a.register(poll.register(),
               Token(1),
               Ready::READABLE,
               PollOpt::EDGE).unwrap();
    s.register(poll.register(),
               Token(3),
               Ready::EMPTY,
               PollOpt::EDGE).unwrap();

    let mut events = Events::with_capacity(1024);
    while events.iter().count() == 0 {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.iter().count(), 1);
    assert_eq!(events.iter().next().unwrap().token(), Token(1));

    let mut s2 = a.accept().unwrap().0;

    s2.register(poll.register(),
                Token(2),
                Ready::WRITABLE,
                PollOpt::EDGE).unwrap();

    let mut events = Events::with_capacity(1024);
    while events.iter().count() == 0 {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.iter().count(), 1);
    assert_eq!(events.iter().next().unwrap().token(), Token(2));

    s2.write(&[1, 2, 3, 4]).unwrap();
    s2.deregister(poll.register()).unwrap();

    s.reregister(poll.register(),
                 Token(3),
                 Ready::READABLE,
                 PollOpt::EDGE).unwrap();
    let mut events = Events::with_capacity(1024);
    while events.iter().count() == 0 {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.iter().count(), 1);
    assert_eq!(events.iter().next().unwrap().token(), Token(3));

    let mut buf = [0; 10];
    assert_eq!(s.read(&mut buf).unwrap(), 4);
    assert_eq!(&buf[0..4], &[1, 2, 3, 4]);
}
