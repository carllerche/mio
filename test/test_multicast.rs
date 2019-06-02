// TODO: This doesn't pass on android 64bit CI...
// Figure out why!
#![cfg(not(target_os = "android"))]

use crate::localhost;
use bytes::{BufMut, Bytes, BytesMut};
use mio::net::UdpSocket;
use mio::{Events, Interests, Poll, PollOpt, Ready, Registry, Token};
use std::net::IpAddr;
use std::str;

const LISTENER: Token = Token(0);
const SENDER: Token = Token(1);

pub struct UdpHandler {
    tx: UdpSocket,
    rx: UdpSocket,
    msg: &'static str,
    buf: Bytes,
    rx_buf: BytesMut,
    localhost: IpAddr,
    shutdown: bool,
}

impl UdpHandler {
    fn new(tx: UdpSocket, rx: UdpSocket, msg: &'static str) -> UdpHandler {
        let sock = UdpSocket::bind(&"127.0.0.1:12345".parse().unwrap()).unwrap();
        UdpHandler {
            tx,
            rx,
            msg,
            buf: Bytes::from_static(msg.as_bytes()),
            rx_buf: BytesMut::with_capacity(1024),
            localhost: sock.local_addr().unwrap().ip(),
            shutdown: false,
        }
    }

    fn handle_read(&mut self, _: &Registry, token: Token, _: Ready) {
        if let LISTENER = token {
            debug!("We are receiving a datagram now...");
            match unsafe { self.rx.recv_from(self.rx_buf.bytes_mut()) } {
                Ok((cnt, addr)) => {
                    unsafe {
                        BufMut::advance_mut(&mut self.rx_buf, cnt);
                    }
                    assert_eq!(addr.ip(), self.localhost);
                }
                res => panic!("unexpected result: {:?}", res),
            }
            assert!(str::from_utf8(self.rx_buf.as_ref()).unwrap() == self.msg);
            self.shutdown = true;
        }
    }

    fn handle_write(&mut self, _: &Registry, token: Token, _: Ready) {
        if let SENDER = token {
            let addr = self.rx.local_addr().unwrap();
            let cnt = self.tx.send_to(self.buf.as_ref(), &addr).unwrap();
            self.buf.advance(cnt);
        }
    }
}

#[test]
pub fn test_multicast() {
    drop(env_logger::try_init());
    debug!("Starting TEST_UDP_CONNECTIONLESS");
    let mut poll = Poll::new().unwrap();

    let addr = localhost();
    let any = "0.0.0.0:0".parse().unwrap();

    let tx = UdpSocket::bind(&any).unwrap();
    let rx = UdpSocket::bind(&addr).unwrap();

    info!("Joining group 227.1.1.100");
    let any = "0.0.0.0".parse().unwrap();
    rx.join_multicast_v4("227.1.1.100".parse().unwrap(), any)
        .unwrap();

    info!("Joining group 227.1.1.101");
    rx.join_multicast_v4("227.1.1.101".parse().unwrap(), any)
        .unwrap();

    info!("Registering SENDER");
    poll.registry()
        .register(&tx, SENDER, Interests::writable(), PollOpt::edge())
        .unwrap();

    info!("Registering LISTENER");
    poll.registry()
        .register(&rx, LISTENER, Interests::readable(), PollOpt::edge())
        .unwrap();

    let mut events = Events::with_capacity(1024);

    let mut handler = UdpHandler::new(tx, rx, "hello world");

    info!("Starting event loop to test with...");

    while !handler.shutdown {
        poll.poll(&mut events, None).unwrap();

        for event in &events {
            if event.readiness().is_readable() {
                handler.handle_read(poll.registry(), event.token(), event.readiness());
            }

            if event.readiness().is_writable() {
                handler.handle_write(poll.registry(), event.token(), event.readiness());
            }
        }
    }
}
