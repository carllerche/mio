use bytes::{Buf, ByteBuf, MutByteBuf, SliceBuf};
use mio::deprecated::unix::*;
use mio::deprecated::{EventLoop, Handler};
use mio::*;
use slab::Slab;
use std::io;
use std::path::PathBuf;
use tempdir::TempDir;
use {TryRead, TryWrite};

const SERVER: Token = Token(10_000_000);
const CLIENT: Token = Token(10_000_001);

struct EchoConn {
    sock: UnixStream,
    buf: Option<ByteBuf>,
    mut_buf: Option<MutByteBuf>,
    token: Option<Token>,
    interests: Option<Interests>,
}

impl EchoConn {
    fn new(sock: UnixStream) -> EchoConn {
        EchoConn {
            sock: sock,
            buf: None,
            mut_buf: Some(ByteBuf::mut_with_capacity(2048)),
            token: None,
            interests: None,
        }
    }

    fn writable(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        let mut buf = self.buf.take().unwrap();

        match self.sock.try_write_buf(&mut buf) {
            Ok(None) => {
                debug!("client flushing buf; WOULDBLOCK");

                self.buf = Some(buf);
                self.interests = match self.interests {
                    None => Some(Interests::writable()),
                    Some(i) => Some(i | Interests::writable()),
                };
            }
            Ok(Some(r)) => {
                debug!("CONN : we wrote {} bytes!", r);

                self.mut_buf = Some(buf.flip());
                self.interests = match self.interests {
                    None => Some(Interests::readable()),
                    Some(i) => Some((i | Interests::readable()) - Interests::writable()),
                };
            }
            Err(e) => debug!("not implemented; client err={:?}", e),
        }

        assert!(
            self.interests.unwrap().is_readable() || self.interests.unwrap().is_writable(),
            "actual={:?}",
            self.interests
        );
        event_loop.reregister(
            &self.sock,
            self.token.unwrap(),
            self.interests.unwrap(),
            PollOpt::edge() | PollOpt::oneshot(),
        )
    }

    fn readable(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        let mut buf = self.mut_buf.take().unwrap();

        match self.sock.try_read_buf(&mut buf) {
            Ok(None) => {
                debug!("CONN : spurious read wakeup");
                self.mut_buf = Some(buf);
            }
            Ok(Some(r)) => {
                debug!("CONN : we read {} bytes!", r);

                // prepare to provide this to writable
                self.buf = Some(buf.flip());

                self.interests = match self.interests {
                    None => Some(Interests::writable()),
                    Some(i) => Some((i | Interests::writable()) - Interests::readable()),
                };
            }
            Err(e) => {
                debug!("not implemented; client err={:?}", e);
                if let Some(x) = self.interests.as_mut() {
                    *x -= Interests::readable();
                }
            }
        };

        assert!(
            self.interests.unwrap().is_readable() || self.interests.unwrap().is_writable(),
            "actual={:?}",
            self.interests
        );
        event_loop.reregister(
            &self.sock,
            self.token.unwrap(),
            self.interests.unwrap(),
            PollOpt::edge() | PollOpt::oneshot(),
        )
    }
}

struct EchoServer {
    sock: UnixListener,
    conns: Slab<EchoConn>,
}

impl EchoServer {
    fn accept(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        debug!("server accepting socket");

        let sock = self.sock.accept().unwrap();
        let conn = EchoConn::new(sock);
        let tok = self.conns.insert(conn);

        // Register the connection
        self.conns[tok].token = Some(Token(tok));
        event_loop
            .register(
                &self.conns[tok].sock,
                Token(tok),
                Interests::readable(),
                PollOpt::edge() | PollOpt::oneshot(),
            )
            .expect("could not register socket with event loop");

        Ok(())
    }

    fn conn_readable(&mut self, event_loop: &mut EventLoop<Echo>, tok: Token) -> io::Result<()> {
        debug!("server conn readable; tok={:?}", tok);
        self.conn(tok).readable(event_loop)
    }

    fn conn_writable(&mut self, event_loop: &mut EventLoop<Echo>, tok: Token) -> io::Result<()> {
        debug!("server conn writable; tok={:?}", tok);
        self.conn(tok).writable(event_loop)
    }

    fn conn<'a>(&'a mut self, tok: Token) -> &'a mut EchoConn {
        &mut self.conns[tok.into()]
    }
}

struct EchoClient {
    sock: UnixStream,
    msgs: Vec<&'static str>,
    tx: SliceBuf<'static>,
    rx: SliceBuf<'static>,
    mut_buf: Option<MutByteBuf>,
    token: Token,
    interests: Option<Interests>,
}

// Sends a message and expects to receive the same exact message, one at a time
impl EchoClient {
    fn new(sock: UnixStream, tok: Token, mut msgs: Vec<&'static str>) -> EchoClient {
        let curr = msgs.remove(0);

        EchoClient {
            sock: sock,
            msgs: msgs,
            tx: SliceBuf::wrap(curr.as_bytes()),
            rx: SliceBuf::wrap(curr.as_bytes()),
            mut_buf: Some(ByteBuf::mut_with_capacity(2048)),
            token: tok,
            interests: None,
        }
    }

    fn readable(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        debug!("client socket readable");

        let mut buf = self.mut_buf.take().unwrap();

        match self.sock.try_read_buf(&mut buf) {
            Ok(None) => {
                debug!("CLIENT : spurious read wakeup");
                self.mut_buf = Some(buf);
            }
            Ok(Some(r)) => {
                debug!("CLIENT : We read {} bytes!", r);

                // prepare for reading
                let mut buf = buf.flip();

                debug!(
                    "CLIENT : buf = {:?} -- rx = {:?}",
                    buf.bytes(),
                    self.rx.bytes()
                );
                while buf.has_remaining() {
                    let actual = buf.read_byte().unwrap();
                    let expect = self.rx.read_byte().unwrap();

                    assert!(actual == expect, "actual={}; expect={}", actual, expect);
                }

                self.mut_buf = Some(buf.flip());

                if let Some(x) = self.interests.as_mut() {
                    *x -= Interests::readable();
                }

                if !self.rx.has_remaining() {
                    self.next_msg(event_loop).unwrap();
                }
            }
            Err(e) => {
                panic!("not implemented; client err={:?}", e);
            }
        };

        if let Some(x) = self.interests {
            event_loop.reregister(
                &self.sock,
                self.token,
                x,
                PollOpt::edge() | PollOpt::oneshot(),
            )?;
        }

        Ok(())
    }

    fn writable(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        debug!("client socket writable");

        match self.sock.try_write_buf(&mut self.tx) {
            Ok(None) => {
                debug!("client flushing buf; WOULDBLOCK");
                self.interests = match self.interests {
                    None => Some(Interests::writable()),
                    Some(i) => Some(i | Interests::writable()),
                };
            }
            Ok(Some(r)) => {
                debug!("CLIENT : we wrote {} bytes!", r);
                self.interests = match self.interests {
                    None => Some(Interests::readable()),
                    Some(i) => Some((i | Interests::readable()) - Interests::writable()),
                };
            }
            Err(e) => debug!("not implemented; client err={:?}", e),
        }

        assert!(
            self.interests.unwrap().is_readable() || self.interests.unwrap().is_writable(),
            "actual={:?}",
            self.interests
        );
        event_loop.reregister(
            &self.sock,
            self.token,
            self.interests.unwrap(),
            PollOpt::edge() | PollOpt::oneshot(),
        )
    }

    fn next_msg(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        if self.msgs.is_empty() {
            event_loop.shutdown();
            return Ok(());
        }

        let curr = self.msgs.remove(0);

        debug!("client prepping next message");
        self.tx = SliceBuf::wrap(curr.as_bytes());
        self.rx = SliceBuf::wrap(curr.as_bytes());

        self.interests = match self.interests {
            None => Some(Interests::writable()),
            Some(i) => Some(i | Interests::writable()),
        };
        assert!(
            self.interests.unwrap().is_readable() || self.interests.unwrap().is_writable(),
            "actual={:?}",
            self.interests
        );
        event_loop.reregister(
            &self.sock,
            self.token,
            self.interests.unwrap(),
            PollOpt::edge() | PollOpt::oneshot(),
        )
    }
}

struct Echo {
    server: EchoServer,
    client: EchoClient,
}

impl Echo {
    fn new(srv: UnixListener, client: UnixStream, msgs: Vec<&'static str>) -> Echo {
        Echo {
            server: EchoServer {
                sock: srv,
                conns: Slab::with_capacity(128),
            },
            client: EchoClient::new(client, CLIENT, msgs),
        }
    }
}

impl Handler for Echo {
    type Timeout = usize;
    type Message = ();

    fn ready(&mut self, event_loop: &mut EventLoop<Echo>, token: Token, events: Ready) {
        if events.is_readable() {
            match token {
                SERVER => self.server.accept(event_loop).unwrap(),
                CLIENT => self.client.readable(event_loop).unwrap(),
                i => self.server.conn_readable(event_loop, i).unwrap(),
            };
        }

        if events.is_writable() {
            match token {
                SERVER => panic!("received writable for token 0"),
                CLIENT => self.client.writable(event_loop).unwrap(),
                _ => self.server.conn_writable(event_loop, token).unwrap(),
            };
        }
    }
}

#[test]
pub fn test_unix_echo_server() {
    debug!("Starting TEST_UNIX_ECHO_SERVER");
    let mut event_loop = EventLoop::new().unwrap();

    let tmp_dir = TempDir::new("mio").unwrap();
    let addr = tmp_dir.path().join(&PathBuf::from("sock"));

    let srv = UnixListener::bind(&addr).unwrap();

    info!("listen for connections");
    event_loop
        .register(
            &srv,
            SERVER,
            Interests::readable(),
            PollOpt::edge() | PollOpt::oneshot(),
        )
        .unwrap();

    let sock = UnixStream::connect(&addr).unwrap();

    // Connect to the server
    event_loop
        .register(
            &sock,
            CLIENT,
            Interests::writable(),
            PollOpt::edge() | PollOpt::oneshot(),
        )
        .unwrap();

    // Start the event loop
    event_loop
        .run(&mut Echo::new(srv, sock, vec!["foo", "bar"]))
        .unwrap();
}
