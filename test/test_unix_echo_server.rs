use mio::*;
use mio::net::*;
use mio::net::pipe::*;
use mio::buf::{ByteBuf, RingBuf, SliceBuf};
use mio::util::Slab;
use std::io::TempDir;

type TestEventLoop = EventLoop<uint, ()>;

const SERVER: Token = Token(0);
const CLIENT: Token = Token(1);

struct EchoConn {
    sock: UnixSocket,
    readable: bool,
    writable: bool,
    buf: RingBuf
}

impl EchoConn {
    fn new(sock: UnixSocket) -> EchoConn {
        EchoConn {
            sock: sock,
            readable: false,
            writable: false,
            buf: RingBuf::new(1024)
        }
    }

    fn readable(&mut self) -> MioResult<()> {
        self.readable = true;
        self.echo()
    }

    fn writable(&mut self) -> MioResult<()> {
        self.writable = true;
        self.echo()
    }

    fn can_continue(&self) -> bool {
        (self.readable && !self.buf.is_full()) ||
            (self.writable && !self.buf.is_empty())
    }

    fn echo(&mut self) -> MioResult<()> {
        while self.can_continue() {
            try!(self.fill_buf());
            try!(self.flush_buf());
        }

        Ok(())
    }

    fn fill_buf(&mut self) -> MioResult<()> {
        if !self.readable {
            return Ok(());
        }

        debug!("server filling buf");
        self.sock.read(&mut self.buf.writer())
            .map(|res| {
                if res.would_block() {
                    debug!("  WOULDBLOCK");
                    self.readable = false;
                }
            })
    }

    fn flush_buf(&mut self) -> MioResult<()> {
        if !self.writable {
            return Ok(());
        }

        debug!("server flushing buf");
        self.sock.write(&mut self.buf.reader())
            .map(|res| {
                if res.would_block() {
                    debug!("  WOULDBLOCK");
                    self.writable = false;
                }
            })
    }
}

struct EchoServer {
    sock: UnixAcceptor,
    conns: Slab<EchoConn>
}

impl EchoServer {
    fn accept(&mut self, event_loop: &mut TestEventLoop) {
        debug!("server accepting socket");
        let sock = self.sock.accept().unwrap().unwrap();
        let conn = EchoConn::new(sock);
        let tok = self.conns.insert(conn)
            .ok().expect("could not add connectiont o slab");

        // Register the connection
        event_loop.register(&self.conns[tok].sock, tok)
            .ok().expect("could not register socket with event loop");
    }

    fn conn_readable(&mut self, tok: Token) {
        debug!("server conn readable; tok={}", tok);
        self.conn(tok).readable().unwrap();
    }

    fn conn_writable(&mut self, tok: Token) {
        debug!("server conn writable; tok={}", tok);
        self.conn(tok).writable().unwrap();
    }

    fn conn<'a>(&'a mut self, tok: Token) -> &'a mut EchoConn {
        &mut self.conns[tok]
    }
}

struct EchoClient {
    sock: UnixSocket,
    msgs: Vec<&'static str>,
    tx: SliceBuf<'static>,
    rx: SliceBuf<'static>,
    buf: ByteBuf,
    writable: bool
}


// Sends a message and expects to receive the same exact message, one at a time
impl EchoClient {
    fn new(sock: UnixSocket, mut msgs: Vec<&'static str>) -> EchoClient {
        let curr = msgs.remove(0).expect("At least one message is required");

        EchoClient {
            sock: sock,
            msgs: msgs,
            tx: SliceBuf::wrap(curr.as_bytes()),
            rx: SliceBuf::wrap(curr.as_bytes()),
            buf: ByteBuf::new(1024),
            writable: false
        }
    }

    fn readable(&mut self, event_loop: &mut TestEventLoop) {
        debug!("client socket readable");

        loop {
            let res = match self.sock.read(&mut self.buf) {
                Ok(r) => r,
                Err(e) => panic!("not implemented; client err={}", e)
            };

            // prepare for reading
            self.buf.flip();

            while self.buf.has_remaining() {
                let actual = self.buf.read_byte().unwrap();
                let expect = self.rx.read_byte().unwrap();

                assert!(actual == expect);
            }

            self.buf.clear();

            if !self.rx.has_remaining() {
                self.next_msg(event_loop).unwrap();
            }

            // Nothing else to do this round
            if res.would_block() {
                return;
            }
        }
    }

    fn writable(&mut self) {
        debug!("client socket writable");

        self.writable = true;
        self.flush_msg().unwrap();
    }

    fn flush_msg(&mut self) -> MioResult<()> {
        if !self.writable {
            return Ok(());
        }

        self.sock.write(&mut self.tx)
            .map(|res| {
                if res.would_block() {
                    debug!("client flushing buf; WOULDBLOCK");
                    self.writable = false
                } else {
                    debug!("client flushed buf");
                }
            })
    }

    fn next_msg(&mut self, event_loop: &mut TestEventLoop) -> MioResult<()> {
        let curr = match self.msgs.remove(0) {
            Some(msg) => msg,
            None => {
                event_loop.shutdown();
                return Ok(());
            }
        };

        debug!("client prepping next message");
        self.tx = SliceBuf::wrap(curr.as_bytes());
        self.rx = SliceBuf::wrap(curr.as_bytes());

        self.flush_msg()
    }
}

struct EchoHandler {
    server: EchoServer,
    client: EchoClient,
}

impl EchoHandler {
    fn new(srv: UnixAcceptor, client: UnixSocket, msgs: Vec<&'static str>) -> EchoHandler {
        EchoHandler {
            server: EchoServer {
                sock: srv,
                conns: Slab::new_starting_at(Token(2), 128)
            },

            client: EchoClient::new(client, msgs)
        }
    }
}

impl Handler<uint, ()> for EchoHandler {
    fn readable(&mut self, event_loop: &mut TestEventLoop, token: Token, hint: ReadHint) {
        assert_eq!(hint, DATAHINT);

        match token {
            SERVER => self.server.accept(event_loop),
            CLIENT => self.client.readable(event_loop),
            i => self.server.conn_readable(i)
        }
    }

    fn writable(&mut self, _event_loop: &mut TestEventLoop, token: Token) {
        match token {
            SERVER => panic!("received writable for token 0"),
            CLIENT => self.client.writable(),
            i => self.server.conn_writable(i)
        }
    }
}

#[test]
pub fn test_unix_echo_server() {
    let mut event_loop = EventLoop::new().unwrap();

    let tmp_dir = TempDir::new("test_unix_echo_server").unwrap();
    let tmp_sock_path = tmp_dir.path().join(Path::new("sock"));
    let addr = SockAddr::from_path(tmp_sock_path);

    let srv = UnixSocket::stream().unwrap();

    let srv = srv.bind(&addr).unwrap()
        .listen(256u).unwrap();

    info!("listen for connections");
    event_loop.register(&srv, SERVER).unwrap();

    let sock = UnixSocket::stream().unwrap();

    // Connect to the server
    event_loop.connect(&sock, &addr, CLIENT).unwrap();

    // Start the event loop
    event_loop.run(EchoHandler::new(srv, sock, vec!["foo", "bar"]))
        .ok().expect("failed to execute event loop");

}
