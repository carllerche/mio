use mio::*;
use mio::net::*;
use mio::net::tcp::*;
use mio::buf::{ByteBuf, SliceBuf};
use mio::util::Slab;
use super::localhost;
use mio::event as evt;

type TestEventLoop = EventLoop<usize, ()>;

const SERVER: Token = Token(0);
const CLIENT: Token = Token(1);

struct EchoConn {
    sock: TcpSocket,
    buf: ByteBuf,
    token: Token,
    isizeerest: evt::Interest
}

impl EchoConn {
    fn new(sock: TcpSocket) -> EchoConn {
        EchoConn {
            sock: sock,
            buf: ByteBuf::new(2048),
            token: Token(-1),
            isizeerest: evt::HUP
        }
    }

    fn writable(&mut self, event_loop: &mut TestEventLoop) -> MioResult<()> {
        debug!("CON : writing buf = {:?}", self.buf.bytes());

        match self.sock.write(&mut self.buf) {
            Ok(NonBlock::WouldBlock) => {
                debug!("client flushing buf; WOULDBLOCK");
                self.isizeerest.insert(evt::WRITABLE);
            }
            Ok(NonBlock::Ready(r)) => {
                debug!("CONN : we wrote {:?} bytes!", r);

                self.buf.clear();
                self.isizeerest.insert(evt::READABLE);
                self.isizeerest.remove(evt::WRITABLE);
            }
            Err(e) => debug!("not implemented; client err={:?}", e),
        }

        event_loop.reregister(&self.sock, self.token, self.isizeerest, evt::PollOpt::edge())
    }

    fn readable(&mut self, event_loop: &mut TestEventLoop) -> MioResult<()> {
        match self.sock.read(&mut self.buf) {
            Ok(NonBlock::WouldBlock) => {
                panic!("We just got readable, but were unable to read from the socket?");
            }
            Ok(NonBlock::Ready(r)) => {
                debug!("CONN : we read {:?} bytes!", r);
                self.isizeerest.remove(evt::READABLE);
                self.isizeerest.insert(evt::WRITABLE);
            }
            Err(e) => {
                debug!("not implemented; client err={:?}", e);
                self.isizeerest.remove(evt::READABLE);
            }

        };

        // prepare to provide this to writable
        self.buf.flip();

        debug!("CON : read buf = {:?}", self.buf.bytes());

        event_loop.reregister(&self.sock, self.token, self.isizeerest, evt::PollOpt::edge())
    }
}

struct EchoServer {
    sock: TcpAcceptor,
    conns: Slab<EchoConn>
}

impl EchoServer {
    fn accept(&mut self, event_loop: &mut TestEventLoop) -> MioResult<()> {
        debug!("server accepting socket");

        let sock = self.sock.accept().unwrap().unwrap();
        let conn = EchoConn::new(sock,);
        let tok = self.conns.insert(conn)
            .ok().expect("could not add connectiont o slab");

        // Register the connection
        self.conns[tok].token = tok;
        event_loop.register_opt(&self.conns[tok].sock, tok, evt::READABLE, evt::PollOpt::edge())
            .ok().expect("could not register socket with event loop");

        Ok(())
    }

    fn conn_readable(&mut self, event_loop: &mut TestEventLoop, tok: Token) -> MioResult<()> {
        debug!("server conn readable; tok={:?}", tok);
        self.conn(tok).readable(event_loop)
    }

    fn conn_writable(&mut self, event_loop: &mut TestEventLoop, tok: Token) -> MioResult<()> {
        debug!("server conn writable; tok={:?}", tok);
        self.conn(tok).writable(event_loop)
    }

    fn conn<'a>(&'a mut self, tok: Token) -> &'a mut EchoConn {
        &mut self.conns[tok]
    }
}

struct EchoClient {
    sock: TcpSocket,
    msgs: Vec<&'static str>,
    tx: SliceBuf<'static>,
    rx: SliceBuf<'static>,
    buf: ByteBuf,
    token: Token,
    isizeerest: evt::Interest
}


// Sends a message and expects to receive the same exact message, one at a time
impl EchoClient {
    fn new(sock: TcpSocket, tok: Token,  mut msgs: Vec<&'static str>) -> EchoClient {
        let curr = msgs.remove(0);

        EchoClient {
            sock: sock,
            msgs: msgs,
            tx: SliceBuf::wrap(curr.as_bytes()),
            rx: SliceBuf::wrap(curr.as_bytes()),
            buf: ByteBuf::new(2048),
            token: tok,
            isizeerest: evt::Interest::empty()
        }
    }

    fn readable(&mut self, event_loop: &mut TestEventLoop) -> MioResult<()> {
        debug!("client socket readable");

        match self.sock.read(&mut self.buf) {
            Ok(NonBlock::WouldBlock) => {
                panic!("We just got readable, but were unable to read from the socket?");
            }
            Ok(NonBlock::Ready(r)) => {
                debug!("CLIENT : We read {:?} bytes!", r);
            }
            Err(e) => {
                panic!("not implemented; client err={:?}", e);
            }
        };

        // prepare for reading
        self.buf.flip();

        debug!("CLIENT : buf = {:?} -- rx = {:?}", self.buf.bytes(), self.rx.bytes());
        while self.buf.has_remaining() {
            let actual = self.buf.read_byte().unwrap();
            let expect = self.rx.read_byte().unwrap();

            assert!(actual == expect, "actual={:?}; expect={:?}", actual, expect);
        }

        self.buf.clear();

        self.isizeerest.remove(evt::READABLE);

        if !self.rx.has_remaining() {
            self.next_msg(event_loop).unwrap();
        }

        event_loop.reregister(&self.sock, self.token, self.isizeerest, evt::PollOpt::edge())
    }

    fn writable(&mut self, event_loop: &mut TestEventLoop) -> MioResult<()> {
        debug!("client socket writable");

        match self.sock.write(&mut self.tx) {
            Ok(NonBlock::WouldBlock) => {
                debug!("client flushing buf; WOULDBLOCK");
                self.isizeerest.insert(evt::WRITABLE);
            }
            Ok(NonBlock::Ready(r)) => {
                debug!("CLIENT : we wrote {:?} bytes!", r);
                self.isizeerest.insert(evt::READABLE);
                self.isizeerest.remove(evt::WRITABLE);
            }
            Err(e) => debug!("not implemented; client err={:?}", e)
        }

        event_loop.reregister(&self.sock, self.token, self.isizeerest, evt::PollOpt::edge())
    }

    fn next_msg(&mut self, event_loop: &mut TestEventLoop) -> MioResult<()> {
        if self.msgs.is_empty() {
            event_loop.shutdown();
            return Ok(());
        }

        let curr = self.msgs.remove(0);

        debug!("client prepping next message");
        self.tx = SliceBuf::wrap(curr.as_bytes());
        self.rx = SliceBuf::wrap(curr.as_bytes());

        self.isizeerest.insert(evt::WRITABLE);
        event_loop.reregister(&self.sock, self.token, self.isizeerest, evt::PollOpt::edge())
    }
}

struct EchoHandler {
    server: EchoServer,
    client: EchoClient,
}

impl EchoHandler {
    fn new(srv: TcpAcceptor, client: TcpSocket, msgs: Vec<&'static str>) -> EchoHandler {
        EchoHandler {
            server: EchoServer {
                sock: srv,
                conns: Slab::new_starting_at(Token(2), 128)
            },
            client: EchoClient::new(client, CLIENT, msgs)
        }
    }
}

impl Handler<usize, ()> for EchoHandler {
    fn readable(&mut self, event_loop: &mut TestEventLoop, token: Token, hisize: evt::ReadHisize) {
        assert_eq!(hisize, evt::DATAHINT);

        match token {
            SERVER => self.server.accept(event_loop).unwrap(),
            CLIENT => self.client.readable(event_loop).unwrap(),
            i => self.server.conn_readable(event_loop, i).unwrap()
        };
    }

    fn writable(&mut self, event_loop: &mut TestEventLoop, token: Token) {
        match token {
            SERVER => panic!("received writable for token 0"),
            CLIENT => self.client.writable(event_loop).unwrap(),
            _ => self.server.conn_writable(event_loop, token).unwrap()
        };
    }
}

#[test]
pub fn test_echo_server() {
    debug!("Starting TEST_ECHO_SERVER");
    let mut event_loop = EventLoop::new().unwrap();

    let addr = SockAddr::parse(localhost().as_slice())
        .expect("could not parse InetAddr");

    let srv = TcpSocket::v4().unwrap();

    info!("setting re-use addr");
    srv.set_reuseaddr(true).unwrap();

    let srv = srv.bind(&addr).unwrap()
        .listen(256us).unwrap();

    info!("listen for connections");
    event_loop.register_opt(&srv, SERVER, evt::READABLE, evt::PollOpt::edge()).unwrap();

    let sock = TcpSocket::v4().unwrap();

    // Connect to the server
    event_loop.register_opt(&sock, CLIENT, evt::WRITABLE, evt::PollOpt::edge()).unwrap();
    sock.connect(&addr).unwrap();

    // Start the event loop
    event_loop.run(EchoHandler::new(srv, sock, vec!["foo", "bar"]))
        .ok().expect("failed to execute event loop");

}
