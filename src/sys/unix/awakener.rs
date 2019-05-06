pub use self::pipe::Awakener;

/// Default awakener backed by a pipe
mod pipe {
    use event::Evented;
    use std::io::{Read, Write};
    use sys::unix;
    use {io, PollOpt, Ready, Registry, Token};

    /*
     *
     * ===== Awakener =====
     *
     */

    pub struct Awakener {
        reader: unix::Io,
        writer: unix::Io,
    }

    impl Awakener {
        pub fn new() -> io::Result<Awakener> {
            let (rd, wr) = unix::pipe()?;

            Ok(Awakener {
                reader: rd,
                writer: wr,
            })
        }

        pub fn wakeup(&self) -> io::Result<()> {
            match (&self.writer).write(&[1]) {
                Ok(_) => Ok(()),
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        Ok(())
                    } else {
                        Err(e)
                    }
                }
            }
        }

        pub fn cleanup(&self) {
            let mut buf = [0; 128];

            loop {
                // Consume data until all bytes are purged
                match (&self.reader).read(&mut buf) {
                    Ok(i) if i > 0 => {}
                    _ => return,
                }
            }
        }

        fn reader(&self) -> &unix::Io {
            &self.reader
        }
    }

    impl Evented for Awakener {
        fn register(
            &self,
            registry: &Registry,
            token: Token,
            interest: Ready,
            opts: PollOpt,
        ) -> io::Result<()> {
            self.reader().register(registry, token, interest, opts)
        }

        fn reregister(
            &self,
            registry: &Registry,
            token: Token,
            interest: Ready,
            opts: PollOpt,
        ) -> io::Result<()> {
            self.reader().reregister(registry, token, interest, opts)
        }

        fn deregister(&self, registry: &Registry) -> io::Result<()> {
            self.reader().deregister(registry)
        }
    }
}
