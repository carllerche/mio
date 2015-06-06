use token::Token;
use std::{fmt, ops};

#[derive(Copy, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct PollOpt(usize);

impl PollOpt {
    #[inline]
    pub fn edge() -> PollOpt {
        PollOpt(0x020)
    }

    #[inline]
    pub fn empty() -> PollOpt {
        PollOpt(0)
    }

    #[inline]
    pub fn level() -> PollOpt {
        PollOpt(0x040)
    }

    #[inline]
    pub fn oneshot() -> PollOpt {
        PollOpt(0x080)
    }

    #[inline]
    pub fn all() -> PollOpt {
        PollOpt::edge() | PollOpt::level() | PollOpt::oneshot()
    }

    #[inline]
    pub fn is_edge(&self) -> bool {
        self.contains(PollOpt::edge())
    }

    #[inline]
    pub fn is_level(&self) -> bool {
        self.contains(PollOpt::level())
    }

    #[inline]
    pub fn is_oneshot(&self) -> bool {
        self.contains(PollOpt::oneshot())
    }

    #[inline]
    pub fn bits(&self) -> usize {
        self.0
    }

    #[inline]
    pub fn contains(&self, other: PollOpt) -> bool {
        (*self & other) == other
    }

    #[inline]
    pub fn insert(&mut self, other: PollOpt) {
        self.0 |= other.0;
    }

    #[inline]
    pub fn remove(&mut self, other: PollOpt) {
        self.0 &= !other.0;
    }
}

impl ops::BitOr for PollOpt {
    type Output = PollOpt;

    #[inline]
    fn bitor(self, other: PollOpt) -> PollOpt {
        PollOpt(self.bits() | other.bits())
    }
}

impl ops::BitXor for PollOpt {
    type Output = PollOpt;

    #[inline]
    fn bitxor(self, other: PollOpt) -> PollOpt {
        PollOpt(self.bits() ^ other.bits())
    }
}

impl ops::BitAnd for PollOpt {
    type Output = PollOpt;

    #[inline]
    fn bitand(self, other: PollOpt) -> PollOpt {
        PollOpt(self.bits() & other.bits())
    }
}

impl ops::Sub for PollOpt {
    type Output = PollOpt;

    #[inline]
    fn sub(self, other: PollOpt) -> PollOpt {
        PollOpt(self.bits() & !other.bits())
    }
}

impl ops::Not for PollOpt {
    type Output = PollOpt;

    #[inline]
    fn not(self) -> PollOpt {
        PollOpt(!self.bits() & PollOpt::all().bits())
    }
}

impl fmt::Debug for PollOpt {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let mut one = false;
        let flags = [
            (PollOpt::edge(), "Edge-Triggered"),
            (PollOpt::level(), "Level-Triggered"),
            (PollOpt::oneshot(), "OneShot")];

        for &(flag, msg) in flags.iter() {
            if self.contains(flag) {
                if one { try!(write!(fmt, " | ")) }
                try!(write!(fmt, "{}", msg));

                one = true
            }
        }

        Ok(())
    }
}

#[derive(Copy, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct Interest(usize);

impl Interest {
    pub fn none() -> Interest {
        Interest(0)
    }

    #[inline]
    pub fn readable() -> Interest {
        Interest(0x001)
    }

    #[inline]
    pub fn writable() -> Interest {
        Interest(0x002)
    }

    #[inline]
    pub fn error() -> Interest {
        Interest(0x004)
    }

    #[inline]
    pub fn hup() -> Interest {
        Interest(0x008)
    }

    #[inline]
    pub fn hinted() -> Interest {
        Interest(0x010)
    }

    #[inline]
    pub fn all() -> Interest {
        Interest::readable() |
            Interest::writable() |
            Interest::hup() |
            Interest::error()
    }

    #[inline]
    pub fn is_readable(&self) -> bool {
        self.contains(Interest::readable())
    }

    #[inline]
    pub fn is_writable(&self) -> bool {
        self.contains(Interest::writable())
    }

    #[inline]
    pub fn is_error(&self) -> bool {
        self.contains(Interest::error())
    }

    #[inline]
    pub fn is_hup(&self) -> bool {
        self.contains(Interest::hup())
    }

    #[inline]
    pub fn is_hinted(&self) -> bool {
        self.contains(Interest::hinted())
    }

    #[inline]
    pub fn insert(&mut self, other: Interest) {
        self.0 |= other.0;
    }

    #[inline]
    pub fn remove(&mut self, other: Interest) {
        self.0 &= !other.0;
    }

    #[inline]
    pub fn bits(&self) -> usize {
        self.0
    }

    #[inline]
    pub fn contains(&self, other: Interest) -> bool {
        (*self & other) == other
    }
}

impl ops::BitOr for Interest {
    type Output = Interest;

    #[inline]
    fn bitor(self, other: Interest) -> Interest {
        Interest(self.bits() | other.bits())
    }
}

impl ops::BitXor for Interest {
    type Output = Interest;

    #[inline]
    fn bitxor(self, other: Interest) -> Interest {
        Interest(self.bits() ^ other.bits())
    }
}

impl ops::BitAnd for Interest {
    type Output = Interest;

    #[inline]
    fn bitand(self, other: Interest) -> Interest {
        Interest(self.bits() & other.bits())
    }
}

impl ops::Sub for Interest {
    type Output = Interest;

    #[inline]
    fn sub(self, other: Interest) -> Interest {
        Interest(self.bits() & !other.bits())
    }
}

impl ops::Not for Interest {
    type Output = Interest;

    #[inline]
    fn not(self) -> Interest {
        Interest(!self.bits() & Interest::all().bits())
    }
}

impl fmt::Debug for Interest {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let mut one = false;
        let flags = [
            (Interest::readable(), "Readable"),
            (Interest::writable(), "Writable"),
            (Interest::error(),    "Error"),
            (Interest::hup(),      "HupHint"),
            (Interest::hinted(),   "Hinted")];

        for &(flag, msg) in flags.iter() {
            if self.contains(flag) {
                if one { try!(write!(fmt, " | ")) }
                try!(write!(fmt, "{}", msg));

                one = true
            }
        }

        Ok(())
    }
}


#[derive(Copy, Clone, Debug)]
pub struct IoEvent {
    kind: Interest,
    token: Token
}

/// IoEvent represents the raw event that the OS-specific selector
/// returned. An event can represent more than one kind (such as
/// readable or writable) at a time.
///
/// These IoEvent objects are created by the OS-specific concrete
/// Selector when they have events to report.
impl IoEvent {
    /// Create a new IoEvent.
    pub fn new(kind: Interest, token: usize) -> IoEvent {
        IoEvent {
            kind: kind,
            token: Token(token)
        }
    }

    #[inline]
    pub fn token(&self) -> Token {
        self.token
    }

    #[inline]
    pub fn events(&self) -> Interest {
        self.kind
    }
}

