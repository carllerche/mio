use crate::sys::Selector;
use crate::Token;

use std::io;

#[derive(Debug)]
pub struct Waker {
}

impl Waker {
    pub fn new(selector: &Selector, token: Token) -> io::Result<Waker> {
        os_required!();
    }

    pub fn wake(&self) -> io::Result<()> {
        os_required!();
    }

    /// Reset the eventfd object, only need to call this if `wake` fails.
    fn reset(&self) -> io::Result<()> {
        os_required!();
    }
}
