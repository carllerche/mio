use crate::sys::windows::selector::WAKER_OVERLAPPED;
use crate::sys::windows::Selector;
use crate::sys::windows::iocp_handler::IocpWaker;
use crate::Token;

use std::io;

#[derive(Debug)]
pub struct Waker {
    cp_waker: IocpWaker,
}

impl Waker {
    pub fn new(selector: &Selector, token: Token) -> io::Result<Waker> {
        let cp_registry = selector.clone_port();
        let cp_waker = cp_registry.register_waker(token);
        Ok(Waker { cp_waker })
    }

    pub fn wake(&self) -> io::Result<()> {
        // Keep NULL as Overlapped value to notify waking.
        self.cp_waker.post(0, WAKER_OVERLAPPED)
    }
}
