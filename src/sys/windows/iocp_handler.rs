use std::{
    sync::{Arc, Mutex},
    time::Duration,
    io,
    fmt
};

use winapi::shared::winerror;
use miow::{
    Overlapped,
    iocp::{
        CompletionPort,
        CompletionStatus
    }
};

use slab::Slab;

use crate::{
    Token,
    sys::windows::{
        Event,
        afd,
        selector::AfdCompletionPortEventHandler,
    },
};

#[cfg(feature = "os-util")]
use crate::sys::windows::selector::RawHandleCompletionHandler;

pub trait IocpHandler: fmt::Debug + Send + Sync + 'static {
    fn handle_completion(&mut self, status: &CompletionStatus) -> Option<Event>;
    fn on_poll_finished(&mut self) { }
}

#[derive(Debug)]
pub(crate) enum RegisteredHandler {
    AfdHandler(AfdCompletionPortEventHandler),
    WakerHandler(WakerHandler),
    #[cfg(feature = "os-util")]
    RawHandleHandler(RawHandleCompletionHandler)
}

impl From<AfdCompletionPortEventHandler> for RegisteredHandler {
    fn from(h: AfdCompletionPortEventHandler) -> Self {
        RegisteredHandler::AfdHandler(h)
    }
}

impl From<WakerHandler> for RegisteredHandler {
    fn from(h: WakerHandler) -> Self {
        RegisteredHandler::WakerHandler(h)
    }
}

#[cfg(feature = "os-util")]
impl From<RawHandleCompletionHandler> for RegisteredHandler {
    fn from(h: RawHandleCompletionHandler) -> Self {
        RegisteredHandler::RawHandleHandler(h)
    }
}

impl IocpHandler for RegisteredHandler {
    fn handle_completion(&mut self, status: &CompletionStatus) -> Option<Event> {
        match self {
            RegisteredHandler::AfdHandler(handler) => handler.handle_completion(status),
            RegisteredHandler::WakerHandler(handler) => handler.handle_completion(status),
            #[cfg(feature = "os-util")]
            RegisteredHandler::RawHandleHandler(handler) => handler.handle_completion(status),
        }
    }

    fn on_poll_finished(&mut self) {
        match self {
            RegisteredHandler::AfdHandler(handler) => handler.on_poll_finished(),
            RegisteredHandler::WakerHandler(handler) => handler.on_poll_finished(),
            #[cfg(feature = "os-util")]
            RegisteredHandler::RawHandleHandler(handler) => handler.on_poll_finished(),
        }
    }
}

#[derive(Debug)]
pub struct IocpWaker {
    token: usize,
    iocp_registry: Arc<IocpHandlerRegistry>,
}

#[derive(Debug)]
pub(crate) struct WakerHandler {
    external_token: Token,
}

impl IocpHandler for WakerHandler {
    fn handle_completion(&mut self, _status: &CompletionStatus) -> Option<Event> {
        Some(Event {
            flags: afd::POLL_RECEIVE,
            data: self.external_token.0 as u64
        })
    }
}

impl IocpWaker {
    pub fn post(&self, bytes: u32, overlapped: *mut Overlapped) -> io::Result<()> {
        self.iocp_registry.cp.post(CompletionStatus::new(bytes, self.token, overlapped))
    }
}

#[derive(Debug)]
pub struct IocpHandlerRegistry {
    cp: CompletionPort,
    handlers: Mutex<Slab<RegisteredHandler>>,
}

impl IocpHandlerRegistry {
    pub fn new() -> io::Result<Self> {
        CompletionPort::new(0).map(|cp|
            Self {
                cp,
                handlers: Mutex::new(Slab::new())
            })
    }

    pub fn register_waker(self: Arc<Self>, token: Token) -> IocpWaker {
        let handler = WakerHandler {
            external_token: token
        };
        let slab_token = self.handlers.lock().unwrap()
            .insert(handler.into());
        IocpWaker {
            token: slab_token,
            iocp_registry: self
        }
    }

    pub fn handle_pending_events(&self,
                                 statuses: &mut [CompletionStatus],
                                 mut events: Option<&mut Vec<Event>>,
                                 timeout: Option<Duration>) -> io::Result<usize> {
        let result = match self.cp.get_many(statuses, timeout) {
            Ok(iocp_events) => {
                let mut num_events = 0;
                let mut handlers = self.handlers.lock().unwrap();
                for status in iocp_events {
                    let key = status.token();
                    if let Some(handler) = handlers.get_mut(key) {
                        if let Some(event) = handler.handle_completion(status) {
                            if let Some(events) = &mut events {
                                events.push(event);
                            }
                            num_events += 1;
                        }
                    }
                }

                Ok(num_events)
            },

            Err(ref e) if e.raw_os_error() == Some(winerror::WAIT_TIMEOUT as i32) => Ok(0),

            Err(e) => Err(e)
        };

        for (_, handler) in self.handlers.lock().unwrap().iter_mut() {
            handler.on_poll_finished();
        }

        result
    }
}

cfg_any_os_util! {
    use std::os::windows::io::AsRawHandle;

    impl IocpHandlerRegistry {
        pub(crate) fn register_handle<T>(&self, handle: &T, handler: RegisteredHandler) -> io::Result<()>
            where T: AsRawHandle + ?Sized {
            let token = self.handlers.lock().unwrap().insert(handler);
            self.cp.add_handle(token, handle)
        }
    }
}
