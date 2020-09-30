use super::afd::{self, Afd, AfdPollInfo};
use super::io_status_block::IoStatusBlock;
use super::Event;
use crate::sys::event::{
    ERROR_FLAGS, READABLE_FLAGS, READ_CLOSED_FLAGS, WRITABLE_FLAGS, WRITE_CLOSED_FLAGS,
};
use crate::sys::Events;
use crate::sys::windows::iocp_handler::{
    IocpHandler, IocpHandlerRegistry
};
use crate::Interest;

use miow::iocp::CompletionStatus;
use std::collections::VecDeque;
use std::marker::PhantomPinned;
use std::os::windows::io::RawSocket;
use std::pin::Pin;
#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{io, ptr};
use winapi::shared::ntdef::NT_SUCCESS;
use winapi::shared::ntdef::{HANDLE, PVOID};
use winapi::shared::ntstatus::STATUS_CANCELLED;
use winapi::shared::winerror::{ERROR_INVALID_HANDLE, ERROR_IO_PENDING};
use winapi::um::minwinbase::OVERLAPPED;

/// Overlapped value to indicate a `Waker` event.
//
// Note: this must be null, `SelectorInner::feed_events` depends on it.
pub const WAKER_OVERLAPPED: *mut miow::Overlapped = ptr::null_mut();

#[derive(Debug)]
struct AfdGroup {
    cp: Arc<IocpHandlerRegistry>,
    afd_group: Mutex<Vec<Arc<Afd>>>,
    completion_handler: AfdCompletionPortEventHandler,
}

impl AfdGroup {
    pub fn new(cp: Arc<IocpHandlerRegistry>, completion_handler: AfdCompletionPortEventHandler) -> AfdGroup {
        AfdGroup {
            afd_group: Mutex::new(Vec::new()),
            cp,
            completion_handler
        }
    }

    pub fn release_unused_afd(&self) {
        let mut afd_group = self.afd_group.lock().unwrap();
        afd_group.retain(|g| Arc::strong_count(&g) > 1);
    }
}

impl Drop for AfdGroup {
    fn drop(&mut self) {
        self.release_unused_afd();
    }
}

cfg_net! {
    const POLL_GROUP__MAX_GROUP_SIZE: usize = 32;

    impl AfdGroup {
        pub fn acquire(&self) -> io::Result<Arc<Afd>> {
            let mut afd_group = self.afd_group.lock().unwrap();
            if afd_group.len() == 0 {
                self._alloc_afd_group(&mut afd_group)?;
            } else {
                // + 1 reference in Vec
                if Arc::strong_count(afd_group.last().unwrap()) >= POLL_GROUP__MAX_GROUP_SIZE + 1 {
                    self._alloc_afd_group(&mut afd_group)?;
                }
            }

            match afd_group.last() {
                Some(arc) => Ok(arc.clone()),
                None => unreachable!(
                    "Cannot acquire afd, {:#?}, afd_group: {:#?}",
                    self, afd_group
                ),
            }
        }

        fn _alloc_afd_group(&self, afd_group: &mut Vec<Arc<Afd>>) -> io::Result<()> {
            let afd = Afd::new(&self.cp, self.completion_handler.clone())?;
            let arc = Arc::new(afd);
            afd_group.push(arc);
            Ok(())
        }
    }
}

#[derive(Debug)]
enum SockPollStatus {
    Idle,
    Pending,
    Cancelled,
}

#[derive(Debug)]
pub struct SockState {
    iosb: IoStatusBlock,
    poll_info: AfdPollInfo,
    afd: Arc<Afd>,

    raw_socket: RawSocket,
    base_socket: RawSocket,

    user_evts: u32,
    pending_evts: u32,

    user_data: u64,

    poll_status: SockPollStatus,
    delete_pending: bool,

    // last raw os error
    error: Option<i32>,

    pinned: PhantomPinned,
}

#[derive(Debug)]
pub(crate) struct AfdCompletionPortEventHandler {
    update_queue: Vec<Pin<Arc<Mutex<SockState>>>>,
    selector_update_queue: Arc<Mutex<VecDeque<Pin<Arc<Mutex<SockState>>>>>>,
}

impl Clone for AfdCompletionPortEventHandler {
    fn clone(&self) -> Self {
        Self {
            update_queue: Vec::new(),
            selector_update_queue: Arc::clone(&self.selector_update_queue)
        }
    }
}

impl IocpHandler for AfdCompletionPortEventHandler {
    fn handle_completion(&mut self, status: &CompletionStatus) -> Option<Event> {
        let sock_state = from_overlapped(status.overlapped());
        let mut sock_guard = sock_state.lock().unwrap();
        let event = sock_guard.feed_event();
        if !sock_guard.is_pending_deletion() {
            self.update_queue.push(sock_state.clone());
        }
        event
    }

    fn on_poll_finished(&mut self) {
        self.selector_update_queue.lock().unwrap().extend(self.update_queue.drain(..));
    }
}

impl SockState {
    fn update(&mut self, self_arc: &Pin<Arc<Mutex<SockState>>>) -> io::Result<()> {
        assert!(!self.delete_pending);

        // make sure to reset previous error before a new update
        self.error = None;

        if let SockPollStatus::Pending = self.poll_status {
            if (self.user_evts & afd::KNOWN_EVENTS & !self.pending_evts) == 0 {
                /* All the events the user is interested in are already being monitored by
                 * the pending poll operation. It might spuriously complete because of an
                 * event that we're no longer interested in; when that happens we'll submit
                 * a new poll operation with the updated event mask. */
            } else {
                /* A poll operation is already pending, but it's not monitoring for all the
                 * events that the user is interested in. Therefore, cancel the pending
                 * poll operation; when we receive it's completion package, a new poll
                 * operation will be submitted with the correct event mask. */
                if let Err(e) = self.cancel() {
                    self.error = e.raw_os_error();
                    return Err(e);
                }
                return Ok(());
            }
        } else if let SockPollStatus::Cancelled = self.poll_status {
            /* The poll operation has already been cancelled, we're still waiting for
             * it to return. For now, there's nothing that needs to be done. */
        } else if let SockPollStatus::Idle = self.poll_status {
            /* No poll operation is pending; start one. */
            self.poll_info.exclusive = 0;
            self.poll_info.number_of_handles = 1;
            *unsafe { self.poll_info.timeout.QuadPart_mut() } = std::i64::MAX;
            self.poll_info.handles[0].handle = self.base_socket as HANDLE;
            self.poll_info.handles[0].status = 0;
            self.poll_info.handles[0].events = self.user_evts | afd::POLL_LOCAL_CLOSE;

            // Increase the ref count as the memory will be used by the kernel.
            let overlapped_ptr = into_overlapped(self_arc.clone());

            let result = unsafe {
                self.afd
                    .poll(&mut self.poll_info, &mut *self.iosb, overlapped_ptr)
            };
            if let Err(e) = result {
                let code = e.raw_os_error().unwrap();
                if code == ERROR_IO_PENDING as i32 {
                    /* Overlapped poll operation in progress; this is expected. */
                } else {
                    // Since the operation failed it means the kernel won't be
                    // using the memory any more.
                    drop(from_overlapped(overlapped_ptr as *mut _));
                    if code == ERROR_INVALID_HANDLE as i32 {
                        /* Socket closed; it'll be dropped. */
                        self.mark_delete();
                        return Ok(());
                    } else {
                        self.error = e.raw_os_error();
                        return Err(e);
                    }
                }
            }

            self.poll_status = SockPollStatus::Pending;
            self.pending_evts = self.user_evts;
        } else {
            unreachable!("Invalid poll status during update, {:#?}", self)
        }

        Ok(())
    }

    fn cancel(&mut self) -> io::Result<()> {
        match self.poll_status {
            SockPollStatus::Pending => {}
            _ => unreachable!("Invalid poll status during cancel, {:#?}", self),
        };
        unsafe {
            self.afd.cancel(&mut *self.iosb)?;
        }
        self.poll_status = SockPollStatus::Cancelled;
        self.pending_evts = 0;
        Ok(())
    }

    // This is the function called from the overlapped using as Arc<Mutex<SockState>>. Watch out for reference counting.
    fn feed_event(&mut self) -> Option<Event> {
        self.poll_status = SockPollStatus::Idle;
        self.pending_evts = 0;

        let mut afd_events = 0;
        // We use the status info in IO_STATUS_BLOCK to determine the socket poll status. It is unsafe to use a pointer of IO_STATUS_BLOCK.
        unsafe {
            if self.delete_pending {
                return None;
            } else if self.iosb.u.Status == STATUS_CANCELLED {
                /* The poll request was cancelled by CancelIoEx. */
            } else if !NT_SUCCESS(self.iosb.u.Status) {
                /* The overlapped request itself failed in an unexpected way. */
                afd_events = afd::POLL_CONNECT_FAIL;
            } else if self.poll_info.number_of_handles < 1 {
                /* This poll operation succeeded but didn't report any socket events. */
            } else if self.poll_info.handles[0].events & afd::POLL_LOCAL_CLOSE != 0 {
                /* The poll operation reported that the socket was closed. */
                self.mark_delete();
                return None;
            } else {
                afd_events = self.poll_info.handles[0].events;
            }
        }

        afd_events &= self.user_evts;

        if afd_events == 0 {
            return None;
        }

        // In mio, we have to simulate Edge-triggered behavior to match API usage.
        // The strategy here is to intercept all read/write from user that could cause WouldBlock usage,
        // then reregister the socket to reset the interests.

        // Reset readable event
        if (afd_events & interests_to_afd_flags(Interest::READABLE)) != 0 {
            self.user_evts &= !(interests_to_afd_flags(Interest::READABLE));
        }
        // Reset writable event
        if (afd_events & interests_to_afd_flags(Interest::WRITABLE)) != 0 {
            self.user_evts &= !interests_to_afd_flags(Interest::WRITABLE);
        }

        Some(Event {
            data: self.user_data,
            flags: afd_events,
        })
    }

    pub fn is_pending_deletion(&self) -> bool {
        self.delete_pending
    }

    pub fn mark_delete(&mut self) {
        if !self.delete_pending {
            if let SockPollStatus::Pending = self.poll_status {
                drop(self.cancel());
            }

            self.delete_pending = true;
        }
    }

    fn has_error(&self) -> bool {
        self.error.is_some()
    }
}

cfg_net! {
    impl SockState {
        fn new(raw_socket: RawSocket, afd: Arc<Afd>) -> io::Result<SockState> {
            Ok(SockState {
                iosb: IoStatusBlock::zeroed(),
                poll_info: AfdPollInfo::zeroed(),
                afd,
                raw_socket,
                base_socket: get_base_socket(raw_socket)?,
                user_evts: 0,
                pending_evts: 0,
                user_data: 0,
                poll_status: SockPollStatus::Idle,
                delete_pending: false,
                error: None,
                pinned: PhantomPinned,
            })
        }

        /// True if need to be added on update queue, false otherwise.
        fn set_event(&mut self, ev: Event) -> bool {
            /* afd::POLL_CONNECT_FAIL and afd::POLL_ABORT are always reported, even when not requested by the caller. */
            let events = ev.flags | afd::POLL_CONNECT_FAIL | afd::POLL_ABORT;

            self.user_evts = events;
            self.user_data = ev.data;

            (events & !self.pending_evts) != 0
        }
    }
}

impl Drop for SockState {
    fn drop(&mut self) {
        self.mark_delete();
    }
}

/// Converts the pointer to a `SockState` into a raw pointer.
/// To revert see `from_overlapped`.
fn into_overlapped(sock_state: Pin<Arc<Mutex<SockState>>>) -> PVOID {
    let overlapped_ptr: *const Mutex<SockState> =
        unsafe { Arc::into_raw(Pin::into_inner_unchecked(sock_state)) };
    overlapped_ptr as *mut _
}

/// Convert a raw overlapped pointer into a reference to `SockState`.
/// Reverts `into_overlapped`.
fn from_overlapped(ptr: *mut OVERLAPPED) -> Pin<Arc<Mutex<SockState>>> {
    let sock_ptr: *const Mutex<SockState> = ptr as *const _;
    unsafe { Pin::new_unchecked(Arc::from_raw(sock_ptr)) }
}

/// Each Selector has a globally unique(ish) ID associated with it. This ID
/// gets tracked by `TcpStream`, `TcpListener`, etc... when they are first
/// registered with the `Selector`. If a type that is previously associated with
/// a `Selector` attempts to register itself with a different `Selector`, the
/// operation will return with an error. This matches windows behavior.
#[cfg(debug_assertions)]
static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

/// Windows implementation of `sys::Selector`
///
/// Edge-triggered event notification is simulated by resetting internal event flag of each socket state `SockState`
/// and setting all events back by intercepting all requests that could cause `io::ErrorKind::WouldBlock` happening.
///
/// This selector is currently only support socket due to `Afd` driver is winsock2 specific.
#[derive(Debug)]
pub struct Selector {
    #[cfg(debug_assertions)]
    id: usize,

    inner: Arc<SelectorInner>,
}

impl Selector {
    pub fn new() -> io::Result<Selector> {
        SelectorInner::new().map(|inner| {
            #[cfg(debug_assertions)]
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed) + 1;
            Selector {
                #[cfg(debug_assertions)]
                id,
                inner: Arc::new(inner),
            }
        })
    }

    pub fn try_clone(&self) -> io::Result<Selector> {
        Ok(Selector {
            #[cfg(debug_assertions)]
            id: self.id,
            inner: Arc::clone(&self.inner),
        })
    }

    /// # Safety
    ///
    /// This requires a mutable reference to self because only a single thread
    /// can poll IOCP at a time.
    pub fn select(&mut self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.select(events, timeout)
    }

    pub(super) fn clone_port(&self) -> Arc<IocpHandlerRegistry> {
        self.inner.cp.clone()
    }
}

cfg_os_util! {
    use std::fmt;
    use std::cell::UnsafeCell;
    use std::os::windows::io::AsRawHandle;
    use std::ops::BitOr;
    use std::num::NonZeroU32;

    use winapi::um::minwinbase::OVERLAPPED_ENTRY;

    use crate::poll;
    use crate::{Registry, Token};

    const OVERLAPPED_CANARY: u32 = 0x5e219f3a;

    /// Holds a set of actions that the sending IOCP client is ready to do.
    ///
    /// These can be combined into one `Readiness` instance by using the bitwise or operator '|'.
    /// However, there is no "not ready" state, i.e. and empty `Readiness`. To indicate the absence of
    /// any ready operation, use Option<Readiness> and return a None value.
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub struct Readiness {
        flags: NonZeroU32,
    }

    impl Readiness {
        /// Readiness that indicates readiness to read
        pub const READ: Readiness = Readiness { flags: unsafe { NonZeroU32::new_unchecked(0b_0000_0001) } };
        /// Readiness that indicates readiness to write
        pub const WRITE: Readiness = Readiness { flags: unsafe { NonZeroU32::new_unchecked(0b_0000_0010) } };

        /// Checks whether the provided readiness is fulfilled
        pub fn is_set(&self, readiness: Readiness) -> bool {
            (self.flags.get() & readiness.flags.get()) == readiness.flags.get()
        }

        pub(crate) fn into_event(self, token: Token) -> Event {
            let readable = if self.is_set(Self::READ) { afd::POLL_RECEIVE } else { 0 };
            let writable = if self.is_set(Self::WRITE) { afd::POLL_SEND } else { 0 };
            let flags = readable | writable;
            debug_assert!(flags != 0);
            Event {
                data: token.0 as u64,
                flags
            }
        }
    }

    impl BitOr for Readiness {
        type Output = Readiness;

        fn bitor(self, rhs: Self) -> Self::Output {
            // Since neither self nor rhs can be zero (assuming absence of transmuted Readinesses), it's
            // safe to assume that the result is also nonzero.
            let flags = unsafe {
                NonZeroU32::new_unchecked(self.flags.get() | rhs.flags.get())
            };
            Readiness { flags }
        }
    }

    /// Callback trait for completed IOCP operations.
    ///
    /// This trait may be implemented to handle completed IOCP operations. The implementing object may
    /// signal the readiness for certain operations to the upper layer by returning a `Readiness` value
    /// that is then converted into `mio::Event` and delivered to the user.
    ///
    /// This trait is implemented for closures as well.
    pub trait CompletionCallback {
        /// Called on completion of the associated operation.
        ///
        /// The callback will be called once the operation that it is bound to via a call to
        /// `Overlapped::new` has been completed and the IOCP is signalled. By returning a `Readiness`
        /// value, the callback may deliver a `mio::Event` to the user.
        fn complete_operation(&mut self, entry: &OVERLAPPED_ENTRY) -> Option<Readiness>;
    }

    impl<F> CompletionCallback for F where F: FnMut(&OVERLAPPED_ENTRY) -> Option<Readiness> {
        fn complete_operation(&mut self, entry: &OVERLAPPED_ENTRY) -> Option<Readiness> {
            (*self)(entry)
        }
    }

    #[derive(Debug)]
    pub(crate) struct RawHandleCompletionHandler {
        token: Token
    }

    impl IocpHandler for RawHandleCompletionHandler {
        fn handle_completion(&mut self, status: &CompletionStatus) -> Option<Event> {
            let overlapped = status.overlapped() as *mut Overlapped;
            unsafe {
                debug_assert!((*overlapped).canary == OVERLAPPED_CANARY);
                (*overlapped).callback.complete_operation(status.entry())
                    .map(|r| r.into_event(self.token))
            }
        }
    }

    /// A `Binding` is embedded in all I/O objects associated with a `Registry`.
    ///
    /// Each registration keeps track of which selector the I/O object is
    /// associated with, ensuring that implementations of `Source` can be
    /// conformant for the various methods on Windows.
    ///
    /// If you're working with custom IOCP-enabled objects then you'll want to
    /// ensure that one of these instances is stored in your object and used in the
    /// implementation of `Source`.
    ///
    /// For more information about how to use this see the `windows` module
    /// documentation in this crate.
    #[derive(Debug, Clone)]
    pub struct Binding {
        selector: Arc<SelectorInner>,
        token: Token
    }

    impl Binding {
        /// Creates a new blank binding ready to be inserted into an I/O
        /// object.
        ///
        /// Won't actually do anything until `register_handle` has been called during a call to
        /// `Registry::register`.
        pub fn new(registry: &Registry, token: Token) -> Self {
            let selector = poll::selector(registry);
            Binding { selector: Arc::clone(&selector.inner), token }
        }

        /// Register a raw windows handle with the IOCP event loop.
        ///
        /// The underlying resource needs to support overlapped operations. Thus, files and similar
        /// entities need to be opened in overlapped mode.
        ///
        /// While it is safely possible to call this function more than once it is discouraged to do
        /// so as the handler will use the same token again when generating events. This makes it hard
        /// for the user of the registered `Source`.
        pub fn register_handle<H>(&self, handle: &H) -> io::Result<()> where H: AsRawHandle + ?Sized {
            let handler = RawHandleCompletionHandler {
                token: self.token
            };
            self.selector.cp.register_handle(handle, handler.into())
        }

        /// Inject an event to the event loop.
        ///
        /// In case it is required to generate an event even though there was no callback due to a
        /// completed IO operation, this method can be used to mark the associated token as ready, e.g.
        /// in case it is necessary to signal an initial readiness before any IO operation has been
        /// scheduled.
        pub fn inject_event(&self, readiness: Readiness) {
            let event = readiness.into_event(self.token);
            self.selector.injected_events.lock().unwrap().push(event)
        }

        /// Check whether the provided registry is the same as the one used during creation of the
        /// binding.
        ///
        /// IOCP driven sources cannot unregister from their completion port before they get closed.
        /// Therefore, implementations of reregister and deregister should check whether they have been
        /// called on the same `Registry` using this method to report errors in case this constraint
        /// has been violated.
        pub fn is_same_registry(&self, registry: &Registry) -> bool {
            Arc::ptr_eq(&self.selector, &poll::selector(registry).inner)
        }
    }

    /// A wrapper around an internal instance over `miow::Overlapped` which is in
    /// turn a wrapper around the Windows type `OVERLAPPED`.
    ///
    /// This type is required to be used for all IOCP operations on handles that are
    /// registered with an event loop. The event loop will receive notifications
    /// over `OVERLAPPED` pointers that have completed, and it will cast that
    /// pointer to a pointer to this structure and invoke the associated callback.
    #[repr(C)]
    pub struct Overlapped {
        inner: UnsafeCell<miow::Overlapped>,
        #[cfg(debug_assertions)]
        canary: u32,
        callback: Box<dyn CompletionCallback>,
    }

    impl fmt::Debug for Overlapped {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Overlapped").finish()
        }
    }

    impl Overlapped {
        /// Creates a new `Overlapped` which will invoke the provided `cb` callback
        /// whenever it's triggered.
        ///
        /// The returned `Overlapped` must be used as the `OVERLAPPED` passed to all
        /// I/O operations that are registered with mio's event loop. When the I/O
        /// operation associated with an `OVERLAPPED` pointer completes the event
        /// loop will invoke the object provided as `callback`.
        pub fn new<C: CompletionCallback+'static>(callback: C) -> Self {
            Self {
                inner: UnsafeCell::new(miow::Overlapped::zero()),
                #[cfg(debug_assertions)]
                canary: OVERLAPPED_CANARY,
                callback: Box::new(callback) as Box<dyn CompletionCallback>,
            }
        }

        /// Get the underlying `Overlapped` instance as a raw pointer.
        ///
        /// This can be useful when only a shared borrow is held and the overlapped
        /// pointer needs to be passed down to winapi.
        pub fn as_mut_ptr(&self) -> *mut OVERLAPPED {
            unsafe {
                (*self.inner.get()).raw()
            }
        }
    }

    // Overlapped's APIs are marked as unsafe as they must be used with caution to ensure thread
    // safety. The structure itself is safe to send across threads.
    unsafe impl Send for Overlapped {}
    unsafe impl Sync for Overlapped {}
}

cfg_net! {
    use super::InternalState;

    impl Selector {
        pub(super) fn register(
            &self,
            socket: RawSocket,
            token: Token,
            interests: Interest,
        ) -> io::Result<InternalState> {
            SelectorInner::register(&self.inner, socket, token, interests)
        }

        pub(super) fn reregister(
            &self,
            state: Pin<Arc<Mutex<SockState>>>,
            token: Token,
            interests: Interest,
        ) -> io::Result<()> {
            self.inner.reregister(state, token, interests)
        }

        #[cfg(debug_assertions)]
        pub fn id(&self) -> usize {
            self.id
        }
    }
}

#[derive(Debug)]
pub struct SelectorInner {
    cp: Arc<IocpHandlerRegistry>,
    update_queue: Arc<Mutex<VecDeque<Pin<Arc<Mutex<SockState>>>>>>,
    injected_events: Mutex<Vec<Event>>,
    afd_group: AfdGroup,
    is_polling: AtomicBool,
}

// We have ensured thread safety by introducing lock manually.
unsafe impl Sync for SelectorInner {}

impl SelectorInner {
    pub fn new() -> io::Result<SelectorInner> {
        IocpHandlerRegistry::new().map(|cp| {
            let cp = Arc::new(cp);
            let cp_afd = Arc::clone(&cp);
            let update_queue = Arc::new(Mutex::new(VecDeque::new()));
            let completion_handler = AfdCompletionPortEventHandler {
                selector_update_queue: Arc::clone(&update_queue),
                update_queue: Vec::new()
            };

            SelectorInner {
                cp,
                update_queue,
                injected_events: Mutex::default(),
                afd_group: AfdGroup::new(cp_afd, completion_handler),
                is_polling: AtomicBool::new(false),
            }
        })
    }

    /// # Safety
    ///
    /// May only be calling via `Selector::select`.
    pub fn select(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        events.clear();

        if timeout.is_none() {
            loop {
                let len = self.select2(&mut events.statuses, &mut events.events, None)?;
                if len == 0 {
                    continue;
                }
                return Ok(());
            }
        } else {
            self.select2(&mut events.statuses, &mut events.events, timeout)?;
            return Ok(());
        }
    }

    pub fn select2(
        &self,
        statuses: &mut [CompletionStatus],
        events: &mut Vec<Event>,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        assert_eq!(self.is_polling.swap(true, Ordering::AcqRel), false);

        unsafe { self.update_sockets_events() }?;

        events.extend(self.injected_events.lock().unwrap().drain(..));
        // since events was empty before this call, the number of events is the number of injected
        // events
        let num_drained_events = events.len();

        if num_drained_events == 0 {
            let result = self.cp.handle_pending_events(statuses, Some(events), timeout);

            self.is_polling.store(false, Ordering::Relaxed);

            match result {
                Ok(num_events) => {
                    self.afd_group.release_unused_afd();
                    Ok(num_events + num_drained_events)
                },
                Err(e) => Err(e),
            }
        } else {
            self.is_polling.store(false, Ordering::Relaxed);
            Ok(num_drained_events)
        }
    }

    unsafe fn update_sockets_events(&self) -> io::Result<()> {
        let mut update_queue = self.update_queue.lock().unwrap();
        for sock in update_queue.iter_mut() {
            let mut sock_internal = sock.lock().unwrap();
            if !sock_internal.is_pending_deletion() {
                sock_internal.update(&sock)?;
            }
        }

        // remove all sock which do not have error, they have afd op pending
        update_queue.retain(|sock| sock.lock().unwrap().has_error());

        self.afd_group.release_unused_afd();
        Ok(())
    }
}

cfg_net! {
    use std::mem::size_of;
    use std::ptr::null_mut;
    use winapi::um::mswsock;
    use winapi::um::winsock2::WSAGetLastError;
    use winapi::um::winsock2::{WSAIoctl, SOCKET_ERROR};

    impl SelectorInner {
        fn register(
            this: &Arc<Self>,
            socket: RawSocket,
            token: Token,
            interests: Interest,
        ) -> io::Result<InternalState> {
            let flags = interests_to_afd_flags(interests);

            let sock = {
                let sock = this._alloc_sock_for_rawsocket(socket)?;
                let event = Event {
                    flags,
                    data: token.0 as u64,
                };
                sock.lock().unwrap().set_event(event);
                sock
            };

            let state = InternalState {
                selector: this.clone(),
                token,
                interests,
                sock_state: sock.clone(),
            };

            this.queue_state(sock);
            unsafe { this.update_sockets_events_if_polling()? };

            Ok(state)
        }

        // Directly accessed in `IoSourceState::do_io`.
        pub(super) fn reregister(
            &self,
            state: Pin<Arc<Mutex<SockState>>>,
            token: Token,
            interests: Interest,
        ) -> io::Result<()> {
            {
                let event = Event {
                    flags: interests_to_afd_flags(interests),
                    data: token.0 as u64,
                };

                state.lock().unwrap().set_event(event);
            }

            // FIXME: a sock which has_error true should not be re-added to
            // the update queue because it's already there.
            self.queue_state(state);
            unsafe { self.update_sockets_events_if_polling() }
        }

        /// This function is called by register() and reregister() to start an
        /// IOCTL_AFD_POLL operation corresponding to the registered events, but
        /// only if necessary.
        ///
        /// Since it is not possible to modify or synchronously cancel an AFD_POLL
        /// operation, and there can be only one active AFD_POLL operation per
        /// (socket, completion port) pair at any time, it is expensive to change
        /// a socket's event registration after it has been submitted to the kernel.
        ///
        /// Therefore, if no other threads are polling when interest in a socket
        /// event is (re)registered, the socket is added to the 'update queue', but
        /// the actual syscall to start the IOCTL_AFD_POLL operation is deferred
        /// until just before the GetQueuedCompletionStatusEx() syscall is made.
        ///
        /// However, when another thread is already blocked on
        /// GetQueuedCompletionStatusEx() we tell the kernel about the registered
        /// socket event(s) immediately.
        unsafe fn update_sockets_events_if_polling(&self) -> io::Result<()> {
            if self.is_polling.load(Ordering::Acquire) {
                self.update_sockets_events()
            } else {
                Ok(())
            }
        }

        fn queue_state(&self, sock_state: Pin<Arc<Mutex<SockState>>>) {
            let mut update_queue = self.update_queue.lock().unwrap();
            update_queue.push_back(sock_state);
        }

        fn _alloc_sock_for_rawsocket(
            &self,
            raw_socket: RawSocket,
        ) -> io::Result<Pin<Arc<Mutex<SockState>>>> {
            let afd = self.afd_group.acquire()?;
            Ok(Arc::pin(Mutex::new(SockState::new(raw_socket, afd)?)))
        }
    }

    fn try_get_base_socket(raw_socket: RawSocket, ioctl: u32) -> Result<RawSocket, i32> {
        let mut base_socket: RawSocket = 0;
        let mut bytes: u32 = 0;
        unsafe {
            if WSAIoctl(
                raw_socket as usize,
                ioctl,
                null_mut(),
                0,
                &mut base_socket as *mut _ as PVOID,
                size_of::<RawSocket>() as u32,
                &mut bytes,
                null_mut(),
                None,
            ) != SOCKET_ERROR
            {
                Ok(base_socket)
            } else {
                Err(WSAGetLastError())
            }
        }
    }

    fn get_base_socket(raw_socket: RawSocket) -> io::Result<RawSocket> {
        let res = try_get_base_socket(raw_socket, mswsock::SIO_BASE_HANDLE);
        if let Ok(base_socket) = res {
            return Ok(base_socket);
        }

        // The `SIO_BASE_HANDLE` should not be intercepted by LSPs, therefore
        // it should not fail as long as `raw_socket` is a valid socket. See
        // https://docs.microsoft.com/en-us/windows/win32/winsock/winsock-ioctls.
        // However, at least one known LSP deliberately breaks it, so we try
        // some alternative IOCTLs, starting with the most appropriate one.
        for &ioctl in &[
            mswsock::SIO_BSP_HANDLE_SELECT,
            mswsock::SIO_BSP_HANDLE_POLL,
            mswsock::SIO_BSP_HANDLE,
        ] {
            if let Ok(base_socket) = try_get_base_socket(raw_socket, ioctl) {
                // Since we know now that we're dealing with an LSP (otherwise
                // SIO_BASE_HANDLE would't have failed), only return any result
                // when it is different from the original `raw_socket`.
                if base_socket != raw_socket {
                    return Ok(base_socket);
                }
            }
        }

        // If the alternative IOCTLs also failed, return the original error.
        let os_error = res.unwrap_err();
        let err = io::Error::from_raw_os_error(os_error);
        Err(err)
    }
}

impl Drop for SelectorInner {
    fn drop(&mut self) {
        loop {
            let mut statuses: [CompletionStatus; 1024] = [CompletionStatus::zero(); 1024];

            let result = self
                .cp
                .handle_pending_events(&mut statuses, None, Some(Duration::from_millis(0)));
            match result {
                Ok(events_num) if events_num == 0 => {
                    break
                },

                Ok(_) => (),

                Err(_) => {
                    break;
                }
            }
        }
    }
}

fn interests_to_afd_flags(interests: Interest) -> u32 {
    let mut flags = 0;

    if interests.is_readable() {
        flags |= READABLE_FLAGS | READ_CLOSED_FLAGS | ERROR_FLAGS;
    }

    if interests.is_writable() {
        flags |= WRITABLE_FLAGS | WRITE_CLOSED_FLAGS | ERROR_FLAGS;
    }

    flags
}
