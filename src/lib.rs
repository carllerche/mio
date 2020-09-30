#![doc(html_root_url = "https://docs.rs/mio/0.7.1")]
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    unused_imports,
    dead_code
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Disallow warnings when running tests.
#![cfg_attr(test, deny(warnings))]
// Disallow warnings in examples.
#![doc(test(attr(deny(warnings))))]

//! Mio is a fast, low-level I/O library for Rust focusing on non-blocking APIs
//! and event notification for building high performance I/O apps with as little
//! overhead as possible over the OS abstractions.
//!
//! # Usage
//!
//! Using Mio starts by creating a [`Poll`], which reads events from the OS and
//! puts them into [`Events`]. You can handle I/O events from the OS with it.
//!
//! For more detail, see [`Poll`].
//!
//! [`Poll`]: ../mio/struct.Poll.html
//! [`Events`]: ../mio/event/struct.Events.html
//!
//! ## Examples
//!
//! Examples can found in the `examples` directory of the source code, or [on
//! GitHub].
//!
//! [on GitHub]: https://github.com/tokio-rs/mio/tree/master/examples
//!
//! ## Guide
//!
//! A getting started guide is available in the
#![cfg_attr(
    feature = "extra-docs",
    doc = "[`guide`](../mio/guide/index.html) module."
)]
#![cfg_attr(
    not(feature = "extra-docs"),
    doc = "`guide` (only available when the `extra-docs` feature is enabled)."
)]
//!
//! ## Available features
//!
//! The available features are described in the
#![cfg_attr(
    feature = "extra-docs",
    doc = "[`features`](../mio/features/index.html) module."
)]
#![cfg_attr(
    not(feature = "extra-docs"),
    doc = "`features` (only available when the `extra-docs` feature is enabled)."
)]

// macros used internally
#[macro_use]
mod macros;

mod interest;
mod poll;
mod sys;
mod token;
mod waker;

pub mod event;

cfg_net! {
    mod io_source;

    pub mod net;
}

#[doc(no_inline)]
pub use event::Events;
pub use interest::Interest;
pub use poll::{Poll, Registry};
pub use token::Token;
pub use waker::Waker;

#[cfg(all(unix, feature = "os-util"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "os-util"))))]
pub mod unix {
    //! Unix only extensions.
    pub use crate::sys::SourceFd;
}

cfg_any_os_util! {
/// Windows-only extensions to the mio crate.
///
/// Mio on windows is currently implemented with IOCP for a high-performance
/// implementation of asynchronous I/O. On top of the IOCP, a connection to the "Ancillary Function
/// Driver" provides socket services. However, this approach doesn't work for other handle types
/// like files and named pipes. They need to be connected directly to the IO completion port like
/// this is done on unix-like systems  with the `SourceFd` type. The purpose of this module is to
/// similarly provide a mechanism for foreign I/O types to get hooked up into the IOCP event loop.
///
/// This module provides types for interfacing with a custom IOCP handle:
///
/// * `Binding` - this type is intended to govern binding with mio's `Registry`
///   type. Each I/O object should contain an instance of `Binding` that's
///   interfaced with for the implementation of the `Source` trait. The
///   `register`, `reregister`, and `deregister` methods for the `Source` trait
///   may use methods provided by `Binding` during their operations.
///
///   Also note that for types which represent streams of bytes the mio
///   interface of *readiness* doesn't map directly to the Windows model of
///   *completion*. This means that types will have to perform internal
///   buffering to ensure that a readiness interface can be provided.
///
/// * `Overlapped` - this type is intended to be used as the concrete instances
///   of the `OVERLAPPED` type that most win32 methods expect. It's crucial, for
///   safety, that all asynchronous operations are initiated with an instance of
///   `Overlapped` and not another instantiation of `OVERLAPPED`.
///
///   Mio's `Overlapped` type is created with an object implementing `CompletionHandler` that
///   is notified on completion of the associated asynchronous operation. The provided
///   `OVERLAPPED_ENTRY` type is defined in the `winapi` crate. Whenever a completion is posted to
///   an IOCP object the `OVERLAPPED` that was signaled will be interpreted as
///   `Overlapped` in the mio crate and the `CompletionHandler` will be invoked.
///   Through this object, and through the `OVERLAPPED` pointer, implementations can handle
///   management of I/O events.
///
/// * `Readiness` - this type is used to indicate whether an IOCP client is ready to start the next
///   IO operation. This is done either by returning a `Readiness` value from the
///   `CompletionHandler` or by using `Binding::inject_event`.
///
/// * `CompletionHandler` - this trait is implemented by the IOCP client and is used as a callback
///   when an asynchronous operation has finished. Currently it only implements one callback but may
///   be extended in the future.
///
/// When put together these types enable custom Windows handles to be
/// registered with mio's event loops. The `Binding` type is used to associate
/// handles and the `Overlapped` type is used to execute I/O operations. When
/// the I/O operations are completed an object implementing `CompletionHandler` is called which
/// typically emits a `Readiness` that is then reported to the user who then may start the next
/// asynchronous operation.
#[cfg(windows)]
    pub mod windows {
        //! Windows only extensions.
        pub use crate::sys::{
            Binding,
            CompletionCallback,
            Overlapped,
            Readiness,
        };
    }
}

// Enable with `cargo doc --features extra-docs`.
#[cfg(feature = "extra-docs")]
pub mod features {
    //! # Mio's optional features.
    //!
    //! This document describes the available features in Mio.
    //!
    #![cfg_attr(feature = "os-poll", doc = "## `os-poll` (enabled)")]
    #![cfg_attr(not(feature = "os-poll"), doc = "## `os-poll` (disabled)")]
    //!
    //! Mio by default provides only a shell implementation, that `panic!`s the
    //! moment it is actually run. To run it requires OS support, this is
    //! enabled by activating the `os-poll` feature.
    //!
    //! This makes `Poll`, `Registry` and `Waker` functional.
    //!
    #![cfg_attr(feature = "os-util", doc = "## `os-util` (enabled)")]
    #![cfg_attr(not(feature = "os-util"), doc = "## `os-util` (disabled)")]
    //!
    //! `os-util` enables additional OS specific facilities. Currently this
    //! means the `unix` module (with `SourceFd`) becomes available.
    //!
    //! ## Network types
    //!
    //! Mio provide three features to enable network types:
    //!
    #![cfg_attr(feature = "tcp", doc = "* `tcp` (enabled)")]
    #![cfg_attr(not(feature = "tcp"), doc = "* `tcp` (disabled)")]
    //! : includes `TcpStream` and `TcpListener`,
    #![cfg_attr(feature = "udp", doc = "* `udp` (enabled)")]
    #![cfg_attr(not(feature = "udp"), doc = "* `udp` (disabled)")]
    //! : includes `UdpSocket`, and
    #![cfg_attr(feature = "uds", doc = "* `uds` (enabled)")]
    #![cfg_attr(not(feature = "uds"), doc = "* `uds` (disabled)")]
    //! : includes `UnixDatagram`, `UnixListener`, `UnixStream` and `SocketAddr`.
    //!
    //! All types can be found in the `net` module.
    //!
    #![cfg_attr(feature = "extra-docs", doc = "## `extra-docs` (enabled)")]
    #![cfg_attr(not(feature = "extra-docs"), doc = "## `extra-docs` (disabled)")]
    //!
    //! This feature includes additional documentation such as this document and
    //! the getting started guide. It adds nothing in terms of types (only
    //! documentation).
}

// Enable with `cargo doc --features extra-docs`.
#[cfg(feature = "extra-docs")]
pub mod guide {
    //! # Getting started guide.
    //!
    //! In this guide we'll do the following:
    //!
    //! 1. Create a [`Poll`] instance (and learn what it is).
    //! 2. Register an [event source].
    //! 3. Create an event loop.
    //!
    //! At the end you'll have a very small (but quick) TCP server that accepts
    //! connections and then drops (disconnects) them.
    //!
    //! ## 1. Creating a `Poll` instance
    //!
    //! Using Mio starts by creating a [`Poll`] instance, which monitors events
    //! from the OS and puts them into [`Events`]. This allows us to execute I/O
    //! operations based on what operations are ready.
    //!
    //! [`Poll`]: ../struct.Poll.html
    //! [`Events`]: ../event/struct.Events.html
    //!
    //! ```
    //! # use mio::{Poll, Events};
    //! # fn main() -> std::io::Result<()> {
    //! // `Poll` allows for polling of readiness events.
    //! let poll = Poll::new()?;
    //! // `Events` is collection of readiness `Event`s and can be filled by
    //! // calling `Poll::poll`.
    //! let events = Events::with_capacity(128);
    //! # drop((poll, events));
    //! # Ok(())
    //! # }
    //! ```
    //!
    //! For example if we're using a [`TcpListener`],  we'll only want to
    //! attempt to accept an incoming connection *iff* any connections are
    //! queued and ready to be accepted. We don't want to waste our time if no
    //! connections are ready.
    //!
    //! [`TcpListener`]: ../net/struct.TcpListener.html
    //!
    //! ## 2. Registering event source
    //!
    //! After we've created a [`Poll`] instance that monitors events from the OS
    //! for us, we need to provide it with a source of events. This is done by
    //! registering an [event source]. As the name “event source” suggests it is
    //! a source of events which can be polled using a `Poll` instance. On Unix
    //! systems this is usually a file descriptor, or a socket/handle on
    //! Windows.
    //!
    //! In the example below we'll use a [`TcpListener`] for which we'll receive
    //! an event (from [`Poll`]) once a connection is ready to be accepted.
    //!
    //! [event source]: ../event/trait.Source.html
    //!
    //! ```
    //! # use mio::net::TcpListener;
    //! # use mio::{Poll, Token, Interest};
    //! # fn main() -> std::io::Result<()> {
    //! # let poll = Poll::new()?;
    //! # let address = "127.0.0.1:0".parse().unwrap();
    //! // Create a `TcpListener`, binding it to `address`.
    //! let mut listener = TcpListener::bind(address)?;
    //!
    //! // Next we register it with `Poll` to receive events for it. The `SERVER`
    //! // `Token` is used to determine that we received an event for the listener
    //! // later on.
    //! const SERVER: Token = Token(0);
    //! poll.registry().register(&mut listener, SERVER, Interest::READABLE)?;
    //! # Ok(())
    //! # }
    //! ```
    //!
    //! Multiple event sources can be [registered] (concurrently), so we can
    //! monitor multiple sources at a time.
    //!
    //! [registered]: ../struct.Registry.html#method.register
    //!
    //! ## 3. Creating the event loop
    //!
    //! After we've created a [`Poll`] instance and registered one or more
    //! [event sources] with it, we can [poll] it for events. Polling for events
    //! is simple, we need a container to store the events: [`Events`] and need
    //! to do something based on the polled events (this part is up to you, we
    //! can't do it all!). If we do this in a loop we've got ourselves an event
    //! loop.
    //!
    //! The example below shows the event loop in action, completing our small
    //! TCP server.
    //!
    //! [poll]: ../struct.Poll.html#method.poll
    //! [event sources]: ../event/trait.Source.html
    //!
    //! ```
    //! # use std::io;
    //! # use std::time::Duration;
    //! # use mio::net::TcpListener;
    //! # use mio::{Poll, Token, Interest, Events};
    //! # fn main() -> io::Result<()> {
    //! # let mut poll = Poll::new()?;
    //! # let mut events = Events::with_capacity(128);
    //! # let address = "127.0.0.1:0".parse().unwrap();
    //! # let mut listener = TcpListener::bind(address)?;
    //! # const SERVER: Token = Token(0);
    //! # poll.registry().register(&mut listener, SERVER, Interest::READABLE)?;
    //! // Start our event loop.
    //! loop {
    //!     // Poll the OS for events, waiting at most 100 milliseconds.
    //!     poll.poll(&mut events, Some(Duration::from_millis(100)))?;
    //!
    //!     // Process each event.
    //!     for event in events.iter() {
    //!         // We can use the token we previously provided to `register` to
    //!         // determine for which type the event is.
    //!         match event.token() {
    //!             SERVER => loop {
    //!                 // One or more connections are ready, so we'll attempt to
    //!                 // accept them (in a loop).
    //!                 match listener.accept() {
    //!                     Ok((connection, address)) => {
    //!                         println!("Got a connection from: {}", address);
    //! #                       drop(connection);
    //!                     },
    //!                     // A "would block error" is returned if the operation
    //!                     // is not ready, so we'll stop trying to accept
    //!                     // connections.
    //!                     Err(ref err) if would_block(err) => break,
    //!                     Err(err) => return Err(err),
    //!                 }
    //!             }
    //! #           _ => unreachable!(),
    //!         }
    //!     }
    //! #   return Ok(());
    //! }
    //!
    //! fn would_block(err: &io::Error) -> bool {
    //!     err.kind() == io::ErrorKind::WouldBlock
    //! }
    //! # }
    //! ```
}
