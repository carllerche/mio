# MIO - Metal IO

MIO is a lightweight IO library for Rust with a focus on adding as
little overhead as possible over the OS abstractions.

[![Build Status](https://travis-ci.org/carllerche/mio.svg?branch=master)](https://travis-ci.org/carllerche/mio)
[![crates.io](http://meritbadge.herokuapp.com/mio)](https://crates.io/crates/mio)

**Getting started guide**

Currently a work in progress: [Getting
Started](https://github.com/carllerche/mio/blob/getting-started/doc/getting-started.md).
Feedback can be posted on the [PR](https://github.com/carllerche/mio/pull/222).

**API documentation**

* [master](http://rustdoc.s3-website-us-east-1.amazonaws.com/mio/master/mio/)
* [v0.4](http://rustdoc.s3-website-us-east-1.amazonaws.com/mio/v0.4.x/mio/)
* [v0.3](http://rustdoc.s3-website-us-east-1.amazonaws.com/mio/v0.3.x/mio/)

## Usage

To use `mio`, first add this to your `Cargo.toml`:

```toml
[dependencies]
mio = "0.3.0"
```

Then, add this to your crate root:

```rust
extern crate mio;
```

## Features

* Event loop backed by epoll, kqueue.
* Zero allocations at runtime
* Non-blocking TCP, UDP and Unix domain sockets
* High performance timer system
* Thread safe message channel for cross thread communication

__Eventually__

* Signal handling
* Windows support

## Non goals

The following are specifically omitted from MIO and are left to the user
or higher level libraries.

* File operations
* Thread pools / multi-threaded event loop

## Platforms

Currently, MIO only supports Linux and Darwin. The goal is to support
all platforms that support Rust and the readiness IO model.

## Resources

Please submit PRs containing links to MIO resources.

* [Mio Rustcamp talk](http://confreaks.tv/videos/rustcamp2015-writing-high-performance-async-io-apps)
* [My Basic Understanding of mio and Asynchronous IO](http://hermanradtke.com/2015/07/12/my-basic-understanding-of-mio-and-async-io.html)
* [Creating A Multi-echo Server using Rust and mio](http://hermanradtke.com/2015/07/22/creating-a-multi-echo-server-using-rust-and-mio.html)
* [Writing Scalable Chat Service from Scratch](http://nbaksalyar.github.io/2015/07/10/writing-chat-in-rust.html)
* [Design Notes About Rotor Library](https://medium.com/@paulcolomiets/asynchronous-io-in-rust-36b623e7b965)

### Libraries

* [Eventual IO](//github.com/carllerche/eventual_io) - Proof of
  concept TCP library built on top of Mio and Eventual's futures &
  streams.
* [mioco](//github.com/dpc/mioco) - Mio COroutines
* [simplesched](//github.com/zonyitoo/simplesched) - Coroutine I/O with a simple scheduler
* [coio-rs](//github.com/zonyitoo/coio-rs) - Coroutine I/O with work-stealing scheduler
* [rotor](//github.com/tailhook/rotor) - A wrapper that allows to create composable I/O libraries on top of mio

## Community

A group of mio users hang out in the #mio channel on the Mozilla IRC
server (irc.mozilla.org). This can be a good place to go for questions.
