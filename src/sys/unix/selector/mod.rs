#[cfg(any(target_os = "linux", target_os = "android", target_os = "solaris"))]
mod epoll;

#[cfg(any(target_os = "linux", target_os = "android", target_os = "solaris"))]
pub use self::epoll::{event, Event, Selector};

#[cfg(any(
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "ios",
    target_os = "macos",
    target_os = "netbsd",
    target_os = "openbsd"
))]
mod kqueue;

#[cfg(any(
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "ios",
    target_os = "macos",
    target_os = "netbsd",
    target_os = "openbsd"
))]
pub use self::kqueue::{event, Event, Selector};

pub type Events = Vec<Event>;
