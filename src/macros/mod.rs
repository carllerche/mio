//! Macros to ease conditional code based on enabled features.

// Depending on the features not all macros are used.
#![allow(unused_macros)]

/// Feature `os-poll` enabled.
macro_rules! cfg_os_poll {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "os-poll")]
            #[cfg_attr(docsrs, doc(cfg(feature = "os-poll")))]
            $item
        )*
    }
}

/// Feature `os-poll` disabled.
macro_rules! cfg_not_os_poll {
    ($($item:item)*) => {
        $(
            #[cfg(not(feature = "os-poll"))]
            $item
        )*
    }
}

/// One of the `tcp`, `udp`, `uds` features enabled.
#[cfg(unix)]
macro_rules! cfg_net {
    ($($item:item)*) => {
        $(
            #[cfg(any(feature = "tcp", feature = "udp", feature = "uds"))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "tcp", feature = "udp", feature = "uds"))))]
            $item
        )*
    }
}

/// One of the `tcp`, `udp` features enabled.
#[cfg(windows)]
macro_rules! cfg_net {
    ($($item:item)*) => {
        $(
            #[cfg(any(feature = "tcp", feature = "udp"))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "tcp", feature = "udp"))))]
            $item
        )*
    }
}

/// Feature `tcp` enabled.
macro_rules! cfg_tcp {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "tcp")]
            #[cfg_attr(docsrs, doc(cfg(feature = "tcp")))]
            $item
        )*
    }
}

/// Feature `udp` enabled.
macro_rules! cfg_udp {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "udp")]
            #[cfg_attr(docsrs, doc(cfg(feature = "udp")))]
            $item
        )*
    }
}

/// Feature `os-util` enabled.
macro_rules! cfg_os_util {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "os-util")]
            #[cfg_attr(docsrs, doc(cfg(feature = "os-util")))]
            $item
        )*
    }
}

/// Feature `uds` enabled.
#[cfg(unix)]
macro_rules! cfg_uds {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "uds")]
            #[cfg_attr(docsrs, doc(cfg(feature = "uds")))]
            $item
        )*
    }
}

/// Feature `os-util` enabled, or one of the features that need `os-util`.
#[cfg(unix)]
macro_rules! cfg_any_os_util {
    ($($item:item)*) => {
        $(
            #[cfg(any(feature = "os-util", feature = "tcp", feature = "udp", feature = "uds"))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "os-util", feature = "tcp", feature = "udp", feature = "uds"))))]
            $item
        )*
    }
}

/// Feature `os-util` enabled, or one of the features that need `os-util`.
#[cfg(windows)]
macro_rules! cfg_any_os_util {
    ($($item:item)*) => {
        $(
            #[cfg(any(feature = "os-util", feature = "tcp", feature = "udp"))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "os-util", feature = "tcp", feature = "udp"))))]
            $item
        )*
    }
}
