#![cfg(all(windows, feature = "os-util"))]

use mio::windows;
use mio::windows::Readiness;
use mio::{event::Source, Events, Interest, Poll, Registry, Token};
use std::default::Default;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::iter;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, RawHandle};
use std::ptr;
use winapi::shared::winerror::ERROR_IO_PENDING;
use winapi::um::minwinbase::OVERLAPPED_ENTRY;
use winapi::um::{
    errhandlingapi::GetLastError,
    fileapi::{CreateFileW, WriteFile, CREATE_ALWAYS},
    handleapi::INVALID_HANDLE_VALUE,
    minwinbase::OVERLAPPED,
    winbase::FILE_FLAG_OVERLAPPED,
    winnt::GENERIC_WRITE,
};

struct AsyncFile {
    file: File,
    binding: Option<windows::Binding>,
}

impl Source for AsyncFile {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        _interests: Interest,
    ) -> io::Result<()> {
        let binding = windows::Binding::new(registry, token);
        self.binding = Some(binding);
        self.binding.as_ref().unwrap().register_handle(&self.file)
    }

    fn reregister(
        &mut self,
        _registry: &Registry,
        _token: Token,
        _interests: Interest,
    ) -> io::Result<()> {
        unimplemented!()
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        unimplemented!()
    }
}

impl AsRawHandle for AsyncFile {
    fn as_raw_handle(&self) -> RawHandle {
        self.file.as_raw_handle()
    }
}

#[test]
fn register_custom_iocp_handler() {
    let mut poll = Poll::new().unwrap();

    let path: &OsStr = "C:\\temp\\test.txt".as_ref();
    let path_u16: Vec<u16> = path.encode_wide().chain(iter::once(0)).collect();
    let mut file = unsafe {
        let handle = CreateFileW(
            path_u16.as_ptr(),
            GENERIC_WRITE,
            0,
            ptr::null_mut(),
            CREATE_ALWAYS,
            FILE_FLAG_OVERLAPPED,
            ptr::null_mut(),
        );
        if handle == INVALID_HANDLE_VALUE {
            panic!("Unable to open file!");
        }
        AsyncFile {
            file: File::from_raw_handle(handle),
            binding: None,
        }
    };

    poll.registry()
        .register(&mut file, Token(0), Interest::WRITABLE)
        .unwrap();

    let mut buffer: Vec<_> = iter::successors(Some(15u8), |p| Some(p.wrapping_add(2)))
        .take(1024)
        .collect();
    loop {
        unsafe {
            let mut overlapped =
                windows::Overlapped::new(move |_: &OVERLAPPED_ENTRY| Some(Readiness::WRITE));
            if WriteFile(
                file.as_raw_handle(),
                buffer.as_mut_ptr() as *mut _,
                buffer.len() as u32,
                ptr::null_mut(),
                &mut overlapped as *mut windows::Overlapped as *mut OVERLAPPED,
            ) == 0
            {
                match GetLastError() {
                    ERROR_IO_PENDING => {
                        let mut events = Events::with_capacity(16);
                        poll.poll(&mut events, None).unwrap();
                        let mut event_iter = events.iter();
                        let event = event_iter.next().unwrap();
                        assert_eq!(0, event.token().0);
                        assert!(event.is_writable());
                        assert!(event_iter.next().is_none());
                        break;
                    }

                    e => panic!("Error during file write operation: {}", e),
                }
            }
        }
    }
}
