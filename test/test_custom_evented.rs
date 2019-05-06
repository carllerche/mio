use mio::event::Evented;
use mio::{Events, Poll, PollOpt, Ready, Registration, SetReadiness, Token};
use std::time::Duration;

#[test]
fn smoke() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);

    let (r, set) = Registration::new();
    r.register(
        poll.registry(),
        Token(0),
        Ready::readable(),
        PollOpt::edge(),
    )
    .unwrap();

    let n = poll
        .poll(&mut events, Some(Duration::from_millis(0)))
        .unwrap();
    assert_eq!(n, 0);

    set.set_readiness(Ready::readable()).unwrap();

    let n = poll
        .poll(&mut events, Some(Duration::from_millis(0)))
        .unwrap();
    assert_eq!(n, 1);

    assert_eq!(events.iter().next().unwrap().token(), Token(0));
}

#[test]
fn set_readiness_before_register() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);

    for _ in 0..5_000 {
        let (r, set) = Registration::new();

        let b1 = Arc::new(Barrier::new(2));
        let b2 = b1.clone();

        let th = thread::spawn(move || {
            // set readiness before register
            set.set_readiness(Ready::readable()).unwrap();

            // run into barrier so both can pass
            b2.wait();
        });

        // wait for readiness
        b1.wait();

        // now register
        poll.registry()
            .register(&r, Token(123), Ready::readable(), PollOpt::edge())
            .unwrap();

        loop {
            let n = poll.poll(&mut events, None).unwrap();

            if n == 0 {
                continue;
            }

            assert_eq!(n, 1);
            assert_eq!(events.iter().next().unwrap().token(), Token(123));
            break;
        }

        th.join().unwrap();
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
mod stress {
    use mio::event::Evented;
    use mio::{Events, Poll, PollOpt, Ready, Registration, SetReadiness, Token};
    use std::time::Duration;

    #[test]
    fn single_threaded_poll() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::{Acquire, Release};
        use std::sync::Arc;
        use std::thread;

        const NUM_ATTEMPTS: usize = 30;
        const NUM_ITERS: usize = 500;
        const NUM_THREADS: usize = 4;
        const NUM_REGISTRATIONS: usize = 128;

        for _ in 0..NUM_ATTEMPTS {
            let mut poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(NUM_REGISTRATIONS);

            let registrations: Vec<_> = (0..NUM_REGISTRATIONS)
                .map(|i| {
                    let (r, s) = Registration::new();
                    r.register(
                        poll.registry(),
                        Token(i),
                        Ready::readable(),
                        PollOpt::edge(),
                    )
                    .unwrap();
                    (r, s)
                })
                .collect();

            let mut ready: Vec<_> = (0..NUM_REGISTRATIONS).map(|_| Ready::empty()).collect();

            let remaining = Arc::new(AtomicUsize::new(NUM_THREADS));

            for _ in 0..NUM_THREADS {
                let remaining = remaining.clone();

                let set_readiness: Vec<SetReadiness> =
                    registrations.iter().map(|r| r.1.clone()).collect();

                thread::spawn(move || {
                    for _ in 0..NUM_ITERS {
                        for i in 0..NUM_REGISTRATIONS {
                            set_readiness[i].set_readiness(Ready::readable()).unwrap();
                            set_readiness[i].set_readiness(Ready::empty()).unwrap();
                            set_readiness[i].set_readiness(Ready::writable()).unwrap();
                            set_readiness[i]
                                .set_readiness(Ready::readable() | Ready::writable())
                                .unwrap();
                            set_readiness[i].set_readiness(Ready::empty()).unwrap();
                        }
                    }

                    for i in 0..NUM_REGISTRATIONS {
                        set_readiness[i].set_readiness(Ready::readable()).unwrap();
                    }

                    remaining.fetch_sub(1, Release);
                });
            }

            while remaining.load(Acquire) > 0 {
                // Set interest
                for (i, &(ref r, _)) in registrations.iter().enumerate() {
                    r.reregister(
                        poll.registry(),
                        Token(i),
                        Ready::writable(),
                        PollOpt::edge(),
                    )
                    .unwrap();
                }

                poll.poll(&mut events, Some(Duration::from_millis(0)))
                    .unwrap();

                for event in &events {
                    ready[event.token().0] = event.readiness();
                }

                // Update registration
                // Set interest
                for (i, &(ref r, _)) in registrations.iter().enumerate() {
                    r.reregister(
                        poll.registry(),
                        Token(i),
                        Ready::readable(),
                        PollOpt::edge(),
                    )
                    .unwrap();
                }
            }

            // Finall polls, repeat until readiness-queue empty
            loop {
                // Might not read all events from custom-event-queue at once, implementation dependend
                poll.poll(&mut events, Some(Duration::from_millis(0)))
                    .unwrap();
                if events.is_empty() {
                    // no more events in readiness queue pending
                    break;
                }
                for event in &events {
                    ready[event.token().0] = event.readiness();
                }
            }

            // Everything should be flagged as readable
            for ready in ready {
                assert_eq!(ready, Ready::readable());
            }
        }
    }

    #[test]
    fn with_small_events_collection() {
        const N: usize = 8;
        const ITER: usize = 1_000;

        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering::{Acquire, Release};
        use std::sync::{Arc, Barrier};
        use std::thread;

        let mut poll = Poll::new().unwrap();
        let mut registrations = vec![];

        let barrier = Arc::new(Barrier::new(N + 1));
        let done = Arc::new(AtomicBool::new(false));

        for i in 0..N {
            let (registration, set_readiness) = Registration::new();
            poll.registry()
                .register(&registration, Token(i), Ready::readable(), PollOpt::edge())
                .unwrap();

            registrations.push(registration);

            let barrier = barrier.clone();
            let done = done.clone();

            thread::spawn(move || {
                barrier.wait();

                while !done.load(Acquire) {
                    set_readiness.set_readiness(Ready::readable()).unwrap();
                }

                // Set one last time
                set_readiness.set_readiness(Ready::readable()).unwrap();
            });
        }

        let mut events = Events::with_capacity(4);

        barrier.wait();

        for _ in 0..ITER {
            poll.poll(&mut events, None).unwrap();
        }

        done.store(true, Release);

        let mut final_ready = vec![false; N];

        for _ in 0..5 {
            poll.poll(&mut events, None).unwrap();

            for event in &events {
                final_ready[event.token().0] = true;
            }

            if final_ready.iter().all(|v| *v) {
                return;
            }

            thread::sleep(Duration::from_millis(10));
        }

        panic!("dead lock?");
    }
}

#[test]
fn drop_registration_from_non_main_thread() {
    use std::sync::mpsc::channel;
    use std::thread;

    const THREADS: usize = 8;
    const ITERS: usize = 50_000;

    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let mut senders = Vec::with_capacity(THREADS);
    let mut token_index = 0;

    // spawn threads, which will send messages to single receiver
    for _ in 0..THREADS {
        let (tx, rx) = channel::<(Registration, SetReadiness)>();
        senders.push(tx);

        thread::spawn(move || {
            for (registration, set_readiness) in rx {
                let _ = set_readiness.set_readiness(Ready::readable());
                drop(registration);
                drop(set_readiness);
            }
        });
    }

    let mut index: usize = 0;
    for _ in 0..ITERS {
        let (registration, set_readiness) = Registration::new();
        registration
            .register(
                poll.registry(),
                Token(token_index),
                Ready::readable(),
                PollOpt::edge(),
            )
            .unwrap();
        let _ = senders[index].send((registration, set_readiness));

        token_index += 1;
        index += 1;
        if index == THREADS {
            index = 0;

            let (registration, set_readiness) = Registration::new();
            registration
                .register(
                    poll.registry(),
                    Token(token_index),
                    Ready::readable(),
                    PollOpt::edge(),
                )
                .unwrap();
            let _ = set_readiness.set_readiness(Ready::readable());
            drop(registration);
            drop(set_readiness);
            token_index += 1;

            thread::park_timeout(Duration::from_millis(0));
            let _ = poll.poll(&mut events, None).unwrap();
        }
    }
}
