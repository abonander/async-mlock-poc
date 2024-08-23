use futures_channel::oneshot;
use futures_util::task::AtomicWaker;
use nix::sys::mman::{MapFlags, ProtFlags};
use nix::unistd::SysconfVar;
use std::any::Any;
use std::fs::File;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LockResult, Mutex, MutexGuard, TryLockError};
use std::task::{ready, Context, Poll};
use std::thread::{self};
use std::{cmp, io};
use std::future::Future;
use std::pin::pin;

#[cfg(feature = "tokio")]
mod tokio_impl;

static THREAD_ID: AtomicUsize = AtomicUsize::new(0);

const THREAD_STACK_SIZE: usize = 32 * 1024; // 32 KiB

//
const READAHEAD_BYTES: usize = 8 * 1024;

pub struct MmapFile {
    shared: Arc<Shared>,
    available_range: Range<usize>,
    requested_range: Range<usize>,
    thread_handle: Option<thread::JoinHandle<io::Result<()>>>,
}

struct Shared {
    file: File,
    mmap: NonNull<[u8]>,

    page_size: usize,

    waker: AtomicWaker,
    cvar: Condvar,

    ranges: Mutex<Ranges>,

    closed: AtomicBool,
    unlocking: AtomicBool,
}

unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

struct Ranges {
    requested: Range<usize>,
    locked: Range<usize>,
}

impl MmapFile {
    pub async fn open<P: Into<PathBuf>>(path: P) -> io::Result<Self> {
        let path = path.into();

        let (opened_tx, opened_rx) = oneshot::channel();

        let thread_handle = thread::Builder::new()
            .name(format!(
                "async-mlock-{}",
                THREAD_ID.fetch_add(1, Ordering::SeqCst)
            ))
            .stack_size(THREAD_STACK_SIZE)
            .spawn(move || worker(path, opened_tx))?;

        let Ok(shared) = opened_rx.await else {
            thread_handle
                .join()
                .unwrap_or_else(|e| {
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("worker thread panicked: {}", panic_payload_str(&e)),
                    ))
                })?;

            panic!("BUG: worker thread exited without returning a result or panicking")
        };

        Ok(Self {
            shared,
            available_range: 0..0,
            requested_range: 0..0,
            thread_handle: Some(thread_handle),
        })
    }

    #[inline(always)]
    pub(crate) fn consume_inner(&mut self, bytes: usize) {
        // `.nth(N)` consumes N + 1 elements.
        let _ = bytes
            .checked_sub(1)
            .and_then(|n| self.available_range.nth(n));
    }

    pub(crate) fn poll_fill_buf_inner<'a>(
        &'a mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<&'a [u8]>> {
        loop {
            self.shared.waker.register(cx.waker());

            if !self.available_range.is_empty() {
                return Poll::Ready(Ok(unsafe {
                    &self.shared.mmap.as_ref()[self.available_range.clone()]
                }));
            }

            if self.available_range.end == self.shared.mmap.len() {
                return Poll::Ready(Ok(&[]));
            }

            self.request_position(self.available_range.end as u64);

            ready!(self.poll_complete_request(cx))?;
        }
    }

    pub(crate) fn request_position(&mut self, pos: u64) {
        let len = self.shared.mmap.len();

        let pos = usize::try_from(pos).unwrap_or(len);

        let range_start = cmp::min(pos, len);
        let range_end = cmp::min(pos.saturating_add(READAHEAD_BYTES), len);

        self.requested_range = range_start..range_end;
    }

    pub(crate) fn request_seek(&mut self, seek: SeekFrom) -> io::Result<()> {
        let seek_pos = match seek {
            SeekFrom::Start(pos) => pos,
            SeekFrom::Current(offset) => {
                let current = self.available_range.start as u64;

                current.checked_add_signed(offset).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "seek position out of range: {seek:?} (current position: {current})"
                        ),
                    )
                })?
            }
            SeekFrom::End(offset) => {
                let end = self.shared.mmap.len() as u64;

                end.checked_add_signed(offset).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("seek position out of range: {seek:?} (current end: {end})"),
                    )
                })?
            }
        };

        self.request_position(seek_pos);

        Ok(())
    }

    pub(crate) fn poll_complete_request(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        self.shared.waker.register(cx.waker());

        let mut ranges = match ready!(self.shared.poll_lock_ranges(cx)) {
            Ok(ranges) => ranges,
            Err(_) => {
                let Some(thread) = self.thread_handle.take_if(|t| t.is_finished()) else {
                    return Poll::Pending;
                };

                thread.join().unwrap_or_else(|e| {
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("worker thread panicked: {}", panic_payload_str(&e)),
                    ))
                })?;

                panic!("BUG: thread exited without panicking");
            }
        };

        if ranges.requested != self.requested_range {
            ranges.requested = self.requested_range.clone();
        }

        if !ranges.locked_contains_requested() {
            self.shared.cvar.notify_all();
            return Poll::Pending;
        }

        self.available_range =
            cmp::max(ranges.requested.start, ranges.locked.start)..ranges.locked.end;

        Poll::Ready(Ok(self.available_range.start as u64))
    }
}

impl Drop for MmapFile {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.cvar.notify_one();
    }
}

impl Ranges {
    #[inline(always)]
    fn locked_contains_requested(&self) -> bool {
        range_contains(&self.locked, &self.requested)
    }
}

impl Shared {
    fn poll_lock_ranges<'a>(&'a self, cx: &mut Context<'_>) -> Poll<LockResult<MutexGuard<'a, Ranges>>> {
        for _ in 0 .. 256 {
            match self.ranges.try_lock() {
                Ok(ranges) => {
                    self.unlocking.store(false, Ordering::Relaxed);
                    return Poll::Ready(Ok(ranges));
                },
                Err(TryLockError::WouldBlock) => {
                    if self.unlocking.load(Ordering::Acquire) {
                        // The worker thread promises that the lock is going to be unlocked shortly.
                        // Spin for a moment to see if it unlocks before returning to the scheduler.
                        std::hint::spin_loop();
                        continue;
                    }

                    break;
                },
                Err(TryLockError::Poisoned(poison)) => {
                    return Poll::Ready(Err(poison));
                }
            }
        }

        if self.unlocking.load(Ordering::Acquire) {
            // Ensure a notification for our own waker is stored
            // so that the runtime polls the task again soon.
            wake_deferred(cx);
        }

        Poll::Pending
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        unsafe {
            let _ = nix::sys::mman::munmap(self.mmap.cast(), self.mmap.len());
        }
    }
}

struct CloseGuard<'a>(&'a Shared);

impl Drop for CloseGuard<'_> {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.waker.wake();
    }
}

fn worker(path: PathBuf, opened_tx: oneshot::Sender<Arc<Shared>>) -> io::Result<()> {
    let file = File::open(&path)?;

    let meta = file.metadata()?;

    let len = usize::try_from(meta.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("file size too large: {}", meta.len()),
        )
    })?;

    let len = NonZeroUsize::new(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "cannot mmap an empty file"))?;

    let page_size = nix::unistd::sysconf(SysconfVar::PAGE_SIZE)?;

    let page_size = page_size.expect("sysconf(PAGE_SIZE) returned nothing");

    let page_size = usize::try_from(page_size).unwrap_or_else(|_| {
        panic!("sysconf(PAGE_SIZE) returned value out of range: {page_size:?}")
    });

    let mmap = unsafe {
        nix::sys::mman::mmap(None, len, ProtFlags::PROT_READ, MapFlags::MAP_PRIVATE, &file, 0)?
    };

    let shared = Arc::new(Shared {
        file,
        mmap: NonNull::slice_from_raw_parts(mmap.cast(), len.get()),
        page_size,

        ranges: Mutex::new(Ranges {
            requested: 0..0,
            locked: 0..0,
        }),
        cvar: Condvar::new(),

        waker: AtomicWaker::new(),
        closed: AtomicBool::new(false),
        unlocking: AtomicBool::new(false),
    });

    // Marks the file as closed if dropped at any time
    let _guard = CloseGuard(&shared);

    let mut ranges = shared.ranges.lock().unwrap();

    if opened_tx.send(shared.clone()).is_err() {
        return Ok(());
    };

    loop {
        if shared.closed.load(Ordering::Acquire) {
            shared.waker.wake();
            break;
        }

        if ranges.locked_contains_requested() {
            // Because we don't actually have the ability to notify the task _after_ we've released
            // the lock, we need to store a flag telling it that it's _going_ to be unlocked
            // and that the task should keep trying to lock it until it succeeds.
            shared.unlocking.store(true, Ordering::Release);
            shared.waker.wake();
            ranges = shared.cvar.wait(ranges).unwrap();

            continue;
        }

        let len = shared.mmap.len();
        let page_size = shared.page_size;
        let page_mask = !(page_size - 1);

        // Round start and end to the previous and next page boundary, respectively.
        let lock_start = cmp::min(len, ranges.requested.start & page_mask);
        let lock_end = cmp::min(len, ranges.requested.end.next_multiple_of(page_size));

        let lock_range = lock_start..lock_end;

        // TODO: don't unlock pages that we're going to re-lock anyway
        if !ranges.locked.is_empty() {
            unsafe {
                let unlock_slice = &shared.mmap.as_ref()[ranges.locked.clone()];

                nix::sys::mman::munlock(
                    NonNull::from(&unlock_slice[0]).cast(),
                    unlock_slice.len(),
                )?;
            }
        }

        if !lock_range.is_empty() {
            unsafe {
                let lock_slice = &shared.mmap.as_ref()[lock_range.clone()];

                nix::sys::mman::mlock(NonNull::from(&lock_slice[0]).cast(), lock_slice.len())?;
            }
        }

        ranges.locked = lock_range;
    }

    Ok(())
}

#[inline(always)]
fn range_contains(outer: &Range<usize>, inner: &Range<usize>) -> bool {
    inner.start >= outer.start && inner.end <= outer.end
}

fn panic_payload_str<'a>(e: &'a (dyn Any + Send + 'static)) -> &'a str {
    if let Some(s) = e.downcast_ref::<String>() {
        return s;
    }

    if let Some(s) = e.downcast_ref::<&'static str>() {
        return s;
    }

    "<non-string payload>"
}

fn wake_deferred(cx: &mut Context<'_>) {
    #[cfg(feature = "tokio")]
    {
        // Internally tells the runtime to push the task to the back of the queue.
        // Otherwise, it may be polled again immediately.
        let _ = pin!(tokio::task::yield_now()).poll(cx);
    }

    #[cfg(not(feature = "tokio"))]
    {
        // This is essentially what `async-std` does.
        cx.waker().wake_by_ref();
    }
}
