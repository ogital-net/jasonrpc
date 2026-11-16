//! Rejected input must be classified without allocating in proportion to its size.

#![cfg(feature = "server")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use jasonrpc::server::{Output, Router};
use jasonrpc::{codes, Id};

thread_local! {
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

fn record(bytes: usize) {
    let _ = ALLOCATED.try_with(|total| total.set(total.get() + bytes));
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size.saturating_sub(layout.size()));
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn allocated_by<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOCATED.with(Cell::get);
    let value = operation();
    (value, ALLOCATED.with(Cell::get) - before)
}

#[test]
fn rejected_bodies_are_classified_without_building_a_document() {
    const PAYLOAD_LEN: usize = 1024 * 1024;
    const MAX_ALLOCATED: usize = 64 * 1024;

    let mut numbers = String::with_capacity(PAYLOAD_LEN + 1);
    while numbers.len() < PAYLOAD_LEN {
        numbers.push_str("0,");
    }
    numbers.push('0');

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let router = Router::new();
    runtime.block_on(router.handle_bytes(b"null"));

    for (body, code) in [
        // Valid JSON that is not a request needs a full validity scan.
        (format!(r#"{{"a":[{numbers}]}}"#), codes::INVALID_REQUEST),
        // A semantic error stops the request parse before the trailing comma.
        (
            format!(r#"{{"jsonrpc":"1.0","a":[{numbers},]}}"#),
            codes::PARSE_ERROR,
        ),
        (format!(r#"{{"a":[{numbers}]"#), codes::PARSE_ERROR),
        (format!(r#"[{{"a":[{numbers}]}}"#), codes::PARSE_ERROR),
        (
            format!(r#""{}""#, "a".repeat(PAYLOAD_LEN)),
            codes::INVALID_REQUEST,
        ),
    ] {
        let shared = bytes::Bytes::from(body.clone().into_bytes());
        let (owned, owned_allocated) =
            allocated_by(|| runtime.block_on(router.handle_bytes(body.as_bytes())));
        let (shared, shared_allocated) =
            allocated_by(|| runtime.block_on(router.handle_shared_bytes(shared)));

        for (output, allocated) in [(owned, owned_allocated), (shared, shared_allocated)] {
            let Output::Single(response) = output else {
                panic!("expected a single error response");
            };
            assert_eq!(response.id(), &Id::Null);
            assert_eq!(response.error_obj().map(jasonrpc::Error::code), Some(code));
            assert!(
                allocated < MAX_ALLOCATED,
                "allocated {allocated} bytes rejecting a {} byte body",
                body.len()
            );
        }
    }
}
