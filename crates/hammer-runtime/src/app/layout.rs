use std::mem;

use hammer_infra::align::{CACHE_LINE, align_up};
use hammer_infra::ring::LockFreeRingSlot;

use crate::app::ring::{AppCqeDescriptor, AppSqeDescriptor};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppRingMemoryKind {
    ProcessLocal,
    SharedMemory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppRingLayout {
    submission_ring_offset: usize,
    completion_ring_offset: usize,
    fill_ring_offset: usize,
    data_area_offset: usize,
    cacheline_size: usize,
    submission_ring_bytes: usize,
    completion_ring_bytes: usize,
    fill_ring_bytes: usize,
    submission_capacity: usize,
    completion_capacity: usize,
    data_chunk_size: usize,
    data_chunk_count: usize,
}

impl AppRingLayout {
    pub fn new(
        submission_capacity: usize,
        completion_capacity: usize,
        data_chunk_size: usize,
        data_chunk_count: usize,
    ) -> Self {
        let submission_ring_size = ring_size_for_capacity(submission_capacity);
        let completion_ring_size = ring_size_for_capacity(completion_capacity);
        let fill_ring_size = ring_size_for_capacity(data_chunk_count);
        let submission_ring_bytes = mem::size_of::<LockFreeRingSlot<AppSqeDescriptor>>()
            .checked_mul(submission_ring_size)
            .expect("submission ring layout overflow");
        let completion_ring_bytes = mem::size_of::<LockFreeRingSlot<AppCqeDescriptor>>()
            .checked_mul(completion_ring_size)
            .expect("completion ring layout overflow");
        let fill_ring_bytes = mem::size_of::<LockFreeRingSlot<u32>>()
            .checked_mul(fill_ring_size)
            .expect("fill ring layout overflow");
        let submission_ring_offset = 0;
        let completion_ring_offset =
            align_up(submission_ring_offset + submission_ring_bytes, CACHE_LINE);
        let fill_ring_offset = align_up(completion_ring_offset + completion_ring_bytes, CACHE_LINE);
        let data_area_offset = align_up(fill_ring_offset + fill_ring_bytes, CACHE_LINE);
        Self {
            submission_ring_offset,
            completion_ring_offset,
            fill_ring_offset,
            data_area_offset,
            cacheline_size: CACHE_LINE,
            submission_ring_bytes,
            completion_ring_bytes,
            fill_ring_bytes,
            submission_capacity,
            completion_capacity,
            data_chunk_size,
            data_chunk_count,
        }
    }

    #[inline]
    pub const fn submission_ring_offset(self) -> usize {
        self.submission_ring_offset
    }

    #[inline]
    pub const fn completion_ring_offset(self) -> usize {
        self.completion_ring_offset
    }

    #[inline]
    pub const fn fill_ring_offset(self) -> usize {
        self.fill_ring_offset
    }

    #[inline]
    pub const fn data_area_offset(self) -> usize {
        self.data_area_offset
    }

    #[inline]
    pub const fn cacheline_size(self) -> usize {
        self.cacheline_size
    }

    #[inline]
    pub const fn submission_ring_bytes(self) -> usize {
        self.submission_ring_bytes
    }

    #[inline]
    pub const fn completion_ring_bytes(self) -> usize {
        self.completion_ring_bytes
    }

    #[inline]
    pub const fn fill_ring_bytes(self) -> usize {
        self.fill_ring_bytes
    }

    #[inline]
    pub const fn submission_capacity(self) -> usize {
        self.submission_capacity
    }

    #[inline]
    pub const fn completion_capacity(self) -> usize {
        self.completion_capacity
    }

    #[inline]
    pub const fn data_chunk_size(self) -> usize {
        self.data_chunk_size
    }

    #[inline]
    pub const fn data_chunk_count(self) -> usize {
        self.data_chunk_count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppRingExport {
    memory_kind: AppRingMemoryKind,
    layout: AppRingLayout,
}

impl AppRingExport {
    #[inline]
    pub const fn new(memory_kind: AppRingMemoryKind, layout: AppRingLayout) -> Self {
        Self {
            memory_kind,
            layout,
        }
    }

    #[inline]
    pub const fn memory_kind(self) -> AppRingMemoryKind {
        self.memory_kind
    }

    #[inline]
    pub const fn layout(self) -> AppRingLayout {
        self.layout
    }

    #[inline]
    pub const fn submission_ring_offset(self) -> usize {
        self.layout.submission_ring_offset()
    }

    #[inline]
    pub const fn completion_ring_offset(self) -> usize {
        self.layout.completion_ring_offset()
    }

    #[inline]
    pub const fn fill_ring_offset(self) -> usize {
        self.layout.fill_ring_offset()
    }

    #[inline]
    pub const fn data_area_offset(self) -> usize {
        self.layout.data_area_offset()
    }

    #[inline]
    pub const fn cacheline_size(self) -> usize {
        self.layout.cacheline_size()
    }

    #[inline]
    pub const fn submission_ring_bytes(self) -> usize {
        self.layout.submission_ring_bytes()
    }

    #[inline]
    pub const fn completion_ring_bytes(self) -> usize {
        self.layout.completion_ring_bytes()
    }

    #[inline]
    pub const fn fill_ring_bytes(self) -> usize {
        self.layout.fill_ring_bytes()
    }

    #[inline]
    pub const fn submission_capacity(self) -> usize {
        self.layout.submission_capacity()
    }

    #[inline]
    pub const fn completion_capacity(self) -> usize {
        self.layout.completion_capacity()
    }

    #[inline]
    pub const fn data_chunk_size(self) -> usize {
        self.layout.data_chunk_size()
    }

    #[inline]
    pub const fn data_chunk_count(self) -> usize {
        self.layout.data_chunk_count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppRingIpcReservation {
    page_size: usize,
    producer_consumer_page_count: usize,
    export: AppRingExport,
}

impl AppRingIpcReservation {
    pub fn new(
        page_size: usize,
        producer_consumer_page_count: usize,
        submission_capacity: usize,
        completion_capacity: usize,
        data_chunk_size: usize,
        data_chunk_count: usize,
    ) -> Self {
        let layout = AppRingLayout::new(
            submission_capacity,
            completion_capacity,
            data_chunk_size,
            data_chunk_count,
        );
        Self {
            page_size,
            producer_consumer_page_count,
            export: AppRingExport::new(AppRingMemoryKind::SharedMemory, layout),
        }
    }

    #[inline]
    pub const fn page_size(self) -> usize {
        self.page_size
    }

    #[inline]
    pub const fn producer_consumer_page_count(self) -> usize {
        self.producer_consumer_page_count
    }

    #[inline]
    pub const fn memory_kind(self) -> AppRingMemoryKind {
        self.export.memory_kind()
    }

    #[inline]
    pub const fn export(self) -> AppRingExport {
        self.export
    }
}

#[inline]
pub fn ring_size_for_capacity(capacity: usize) -> usize {
    capacity
        .checked_add(1)
        .and_then(usize::checked_next_power_of_two)
        .expect("app ring size overflow")
}
