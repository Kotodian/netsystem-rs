use super::*;

impl DataPlaneMain {
    pub fn new(config: DataPlaneBufferConfig) -> Self {
        Self::try_new(config).expect("create ordinary-page data-plane runtime")
    }

    #[inline]
    pub fn try_new(config: DataPlaneBufferConfig) -> RuntimeResult<Self> {
        Self::from_buffers(config.try_into()?, native_simd_bytes())
    }

    #[inline]
    pub(crate) fn from_buffers(
        buffers: DataPlaneBuffers,
        simd_bytes: usize,
    ) -> RuntimeResult<Self> {
        // Like vlib_main's clock seed, this is not cryptographic entropy.
        // Seed once at runtime construction, never on the packet path.
        let elapsed = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(elapsed) => elapsed,
            Err(error) => error.duration(),
        };
        let seed = elapsed.as_nanos() as u64 ^ u64::from(buffers.thread_index());
        Ok(Self {
            random: Rc::new(RefCell::new(SmallRng::seed_from_u64(seed))),
            active_numa_node: buffers.active_numa_node(),
            buffers,
            nodes: NodeMain::default(),
            current_node: Rc::new(Cell::new(None)),
            handoff: None,
            trace: DataPlaneTrace::default(),
            simd_bytes,
            registry: crate::RuntimeRegistry::new(),
            main_loop_exit_now: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            main_loop_exit_status: std::sync::Arc::new(std::sync::Mutex::new(0)),
            publication: std::sync::Arc::new(crate::global_main::WorkerPublication::new()),
            workers_updating_graph: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
            worker_config: Worker::default(),
            worker_exit_functions: Vec::new(),
            called_worker_init_functions: std::collections::HashSet::new(),
            main_loop_count: std::sync::atomic::AtomicU32::new(0),
            worker_control_queues: std::sync::Arc::from([]),
        })
    }

    #[inline]
    pub fn buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    /// VPP-style runtime thread index: zero for main, one-based for workers.
    #[inline]
    pub fn thread_index(&self) -> u32 {
        self.buffers.thread_index()
    }

    #[inline]
    pub fn alloc_index(&self) -> RuntimeResult<u32> {
        Ok(self.buffers.alloc_index()?)
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> RuntimeResult<u32> {
        Ok(self.buffers.alloc_index_with_bytes(bytes)?)
    }

    #[inline]
    pub(crate) fn drop_index_owned(&self, index: u32) {
        self.buffers
            .drop_index_owned_with_trace(index, |handle| self.trace.finalize(handle));
    }

    #[inline]
    pub fn prefetch_header(&self, index: u32) {
        self.buffers.prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: u32) {
        self.buffers.prefetch_read(index);
    }

    #[inline]
    pub fn prefetch_write(&self, index: u32) {
        self.buffers.prefetch_write(index);
    }

    #[inline]
    pub fn chain(
        &self,
        index: u32,
    ) -> impl Iterator<Item = Result<BufferRef<'_>, DataPlaneError>> + '_ {
        self.buffers.chain(index)
    }

    #[inline]
    pub fn current_config_index(&self, index: u32) -> RuntimeResult<u32> {
        Ok(self.buffers.current_config_index(index)?)
    }

    #[inline]
    #[inline]
    pub fn buffer(&self, index: u32) -> &hammer_core::data_plane::Buffer {
        // SAFETY: the graph owns this live index on the calling Worker. The
        // returned borrow is bounded by this runtime borrow, not BufferMain's
        // process lifetime. Shared chain tails remain immutable.
        unsafe { hammer_core::buffer::BufferMain::global().buffer(index) }
    }

    #[inline]
    pub fn buffer_mut(&mut self, index: u32) -> &mut hammer_core::data_plane::Buffer {
        // SAFETY: Node invocation borrows its Worker runtime exclusively. The
        // graph owns the index throughout this borrow; core rejects shared
        // tails. The borrow ends before the Node transfers the index onward.
        unsafe { hammer_core::buffer::BufferMain::global().buffer_mut(index) }
    }

    #[inline]
    pub fn nodes(&self) -> &NodeMain {
        &self.nodes
    }

    pub fn file_main(&self) -> &'static FileMain {
        FILE_MAIN
            .get()
            .expect("FileMain is initialized before data-plane use")
    }
}

impl DataPlaneMain {
    pub fn buffer_alloc(&mut self, indices: &mut [u32]) -> usize {
        self.buffer_alloc_on_numa(indices, self.active_numa_node)
    }

    pub fn buffer_alloc_from_pool(&mut self, indices: &mut [u32], pool_index: u8) -> usize {
        hammer_core::buffer::BufferMain::global().alloc_from_pool(
            self.thread_index(),
            indices,
            pool_index,
        )
    }

    pub fn buffer_alloc_on_numa(&mut self, indices: &mut [u32], numa_node: u32) -> usize {
        let pool = hammer_core::buffer::BufferMain::global().default_pool(numa_node);
        self.buffer_alloc_from_pool(indices, pool)
    }

    pub fn buffer_alloc_to_ring(&mut self, ring: &mut [u32], start: usize, count: usize) -> usize {
        let pool = hammer_core::buffer::BufferMain::global().default_pool(self.active_numa_node);
        self.buffer_alloc_to_ring_from_pool(ring, start, count, pool)
    }

    pub fn buffer_alloc_to_ring_from_pool(
        &mut self,
        ring: &mut [u32],
        start: usize,
        count: usize,
        pool_index: u8,
    ) -> usize {
        assert!(
            count <= ring.len() && (start < ring.len() || (start == 0 && ring.is_empty())),
            "valid Buffer allocation ring range"
        );
        let before_wrap = count.min(ring.len() - start);
        let allocated =
            self.buffer_alloc_from_pool(&mut ring[start..start + before_wrap], pool_index);
        if allocated != before_wrap {
            return allocated;
        }
        allocated + self.buffer_alloc_from_pool(&mut ring[..count - before_wrap], pool_index)
    }

    pub fn buffer_chain_init(&mut self, first: u32) {
        hammer_core::buffer::BufferMain::global().chain_init(first);
    }

    pub fn buffer_chain_buffer(&mut self, last: u32, next: u32) -> u32 {
        hammer_core::buffer::BufferMain::global().chain_link(last, next)
    }

    pub fn buffer_chain_append_data(&mut self, first: u32, last: u32, data: &[u8]) -> usize {
        hammer_core::buffer::BufferMain::global().chain_append(first, last, data)
    }

    pub fn buffer_chain_append_data_with_alloc(
        &mut self,
        first: u32,
        last: &mut u32,
        data: &[u8],
    ) -> usize {
        hammer_core::buffer::BufferMain::global().chain_append_with_alloc(
            self.thread_index(),
            first,
            last,
            data,
        )
    }

    pub fn buffer_add_data(&mut self, head: &mut u32, data: &[u8]) -> usize {
        hammer_core::buffer::BufferMain::global().add_data(
            self.thread_index(),
            self.active_numa_node,
            head,
            data,
        )
    }

    pub fn buffer_attach_clone(&mut self, head: u32, tail: u32) {
        hammer_core::buffer::BufferMain::global().attach_clone(head, tail);
    }

    pub fn buffer_free(&mut self, indices: &[u32]) {
        hammer_core::buffer::BufferMain::global().free_buffers(
            self.thread_index(),
            indices,
            true,
            |handle| self.trace.finalize(handle),
        );
    }

    pub fn buffer_free_no_next(&mut self, indices: &[u32]) {
        hammer_core::buffer::BufferMain::global().free_buffers(
            self.thread_index(),
            indices,
            false,
            |handle| self.trace.finalize(handle),
        );
    }

    pub fn buffer_free_one(&mut self, index: u32) {
        self.buffer_free(core::slice::from_ref(&index));
    }

    pub fn buffer_free_from_ring(&mut self, ring: &[u32], start: usize, count: usize) {
        assert!(
            count <= ring.len() && (start < ring.len() || (start == 0 && ring.is_empty())),
            "valid Buffer release ring range"
        );
        let before_wrap = count.min(ring.len() - start);
        self.buffer_free(&ring[start..start + before_wrap]);
        self.buffer_free(&ring[..count - before_wrap]);
    }

    pub fn buffer_free_from_ring_no_next(&mut self, ring: &[u32], start: usize, count: usize) {
        assert!(
            count <= ring.len() && (start < ring.len() || (start == 0 && ring.is_empty())),
            "valid Buffer release ring range"
        );
        let before_wrap = count.min(ring.len() - start);
        self.buffer_free_no_next(&ring[start..start + before_wrap]);
        self.buffer_free_no_next(&ring[..count - before_wrap]);
    }
}

#[cfg(test)]
mod buffer_tests {
    use super::*;

    // Derived from buffer_funcs.h alloc/free-to-ring wrapping and vlib_test.c's
    // full-head chain_append_data_with_alloc scenario. No daemon or lab is run.
    #[test]
    fn ring_ranges_and_chain_release_follow_segment_obligations() {
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_infra::main_heap::init_default().unwrap();
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                2,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut runtime = DataPlaneMain::new(DataPlaneBufferConfig::default());
        let mut ring = [u32::MAX; 6];
        assert_eq!(runtime.buffer_alloc_to_ring(&mut ring, 4, 4), 4);
        assert_eq!(&ring[2..4], &[u32::MAX; 2]);
        for position in [4, 5, 0, 1] {
            assert_eq!(runtime.buffer(ring[position]).ref_count(), 1);
        }
        runtime.buffer_free_from_ring(&ring, 4, 4);
        assert_eq!(
            runtime.buffer_alloc_to_ring_from_pool(&mut ring, 5, 3, 0),
            3
        );
        runtime.buffer_free_from_ring_no_next(&ring, 5, 3);

        let mut first = [0];
        assert_eq!(runtime.buffer_alloc_on_numa(&mut first, 0), 1);
        let first = first[0];
        runtime.buffer_chain_init(first);
        assert_eq!(
            runtime.buffer_chain_append_data(first, first, &[0x45; 64]),
            64
        );
        let mut last = first;
        assert_eq!(
            runtime.buffer_chain_append_data_with_alloc(first, &mut last, &[1, 2, 3, 4]),
            4
        );
        assert_ne!(last, first);
        assert_eq!(runtime.buffer(first).next_buffer_slot(), Some(last));
        assert_eq!(runtime.buffer(first).total_len_not_including_first(), 4);
        runtime.buffer_free_no_next(&[first]);
        assert_eq!(runtime.buffer(last).ref_count(), 1);
        assert_eq!(runtime.buffer(last).current(), &[1, 2, 3, 4]);
        runtime.buffer_free_one(last);
        assert_eq!(runtime.buffer_alloc(&mut []), 0);
        runtime.buffer_free(&[]);
    }
}
