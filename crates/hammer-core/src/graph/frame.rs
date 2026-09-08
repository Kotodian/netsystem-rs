use core::{marker::PhantomData, mem, ptr, slice};
use std::alloc::{Layout, alloc, handle_alloc_error};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub const FRAME_VECTOR_CAPACITY: usize = 256;
pub const FRAME_SPECULATIVE_VECTORS: usize = 4;
const FRAME_MAGIC: u32 = 0xabad_c0ed;
const FRAME_HEADER_BYTES: usize = 14;

/// Fixed VPP-style Frame header followed by one initialized argument allocation.
/// Generic arguments select borrows; they occupy no header or argument bytes.
#[derive(Debug)]
#[repr(C, align(64))]
pub struct Frame<Scalar = (), Vector = u32, Aux = ()> {
    pub frame_flags: u16,
    pub flags: u16,
    scalar_offset: u16,
    vector_offset: u16,
    aux_offset: u16,
    n_vectors: u16,
    pub frame_size_index: u16,
    arguments_type: PhantomData<(Scalar, Vector, Aux)>,
    arguments: [u8],
}

impl<S, V, A> Frame<S, V, A>
where
    S: KnownLayout + FromBytes + Immutable + IntoBytes,
    V: KnownLayout + FromBytes + Immutable + IntoBytes,
    A: KnownLayout + FromBytes + Immutable + IntoBytes,
{
    pub const SCALAR_OFFSET: usize = if mem::size_of::<S>() == 0 { 0 } else { 16 };
    pub const VECTOR_OFFSET: usize = 16 + mem::size_of::<S>().next_multiple_of(16);
    pub const MAGIC_OFFSET: usize = Self::VECTOR_OFFSET
        + mem::size_of::<V>() * (FRAME_VECTOR_CAPACITY + FRAME_SPECULATIVE_VECTORS);
    pub const AUX_OFFSET: usize = if mem::size_of::<A>() == 0 {
        0
    } else {
        (Self::MAGIC_OFFSET + 4).next_multiple_of(16)
    };
    pub const ALLOCATION_SIZE: usize = ((Self::MAGIC_OFFSET + 4).next_multiple_of(16)
        + mem::size_of::<A>() * FRAME_VECTOR_CAPACITY)
        .next_multiple_of(64);

    /// Allocate initialized argument storage. Dropping a Frame frees only this
    /// allocation; raw Buffer indices never have a Rust destructor.
    pub fn new(frame_size_index: u16) -> Box<Self> {
        const {
            assert!(
                mem::size_of::<V>() != 0,
                "Frame vector elements have nonzero size"
            );
            assert!(
                mem::align_of::<S>() <= 16
                    && mem::align_of::<V>() <= 16
                    && mem::align_of::<A>() <= 16,
                "Frame arguments fit VPP data alignment"
            );
            assert!(
                Self::ALLOCATION_SIZE <= u16::MAX as usize,
                "Frame allocation fits its registered size"
            );
        }
        let mut frame = Frame::allocate_storage(Self::ALLOCATION_SIZE, frame_size_index);
        frame.install_layout(
            Self::SCALAR_OFFSET,
            Self::VECTOR_OFFSET,
            Self::MAGIC_OFFSET,
            Self::AUX_OFFSET,
            frame_size_index,
        );
        // SAFETY: generic markers have no storage. The common C header and
        // trailing slice metadata are identical for every Frame instantiation;
        // the allocation was initialized with precisely S/V/A's layout above.
        unsafe { Box::from_raw(Box::into_raw(frame) as *mut Self) }
    }

    /// Check the registered argument layout before a generated Node trampoline
    /// constructs its typed borrow. This exposes no erased pointer or cast.
    #[doc(hidden)]
    pub fn validate_layout<T, W, B>(&self)
    where
        T: KnownLayout + FromBytes + Immutable + IntoBytes,
        W: KnownLayout + FromBytes + Immutable + IntoBytes,
        B: KnownLayout + FromBytes + Immutable + IntoBytes,
    {
        const {
            assert!(mem::size_of::<W>() != 0);
            assert!(
                mem::align_of::<T>() <= 16
                    && mem::align_of::<W>() <= 16
                    && mem::align_of::<B>() <= 16
            );
        }
        assert_eq!(self.scalar_offset as usize, Frame::<T, W, B>::SCALAR_OFFSET);
        assert_eq!(self.vector_offset as usize, Frame::<T, W, B>::VECTOR_OFFSET);
        assert_eq!(self.aux_offset as usize, Frame::<T, W, B>::AUX_OFFSET);
        assert_eq!(mem::size_of_val(self), Frame::<T, W, B>::ALLOCATION_SIZE);
        assert!(
            self.len() <= FRAME_VECTOR_CAPACITY,
            "Frame vector count fits capacity"
        );
        if cfg!(debug_assertions) {
            // SAFETY: matching registered offsets and total extent contain magic;
            // the byte address need not have u32 alignment for small vectors.
            let magic = unsafe {
                (ptr::from_ref(self) as *const u8)
                    .add(Frame::<T, W, B>::MAGIC_OFFSET)
                    .cast::<u32>()
                    .read_unaligned()
            };
            assert_eq!(
                magic, FRAME_MAGIC,
                "Frame speculative-vector boundary is intact"
            );
        }
    }

    pub fn len(&self) -> usize {
        usize::from(self.n_vectors)
    }
    pub fn is_empty(&self) -> bool {
        self.n_vectors == 0
    }
    pub fn set_vector_count(&mut self, count: usize) {
        assert!(
            count <= FRAME_VECTOR_CAPACITY,
            "Frame vector count fits capacity"
        );
        self.n_vectors = count as u16;
    }

    pub fn scalar_args(&self) -> Option<&S> {
        self.validate_layout::<S, V, A>();
        if self.scalar_offset == 0 {
            return None;
        }
        // SAFETY: allocation initializes all bytes; the registered aligned
        // scalar region contains S, whose traits permit every initialized bit pattern.
        Some(unsafe {
            &*(ptr::from_ref(self) as *const u8)
                .add(self.scalar_offset as usize)
                .cast::<S>()
        })
    }
    pub fn scalar_args_mut(&mut self) -> Option<&mut S> {
        self.validate_layout::<S, V, A>();
        if self.scalar_offset == 0 {
            return None;
        }
        // SAFETY: identical scalar layout proof, with the exclusive Frame borrow.
        Some(unsafe {
            &mut *(ptr::from_mut(self) as *mut u8)
                .add(self.scalar_offset as usize)
                .cast::<S>()
        })
    }
    pub fn vector_args(&self) -> &[V] {
        self.validate_layout::<S, V, A>();
        // SAFETY: n_vectors is bounded by capacity; initialized vectors have
        // registered size/alignment and share the Frame borrow's lifetime.
        unsafe {
            slice::from_raw_parts(
                (ptr::from_ref(self) as *const u8)
                    .add(self.vector_offset as usize)
                    .cast::<V>(),
                self.len(),
            )
        }
    }
    pub fn vector_args_mut(&mut self) -> &mut [V] {
        self.validate_layout::<S, V, A>();
        // SAFETY: the vector region is initialized and exclusively borrowed.
        unsafe {
            slice::from_raw_parts_mut(
                (ptr::from_mut(self) as *mut u8)
                    .add(self.vector_offset as usize)
                    .cast::<V>(),
                self.len(),
            )
        }
    }
    pub fn aux_args(&self) -> Option<&[A]> {
        self.validate_layout::<S, V, A>();
        if self.aux_offset == 0 {
            return None;
        }
        // SAFETY: the registered auxiliary allocation has one initialized A per
        // vector, with n_vectors bounded by 256.
        Some(unsafe {
            slice::from_raw_parts(
                (ptr::from_ref(self) as *const u8)
                    .add(self.aux_offset as usize)
                    .cast::<A>(),
                self.len(),
            )
        })
    }
    pub fn aux_args_mut(&mut self) -> Option<&mut [A]> {
        self.validate_layout::<S, V, A>();
        if self.aux_offset == 0 {
            return None;
        }
        // SAFETY: the initialized auxiliary region is exclusively borrowed.
        Some(unsafe {
            slice::from_raw_parts_mut(
                (ptr::from_mut(self) as *mut u8)
                    .add(self.aux_offset as usize)
                    .cast::<A>(),
                self.len(),
            )
        })
    }
}

impl Frame {
    /// Initialized Next Frame suffix. Runtime validates the destination's
    /// vector/auxiliary sizes; core verifies the complete stored layout before
    /// constructing the two disjoint borrows.
    #[doc(hidden)]
    pub fn next_args_mut<V, A>(&mut self, scalar_size: u16) -> (&mut [V], Option<&mut [A]>)
    where
        V: KnownLayout + FromBytes + Immutable + IntoBytes,
        A: KnownLayout + FromBytes + Immutable + IntoBytes,
    {
        const {
            assert!(mem::size_of::<V>() > 0);
            assert!(mem::align_of::<V>() <= 16 && mem::align_of::<A>() <= 16);
        }
        let vector_size = u16::try_from(mem::size_of::<V>()).expect("Frame vector size fits u16");
        let aux_size = u16::try_from(mem::size_of::<A>()).expect("Frame auxiliary size fits u16");
        let (scalar, vector, magic, aux, bytes) = Self::layout(scalar_size, vector_size, aux_size);
        assert_eq!(self.scalar_offset as usize, scalar);
        assert_eq!(self.vector_offset as usize, vector);
        assert_eq!(self.aux_offset as usize, aux);
        assert_eq!(mem::size_of_val(self), bytes);
        let count = self.len();
        assert!(count <= FRAME_VECTOR_CAPACITY);
        let start = ptr::from_mut(self) as *mut u8;
        // SAFETY: checked offsets and extent contain aligned, initialized
        // vector and auxiliary arrays. The arrays occupy disjoint regions;
        // their suffixes share only this exclusive Frame borrow's lifetime.
        unsafe {
            if cfg!(debug_assertions) {
                assert_eq!(start.add(magic).cast::<u32>().read_unaligned(), FRAME_MAGIC);
            }
            let vectors = slice::from_raw_parts_mut(
                start.add(vector).cast::<V>().add(count),
                FRAME_VECTOR_CAPACITY - count,
            );
            let auxiliary = if aux_size == 0 {
                None
            } else {
                Some(slice::from_raw_parts_mut(
                    start.add(aux).cast::<A>().add(count),
                    FRAME_VECTOR_CAPACITY - count,
                ))
            };
            (vectors, auxiliary)
        }
    }

    pub(crate) fn layout(
        scalar_size: u16,
        vector_size: u16,
        aux_size: u16,
    ) -> (usize, usize, usize, usize, usize) {
        assert_ne!(vector_size, 0, "Frame vector elements have nonzero size");
        let scalar = if scalar_size == 0 { 0 } else { 16 };
        let vector = 16 + usize::from(scalar_size).next_multiple_of(16);
        let magic =
            vector + usize::from(vector_size) * (FRAME_VECTOR_CAPACITY + FRAME_SPECULATIVE_VECTORS);
        let after_magic = (magic + 4).next_multiple_of(16);
        let aux = if aux_size == 0 { 0 } else { after_magic };
        let bytes =
            (after_magic + usize::from(aux_size) * FRAME_VECTOR_CAPACITY).next_multiple_of(64);
        assert!(
            bytes <= u16::MAX as usize,
            "Frame allocation fits registered byte size"
        );
        (scalar, vector, magic, aux, bytes)
    }

    pub(crate) fn allocate_storage(bytes: usize, frame_size_index: u16) -> Box<Self> {
        let layout = Layout::from_size_align(bytes, 64).expect("Frame allocation is aligned");
        assert!(bytes >= FRAME_HEADER_BYTES);
        // SAFETY: trailing slice metadata excludes the 14-byte C header, so
        // Box's padded layout matches the allocation. All bytes, including the
        // argument regions, are initialized before a Frame reference is formed.
        unsafe {
            let allocation = alloc(layout);
            if allocation.is_null() {
                handle_alloc_error(layout);
            }
            allocation.write_bytes(if cfg!(debug_assertions) { 0xfe } else { 0 }, bytes);
            allocation.write_bytes(0, FRAME_HEADER_BYTES);
            let storage = ptr::slice_from_raw_parts_mut(allocation, bytes - FRAME_HEADER_BYTES);
            let frame = storage as *mut Self;
            ptr::addr_of_mut!((*frame).frame_size_index).write(frame_size_index);
            Box::from_raw(frame)
        }
    }

    pub(crate) fn install_layout(
        &mut self,
        scalar: usize,
        vector: usize,
        magic: usize,
        aux: usize,
        size_index: u16,
    ) {
        assert!(magic + 4 <= mem::size_of_val(self));
        if cfg!(debug_assertions) {
            // SAFETY: the exclusive Frame borrow covers its whole allocation;
            // header fields and argument bytes accept every bit pattern. VPP's
            // vlib_frame_alloc_to_node poisons recycled as well as fresh Frames.
            unsafe {
                (ptr::from_mut(self) as *mut u8).write_bytes(0xfe, mem::size_of_val(self));
            }
        }
        self.scalar_offset = scalar as u16;
        self.vector_offset = vector as u16;
        self.aux_offset = aux as u16;
        self.frame_size_index = size_index;
        self.n_vectors = 0;
        self.flags = 0;
        self.frame_flags = 1 << 1;
        // SAFETY: layout calculation checked the allocation extent; magic may
        // have byte alignment for small vector elements, hence write_unaligned.
        unsafe {
            (ptr::from_mut(self) as *mut u8)
                .add(magic)
                .cast::<u32>()
                .write_unaligned(FRAME_MAGIC);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Layout arithmetic comes from vlib/node.c register_node; initialized
    // argument access follows vlib/main.c vlib_frame_alloc_to_node.
    #[test]
    fn fixed_frame_offsets_and_typed_arguments_match_registration() {
        hammer_infra::main_heap::init_default().unwrap();
        let mut frame = Frame::<u64, u32, u16>::new(7);
        frame.validate_layout::<u64, u32, u16>();
        assert_eq!(Frame::<u64, u32, u16>::SCALAR_OFFSET, 16);
        assert_eq!(Frame::<u64, u32, u16>::VECTOR_OFFSET, 32);
        assert_eq!(Frame::<u64, u32, u16>::MAGIC_OFFSET, 1072);
        assert_eq!(Frame::<u64, u32, u16>::AUX_OFFSET, 1088);
        assert_eq!(mem::size_of_val(&*frame), 1600);
        assert_eq!(ptr::from_ref(&*frame).addr() % 64, 0);
        assert_eq!(frame.frame_size_index, 7);
        *frame.scalar_args_mut().unwrap() = 9;
        frame.set_vector_count(256);
        frame.vector_args_mut().fill(17);
        frame.aux_args_mut().unwrap().fill(23);
        assert_eq!(frame.scalar_args(), Some(&9));
        assert_eq!(frame.vector_args(), &[17; 256]);
        assert_eq!(frame.aux_args(), Some([23; 256].as_slice()));
        frame.validate_layout::<u64, u32, u16>();
        let packet = Frame::<(), u32, ()>::new(0);
        assert!(packet.scalar_args().is_none());
        assert!(packet.aux_args().is_none());
        assert_eq!(Frame::<(), u32, ()>::VECTOR_OFFSET, 16);
        assert_eq!(mem::size_of_val(&*packet), 1088);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameBatchWidth {
    Pair,
    Quad,
    Octo,
}
