use crate::graph::frame::Frame;

/// Worker-owned Frame allocation classes, indexed by the header's size index.
#[derive(Debug, Default)]
pub struct FramePool {
    sizes: Vec<FrameSize>,
    in_use: usize,
}

#[derive(Debug)]
struct FrameSize {
    frame_size: usize,
    free_frames: Vec<Box<Frame>>,
}

impl FramePool {
    pub fn in_use(&self) -> usize {
        self.in_use
    }

    /// VPP register_node groups allocations by their rounded byte size, even
    /// when two nodes interpret their scalar/vector/aux regions differently.
    pub fn allocate(&mut self, scalar_size: u16, vector_size: u16, aux_size: u16) -> Box<Frame> {
        let (scalar, vector, magic, aux, bytes) = Frame::layout(scalar_size, vector_size, aux_size);
        let size_index = match self.sizes.iter().position(|size| size.frame_size == bytes) {
            Some(index) => index,
            None => {
                let index = self.sizes.len();
                assert!(index < u16::MAX as usize, "Frame size index fits u16");
                self.sizes.push(FrameSize {
                    frame_size: bytes,
                    free_frames: Vec::new(),
                });
                index
            }
        };
        let mut frame = self.sizes[size_index]
            .free_frames
            .pop()
            .unwrap_or_else(|| Frame::allocate_storage(bytes, size_index as u16));
        frame.install_layout(scalar, vector, magic, aux, size_index as u16);
        self.in_use += 1;
        frame
    }

    /// Recycle only Frame memory. Buffer release belongs to packet/domain owners.
    pub fn recycle(&mut self, mut frame: Box<Frame>) {
        let size = &mut self.sizes[usize::from(frame.frame_size_index)];
        assert_eq!(
            core::mem::size_of_val(&*frame),
            size.frame_size,
            "Frame returns through its size class"
        );
        assert!(
            self.in_use != 0,
            "Frame allocation has an outstanding pool obligation"
        );
        frame.set_vector_count(0);
        frame.frame_flags = 0;
        size.free_frames.push(frame);
        self.in_use -= 1;
    }
}
