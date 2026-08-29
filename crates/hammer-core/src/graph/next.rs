pub trait NodeNext: Copy + Eq {
    fn slot(self) -> u16;
}

impl NodeNext for u16 {
    #[inline(always)]
    fn slot(self) -> u16 {
        self
    }
}
