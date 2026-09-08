use super::*;

impl DataPlaneBuffers {
    pub fn attach_clone(&self, head: u32, tail: u32) -> DataPlaneResult<()> {
        BufferMain::global().attach_clone(head, tail);
        Ok(())
    }
}
