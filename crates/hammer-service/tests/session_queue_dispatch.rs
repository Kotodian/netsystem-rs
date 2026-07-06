use std::sync::Arc;
use std::time::Instant;

use hammer_adapter::{BufferIndex, DataPlaneRuntime, DataWorkerId, NodeId};
use hammer_core::error::CoreResult;
use hammer_infra::segment::Local;
use hammer_runtime::app::{AppSession, AppSessionConfig, SessionHandle};
use hammer_service::session::SessionQueueNext;
use hammer_service::session::protocol::SessionQueueControlContext;
use hammer_service::session::runtime::{
    SessionDriverRuntime, SessionQueueProtocol, dispatch_session_queue_for_ticks,
};

#[derive(Default)]
struct TestTxProtocol {
    offset: usize,
    prepared: usize,
    committed: usize,
}

impl SessionQueueProtocol for TestTxProtocol {
    fn handle_expired_timer(
        &mut self,
        _: &DataPlaneRuntime,
        _: &mut SessionQueueControlContext,
        _: u32,
        _: SessionQueueNext,
        _: &mut hammer_service::session::node::SessionQueueOutput,
    ) -> CoreResult<bool> {
        Ok(false)
    }

    fn handle_ready_session(
        &mut self,
        _: &DataPlaneRuntime,
        _: &mut SessionQueueControlContext,
        _: bool,
        _: SessionQueueNext,
        _: &mut hammer_service::session::node::SessionQueueOutput,
    ) -> CoreResult<bool> {
        Ok(false)
    }

    fn tx_offset(&self, _: &SessionQueueControlContext) -> CoreResult<usize> {
        Ok(self.offset)
    }

    fn tx_payload_len(
        &mut self,
        _: &mut SessionQueueControlContext,
        _: usize,
        pending_len: usize,
        _: Instant,
    ) -> CoreResult<usize> {
        Ok(pending_len.min(4))
    }

    fn prepare_tx(
        &mut self,
        _: &mut SessionQueueControlContext,
        _: BufferIndex,
        _: usize,
        payload_len: usize,
        _: Instant,
    ) -> CoreResult<()> {
        self.prepared += payload_len;
        Ok(())
    }

    fn cancel_tx(&mut self, _: &mut SessionQueueControlContext, _: BufferIndex) {}

    fn commit_tx(
        &mut self,
        _: &mut SessionQueueControlContext,
        _: BufferIndex,
        tx_offset: usize,
        payload_len: usize,
        _: Instant,
    ) -> CoreResult<()> {
        self.committed += payload_len;
        self.offset = tx_offset + payload_len;
        Ok(())
    }

    fn on_close(&mut self, _: &mut SessionQueueControlContext) {}
}

#[test]
fn session_tx_dispatch_sends_multiple_segments_up_to_budget() {
    // frame_capacity must be >= DEFAULT_TX_DISPATCH_BUDGET (64) so that
    // output.schedule can push all indices into one frame.
    let runtime = DataPlaneRuntime::with_capacities(2048, 64, 64, 8);
    let buffers = runtime.buffers();
    let mut driver =
        SessionDriverRuntime::<TestTxProtocol, Local>::new(DataWorkerId::new(0), buffers.clone());
    let session_id = driver.insert_session(TestTxProtocol::default());

    let app_session = Arc::new(
        AppSession::<Local>::new_in_segment(
            Local::default(),
            AppSessionConfig::new(256, 64),
            SessionHandle::new(session_id.pool_index().slot() as u32, 0),
            driver.app().tx_evt_q().clone(),
        )
        .expect("create app session"),
    );

    let tx_data = [0xABu8; 100];
    app_session.send_bytes(&tx_data).expect("send bytes");

    driver.app_mut().attach_session(session_id, app_session);
    driver.mark_ready(session_id);

    let next: SessionQueueNext = NodeId::new(9).into();
    dispatch_session_queue_for_ticks(&runtime, &mut driver, 0, next)
        .expect("dispatch session queue");

    let protocol = driver.session(session_id).expect("protocol state");
    assert_eq!(
        protocol.prepared, 100,
        "expected 25 segments (4 bytes each) to be prepared"
    );
    assert_eq!(
        protocol.committed, 100,
        "expected 25 segments (4 bytes each) to be committed"
    );
}
