use std::sync::Arc;
use std::time::Instant;

use hammer_adapter::{DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId};
use hammer_core::error::CoreResult;
use hammer_infra::segment::Local;
use hammer_runtime::app::{AppSession, AppSessionConfig, SessionHandle};
use hammer_service::data_plane::DropNode;
use hammer_service::session::SessionQueueNext;
use hammer_service::session::protocol::SessionQueueControlContext;
use hammer_service::session::runtime::{
    SessionDriverRuntime, SessionQueueProtocol, TransportSendFlags, TransportSendParams,
    TxBatchBuffer, dispatch_session_queue_for_ticks,
};

fn test_runtime_configured(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_capacity: usize,
    frame_slots: usize,
) -> DataPlaneRuntime {
    let config = DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity,
            buffer_slots,
            frame_capacity,
            frame_slots,
            ..DataPlaneBufferConfig::default()
        },
    };
    DataPlaneRuntime::new(config)
}

#[derive(Default)]
struct TestTxProtocol {
    offset: usize,
    send_params_calls: usize,
    push_header_calls: usize,
    pushed_batches: std::vec::Vec<std::vec::Vec<(usize, usize)>>,
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

    fn send_params(
        &mut self,
        _: &mut SessionQueueControlContext,
        pending_len: usize,
        _: Instant,
    ) -> CoreResult<TransportSendParams> {
        self.send_params_calls += 1;
        Ok(TransportSendParams {
            snd_space: pending_len,
            tx_offset: self.offset,
            send_goal_size: 4,
            flags: TransportSendFlags::default(),
        })
    }

    fn push_header(
        &mut self,
        _: &mut SessionQueueControlContext,
        batch: &[TxBatchBuffer],
        _: Instant,
    ) -> CoreResult<()> {
        self.push_header_calls += 1;
        self.pushed_batches.push(
            batch
                .iter()
                .map(|entry| (entry.tx_offset, entry.payload_len))
                .collect(),
        );
        self.offset = batch
            .last()
            .map(|entry| entry.tx_offset + entry.payload_len)
            .unwrap_or(self.offset);
        Ok(())
    }

    fn custom_tx(
        &mut self,
        _: &DataPlaneRuntime,
        _: &mut SessionQueueControlContext,
        _: SessionQueueNext,
        _: &mut hammer_service::session::node::SessionQueueOutput,
        _: usize,
        _: Instant,
    ) -> CoreResult<usize> {
        Ok(0)
    }

    fn on_close(&mut self, _: &mut SessionQueueControlContext) {}
}

#[test]
fn session_tx_dispatch_commits_batch_before_graph_visibility() {
    // frame_capacity must be >= DEFAULT_TX_DISPATCH_BUDGET (64) so that
    // output.schedule can push all indices into one frame.
    let runtime = test_runtime_configured(2048, 64, 64, 8);
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

    let tx_data = [0xABu8; 16];
    app_session.send_bytes(&tx_data).expect("send bytes");

    driver.app_mut().attach_session(session_id, app_session);
    driver.mark_ready(session_id);

    let next: SessionQueueNext = runtime.nodes().register_internal(DropNode::new()).into();
    dispatch_session_queue_for_ticks(&runtime, &mut driver, 0, next)
        .expect("dispatch session queue");

    let protocol = driver.session(session_id).expect("protocol state");
    assert_eq!(protocol.send_params_calls, 1);
    assert_eq!(protocol.push_header_calls, 1);
    assert_eq!(
        protocol.pushed_batches,
        vec![vec![(0, 4), (4, 4), (8, 4), (12, 4)]]
    );
}
