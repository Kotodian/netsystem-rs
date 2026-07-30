use std::sync::Arc;

use hammer_runtime::app::{AppSession, AppSessionProtocolConnectionId, AppSessionProtocolEntry};
use hammer_runtime::{DataWorkerId, RuntimeResult};

pub(crate) struct AppSessionLayer {
    session: Arc<AppSession>,
    protocol: AppSessionProtocolEntry,
    connection: Option<AppSessionProtocolConnectionId>,
}

impl AppSessionLayer {
    pub(crate) const fn new(
        session: Arc<AppSession>,
        protocol: AppSessionProtocolEntry,
        connection: AppSessionProtocolConnectionId,
    ) -> Self {
        Self {
            session,
            protocol,
            connection: Some(connection),
        }
    }
}

/// One worker-owned App Session protocol stack selected before construction.
pub(crate) struct AppSessionStack {
    worker: DataWorkerId,
    transport_session: Arc<AppSession>,
    layers: Box<[AppSessionLayer]>,
}

impl AppSessionStack {
    pub(crate) fn new(worker: DataWorkerId, transport_session: Arc<AppSession>) -> Self {
        Self {
            worker,
            transport_session,
            layers: Box::new([]),
        }
    }

    pub(crate) fn push(&mut self, layer: AppSessionLayer) {
        let mut layers = std::mem::take(&mut self.layers).into_vec();
        layers.push(layer);
        self.layers = layers.into_boxed_slice();
    }

    #[inline]
    pub(crate) fn transport_session(&self) -> &Arc<AppSession> {
        &self.transport_session
    }

    pub(crate) fn ingress(&mut self) -> RuntimeResult<()> {
        loop {
            let mut progressed = false;
            for index in 0..self.layers.len() {
                let (lower, current) = self.layers.split_at_mut(index);
                let current = &mut current[0];
                let source = lower
                    .last()
                    .map_or(&self.transport_session, |layer| &layer.session);
                let connection = current
                    .connection
                    .expect("live App Session layer retains its protocol connection");
                let (source_consumed, destination_produced) = current.protocol.ingress(
                    self.worker,
                    connection,
                    source.rx_fifo(),
                    current.session.rx_fifo(),
                )?;
                source.publish_rx_dequeue(source_consumed);
                current.session.publish_rx_enqueue(destination_produced)?;
                progressed |= source_consumed != 0 || destination_produced != 0;
            }
            if !progressed {
                return Ok(());
            }
        }
    }

    pub(crate) fn egress(&mut self) -> RuntimeResult<()> {
        loop {
            let mut progressed = false;
            for index in (0..self.layers.len()).rev() {
                let (lower, current) = self.layers.split_at_mut(index);
                let current = &mut current[0];
                let destination = lower
                    .last()
                    .map_or(&self.transport_session, |layer| &layer.session);
                let connection = current
                    .connection
                    .expect("live App Session layer retains its protocol connection");
                let (source_consumed, destination_produced) = current.protocol.egress(
                    self.worker,
                    connection,
                    current.session.tx_fifo(),
                    destination.tx_fifo(),
                )?;
                current.session.publish_tx_dequeue(source_consumed)?;
                destination.publish_tx_enqueue(destination_produced)?;
                progressed |= source_consumed != 0 || destination_produced != 0;
            }
            if !progressed {
                return Ok(());
            }
        }
    }

    pub(crate) fn app_session(&self) -> &Arc<AppSession> {
        &self
            .layers
            .last()
            .expect("App Session Stack requires one protocol layer")
            .session
    }

    pub(crate) fn destroy(&mut self) -> RuntimeResult<()> {
        let mut primary_error = None;
        for layer in self.layers.iter_mut().rev() {
            let Some(connection) = layer.connection.take() else {
                continue;
            };
            if let Err(error) = layer.protocol.destroy(self.worker, connection) {
                if primary_error.is_none() {
                    primary_error = Some(error);
                } else {
                    tracing::error!(%error, "additional App Session protocol cleanup failed");
                }
            }
        }
        match primary_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for AppSessionStack {
    fn drop(&mut self) {
        if let Err(error) = self.destroy() {
            tracing::error!(%error, "App Session Stack cleanup failed");
        }
    }
}

impl std::fmt::Debug for AppSessionStack {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppSessionStack")
            .field("worker", &self.worker)
            .field("layers", &self.layers.len())
            .finish_non_exhaustive()
    }
}
