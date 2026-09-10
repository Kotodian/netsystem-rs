//! VPP-style Process Nodes owned by the thread-zero [`DataPlaneMain`].
//!
//! A Process callback borrows its owner only while constructing its concrete
//! future. Tokio then polls that future without retaining a `DataPlaneMain`
//! borrow across suspension. Runtime identity and scheduling metadata remain
//! in the owning [`NodeMain`].

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use hammer_core::data_plane::{NodeId, NodeKind};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::DataPlaneMain;
use crate::error::{RuntimeError, RuntimeResult};
use crate::node::{NodeEntry, NodeMain};

/// One concrete Process execution value scheduled by thread-zero Tokio.
pub struct Process<S, F>
where
    F: Future<Output = RuntimeResult<()>>,
{
    node_index: NodeId,
    state: S,
    future: F,
}

impl<S, F> Process<S, F>
where
    F: Future<Output = RuntimeResult<()>>,
{
    #[inline]
    pub fn new(node_index: NodeId, state: S, future: F) -> Self {
        Self {
            node_index,
            state,
            future,
        }
    }

    #[inline]
    pub fn node_index(&self) -> NodeId {
        self.node_index
    }
}

impl<S, F> Future for Process<S, F>
where
    F: Future<Output = RuntimeResult<()>>,
{
    type Output = RuntimeResult<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: pinning `Process` pins `future`; this projection never moves
        // either `future` or `state` out of the enclosing value.
        unsafe { self.map_unchecked_mut(|process| &mut process.future) }.poll(context)
    }
}

pub(crate) struct RunningProcess {
    pub(crate) node: NodeId,
    pub(crate) task: JoinHandle<RuntimeResult<()>>,
}

impl NodeMain {
    pub(crate) fn register_process<S, F>(
        &mut self,
        node: NodeId,
        process: Process<S, F>,
    ) -> RuntimeResult<()>
    where
        S: Send + 'static,
        F: Future<Output = RuntimeResult<()>> + Send + 'static,
    {
        if process.node_index() != node || self.node_kind(node)? != NodeKind::Process {
            return Err(RuntimeError::ProcessNodeNotRegistered { node });
        }
        if self
            .running_processes
            .iter()
            .any(|running| running.node == node)
        {
            return Err(RuntimeError::ProcessNodeAlreadyStarted { node });
        }
        let runtime = self
            .process_runtime
            .as_ref()
            .ok_or(RuntimeError::MainProcessRuntimeUnavailable)?;
        let task = runtime.spawn(process);
        self.running_processes.push(RunningProcess { node, task });
        Ok(())
    }

    pub(crate) fn take_process_events(
        &mut self,
    ) -> RuntimeResult<mpsc::UnboundedReceiver<(u64, u64)>> {
        let node = self
            .current_process_index
            .ok_or(RuntimeError::ProcessConstructorInactive)?;
        let slot = node.slot() as usize;
        let sender_slot = self
            .process_event_senders
            .get_mut(slot)
            .ok_or(RuntimeError::ProcessNodeNotRegistered { node })?;
        if sender_slot.is_some() {
            return Err(RuntimeError::ProcessEventReceiverAlreadyTaken { node });
        }

        let (sender, receiver) = mpsc::unbounded_channel();
        let pending = self
            .process_event_state
            .get_mut(slot)
            .ok_or(RuntimeError::ProcessNodeNotRegistered { node })?;
        for event in pending.drain(..) {
            assert!(
                sender.send(event).is_ok(),
                "new Process event receiver remains live during construction"
            );
        }
        *sender_slot = Some(sender);
        if !self.suspended_processes.contains(&node) {
            self.suspended_processes.push(node);
        }
        Ok(receiver)
    }

    pub fn signal_process(
        &mut self,
        node: NodeId,
        event_type: u64,
        data: u64,
    ) -> RuntimeResult<()> {
        if self.node_kind(node)? != NodeKind::Process || !self.process_node_indices.contains(&node)
        {
            return Err(RuntimeError::ProcessNodeNotRegistered { node });
        }
        let slot = node.slot() as usize;
        let sender = self
            .process_event_senders
            .get(slot)
            .ok_or(RuntimeError::ProcessNodeNotRegistered { node })?;
        if let Some(sender) = sender {
            sender
                .send((event_type, data))
                .map_err(|_| RuntimeError::ProcessEventQueueClosed { node })?;
        } else {
            self.process_event_state
                .get_mut(slot)
                .ok_or(RuntimeError::ProcessNodeNotRegistered { node })?
                .push((event_type, data));
        }

        if !self
            .process_restore_next
            .iter()
            .any(|(current, _)| *current == node)
        {
            self.process_restore_next.push((node, event_type));
        }
        if let Some(index) = self
            .suspended_processes
            .iter()
            .position(|current| *current == node)
        {
            self.suspended_processes.swap_remove(index);
        }
        Ok(())
    }

    pub fn process_wait(&mut self, node: NodeId, deadline: Option<Instant>) -> RuntimeResult<()> {
        if self.node_kind(node)? != NodeKind::Process || !self.process_node_indices.contains(&node)
        {
            return Err(RuntimeError::ProcessNodeNotRegistered { node });
        }
        if !self.suspended_processes.contains(&node) {
            self.suspended_processes.push(node);
        }
        self.process_timer_state
            .retain(|(current, _)| *current != node);
        if let Some(deadline) = deadline {
            self.process_timer_state.push((node, deadline));
        }
        self.time_next_process_ready = self
            .process_timer_state
            .iter()
            .map(|(_, deadline)| *deadline)
            .min();
        Ok(())
    }

    pub fn restore_processes(&mut self, now: Instant) -> RuntimeResult<usize> {
        self.process_restore_current.clear();
        self.process_restore_current
            .append(&mut self.process_restore_next);
        let mut index = 0usize;
        while index < self.process_timer_state.len() {
            if self.process_timer_state[index].1 > now {
                index += 1;
                continue;
            }
            let (node, _) = self.process_timer_state.swap_remove(index);
            self.process_restore_current.push((node, 0));
            if let Some(index) = self
                .suspended_processes
                .iter()
                .position(|current| *current == node)
            {
                self.suspended_processes.swap_remove(index);
            }
        }
        self.time_next_process_ready = self
            .process_timer_state
            .iter()
            .map(|(_, deadline)| *deadline)
            .min();
        Ok(self.process_restore_current.len())
    }

    pub fn process_node_index(&self, name: &str) -> Option<NodeId> {
        self.node_by_name(name)
            .filter(|node| self.process_node_indices.contains(node))
    }

    pub fn stop_processes(&mut self) -> RuntimeResult<()> {
        crate::ensure_main_thread()?;
        if self.process_runtime.is_none() {
            return Err(RuntimeError::MainProcessRuntimeUnavailable);
        }
        let mut running = core::mem::take(&mut self.running_processes);
        for process in &running {
            process.task.abort();
        }
        let runtime = self
            .process_runtime
            .take()
            .expect("validated thread-zero Process runtime remains installed");
        let process_result = runtime.block_on(async move {
            let mut process_error = None;
            for process in running.drain(..) {
                let error = match process.task.await {
                    Ok(Ok(())) => None,
                    Err(source) if source.is_cancelled() => None,
                    Ok(Err(error)) => Some(error),
                    Err(source) => Some(RuntimeError::ProcessTaskJoin {
                        node: process.node,
                        source,
                    }),
                };
                if let Some(error) = error {
                    process_error = Some(match process_error {
                        None => error,
                        Some(primary) => RuntimeError::ProcessShutdownCleanup {
                            primary: Box::new(primary),
                            cleanup: Box::new(error),
                        },
                    });
                }
            }
            process_error.map_or(Ok(()), Err)
        });
        assert!(
            self.process_runtime.replace(runtime).is_none(),
            "thread-zero Process runtime has one NodeMain owner"
        );
        process_result
    }
}

impl DataPlaneMain {
    pub fn start_processes<'entry>(
        &mut self,
        entries: impl Iterator<Item = &'entry NodeEntry>,
    ) -> RuntimeResult<()> {
        crate::ensure_main_thread()?;
        if self.thread_index() != 0 {
            return Err(RuntimeError::MainProcessRuntimeUnavailable);
        }
        if self.nodes.processes_started {
            return Ok(());
        }
        if self.nodes.process_runtime.is_none() {
            return Err(RuntimeError::MainProcessRuntimeUnavailable);
        }

        let mut declarations: Vec<&NodeEntry> = Vec::new();
        for entry in entries {
            if declarations
                .iter()
                .any(|current| std::ptr::eq(*current, entry))
            {
                continue;
            }
            let name = entry
                .registration
                .map(hammer_core::data_plane::NodeRegistration::name)
                .ok_or(RuntimeError::ProcessNodeNameMissing)?;
            if entry.kind != NodeKind::Process {
                return Err(RuntimeError::ProcessNodeKindInvalid {
                    name,
                    kind: entry.kind,
                });
            }
            if entry.process_start.is_none() {
                return Err(RuntimeError::ProcessStartMissing { name });
            }
            if entry.process_node_index.is_none() {
                return Err(RuntimeError::ProcessNodeIndexStorageMissing { name });
            }
            if declarations.iter().any(|current: &&NodeEntry| {
                current.registration.map(|registration| registration.name()) == Some(name)
            }) {
                return Err(RuntimeError::DuplicateProcessNode { name });
            }
            declarations.push(entry);
        }

        for entry in declarations {
            let node = (entry.init)(self)?;
            let node_index = entry
                .process_node_index
                .expect("validated Process declaration owns its NodeId slot");
            if let Some(current) = node_index.get() {
                assert_eq!(*current, node, "Process Node identity remains stable");
            } else {
                assert!(
                    node_index.set(node).is_ok(),
                    "Process Node identity is installed once"
                );
            }
            self.nodes.process_node_indices.push(node);
            let slot = node.slot() as usize;
            self.nodes
                .process_event_state
                .resize_with(slot + 1, Vec::new);
            self.nodes
                .process_event_senders
                .resize_with(slot + 1, || None);
            assert!(
                self.nodes.current_process_index.replace(node).is_none(),
                "Process constructors run serially"
            );
            let result = entry
                .process_start
                .expect("validated Process declaration owns its constructor")(
                self, node
            );
            assert_eq!(
                self.nodes.current_process_index.take(),
                Some(node),
                "Process constructor identity remains stable"
            );
            result?;
        }
        self.nodes.processes_started = true;
        Ok(())
    }

    pub fn process_events(&mut self) -> RuntimeResult<mpsc::UnboundedReceiver<(u64, u64)>> {
        self.nodes.take_process_events()
    }

    pub fn signal_process(
        &mut self,
        node: NodeId,
        event_type: u64,
        data: u64,
    ) -> RuntimeResult<()> {
        self.nodes.signal_process(node, event_type, data)
    }

    pub fn process_wait(&mut self, node: NodeId, deadline: Option<Instant>) -> RuntimeResult<()> {
        self.nodes.process_wait(node, deadline)
    }

    pub fn run_main_until<F>(&mut self, future: F) -> RuntimeResult<F::Output>
    where
        F: Future,
    {
        crate::ensure_main_thread()?;
        if self.thread_index() != 0 {
            return Err(RuntimeError::MainProcessRuntimeUnavailable);
        }
        if self.nodes.process_runtime.is_none() {
            return Err(RuntimeError::MainProcessRuntimeUnavailable);
        }
        let runtime = self
            .nodes
            .process_runtime
            .take()
            .expect("validated thread-zero Process runtime remains installed");
        let output = runtime.block_on(async {
            tokio::pin!(future);
            loop {
                self.nodes.restore_processes(Instant::now())?;
                tokio::select! {
                    output = &mut future => break Ok(output),
                    readiness = self.next_file_readiness() => {
                        readiness?;
                    }
                }
            }
        });
        assert!(
            self.nodes.process_runtime.replace(runtime).is_none(),
            "thread-zero Process runtime has one NodeMain owner"
        );
        output
    }

    pub fn stop_processes(&mut self) -> RuntimeResult<()> {
        self.nodes.stop_processes()
    }

    #[doc(hidden)]
    pub fn __register_process<S, F>(&mut self, process: Process<S, F>) -> RuntimeResult<()>
    where
        S: Send + 'static,
        F: Future<Output = RuntimeResult<()>> + Send + 'static,
    {
        let node = process.node_index();
        self.nodes.register_process(node, process)
    }
}
