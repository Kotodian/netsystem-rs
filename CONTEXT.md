# Hammer Runtime

Hammer's runtime separates process-wide authority from the control scheduler
that owns the main operating-system thread and keeps packet execution on Data
Workers.

## Runtime Language

**GlobalMain**:
The process-wide runtime authority corresponding to VPP's
`vlib_global_main_t`. It owns worker lifecycle, graph publication and refork,
registrations, plugin lifetime, and process-wide lifecycle state; it does not
own Worker Barrier synchronization or execute packet graph work.
_Avoid_: MainThread, DataPlaneMain, engine

**Worker Barrier**:
A main-thread synchronization interval that pauses every Data Worker while
control code mutates worker-visible state. It is distinct from process-wide
worker lifecycle, graph publication, and refork authority.
_Avoid_: GlobalMain barrier, worker lock

**Graph Refork**:
Rebuilding each Data Worker's node/runtime clone from the published main graph
while retaining that worker's existing runtime state.
_Avoid_: graph replacement, worker reinitialization

**ControlThread**:
The scheduler running on the main operating-system thread. It uses a
single-thread Tokio runtime to dispatch Process Nodes, process restores, timer
expirations, main-thread RPCs, control I/O readiness, and lifecycle decisions;
it does not execute Data Worker packet graph work.
_Avoid_: GlobalMain, Data Worker, control loop

**Process Restore**:
A main-thread scheduling record that says why a suspended Process Node may be
resumed, such as an event, clock expiration, timed event, or yield. It is
consumed by `ControlThread` and is distinct from a Data Worker graph frame.
_Avoid_: packet frame, task completion, generic wakeup

**Main-Thread RPC**:
A queued control-plane operation whose callback is executed by `ControlThread`
on the main operating-system thread, with a worker barrier when the operation
publishes worker-visible state.
_Avoid_: Data Worker task, Tokio request, packet dispatch

**Data Worker**:
A worker operating-system thread that owns one `DataPlaneMain` and executes
packet graph nodes, frames, buffers, handoff work, and worker-local readiness.
_Avoid_: main thread, control thread

**Process Node**:
A cooperative control-plane execution context scheduled on the main operating-
system thread. In Hammer it is represented by one Tokio task and may suspend
until an event, clock, timed event, or yield makes it runnable; it is not an OS
thread and does not execute packet graph work.
_Avoid_: process thread, Data Worker, background thread
