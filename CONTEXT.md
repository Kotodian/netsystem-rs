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

**InterfaceRegistrationImage**:
The service-owned static declaration image for device classes, hardware-
interface classes, and interface callbacks. It is consumed by `InterfaceMain`
at startup to build active interface state and is independent of `PluginMain`
ownership or plugin lifecycle.
_Avoid_: generic registration image, interface record

**NetMain**:
The service-owned network authority corresponding to VPP's `vnet_main_t`. It
is the single entry point for network-wide interface and device coordination
and owns the `InterfaceMain` authority.
_Avoid_: network manager, network context

**InterfaceMain**:
The interface authority embedded in `NetMain`, corresponding to VPP's
`vnet_interface_main_t`. It owns interface identity,
address, MTU, hardware-interface, queue, and interface callback state that must
be coordinated across network services and device drivers. It is initialized
before runtime interface configuration is applied.
_Avoid_: InterfaceControlPlane, interface registry

**DeviceMain**:
The service-owned device authority corresponding to VPP's
`vnet_device_main_t`. It is process-global and owns device-input worker scope,
aggregate receive statistics, and device scheduling state, while device
instances, hardware interfaces, and RX/TX queues belong to `InterfaceMain`.
_Avoid_: device registry, interface registry

**DeviceClass**:
The driver behavior declaration that describes how a device class sends and
integrates with the network interface authority.
_Avoid_: device kind, device type

**HwClass**:
The hardware-interface behavior declaration associated with a device class.
_Avoid_: hardware interface type

**HwInterface**:
The hardware-facing interface instance owned by `InterfaceMain`, identified by
its hardware-interface index and linked to a software interface.
_Avoid_: hardware interface record

**SwInterface**:
The software-facing interface instance owned by `InterfaceMain`, identified by
its software-interface index and linked to its hardware interface when one
exists.
_Avoid_: software interface record

**Interface Component**:
A compile-time Hammer component declaration for a network-device class,
hardware-interface class, or interface callback. It registers the driver's
network behavior with the owning network authority and is distinct from a
runtime software-interface or hardware-interface instance.
_Avoid_: interface record, interface helper
