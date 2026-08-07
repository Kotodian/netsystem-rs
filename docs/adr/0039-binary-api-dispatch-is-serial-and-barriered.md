# Binary API Dispatch is Serial and Barriered

Status: accepted

Hammer dispatches Binary API methods serially on the Main Thread. After a complete frame is read and its method registration is resolved, `BinaryApiMain` acquires `WorkerBarrier`, invokes exactly one synchronous `BinaryApiMethodEntry`, constructs its reply, and releases the barrier. A method cannot execute concurrently with another method, spawn parallel method work, or retain the barrier across an await boundary.

This follows VPP's Session Socket API control path: `sapi_sock_read_ready` reads the message, calls `vlib_worker_thread_barrier_sync`, dispatches the selected application or certificate mutation handler, and then calls `vlib_worker_thread_barrier_release`. A single dispatch boundary makes every plugin-visible control mutation use the same publication protocol and avoids method-specific barrier metadata, handler locks, and nested completion mechanisms.

The daemon's current-thread Tokio runtime may interleave connection I/O only at await points outside method dispatch. It does not make plugin methods parallel. Plugin-owned Main Thread operations reached locally through the ABI must use the same serial control authority when they publish worker-visible state.
