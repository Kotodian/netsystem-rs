# Session Message Queue multi-producer safety is separate from event identity

Session event identity (ADR-0010) and Session Message Queue multi-producer safety are separate decisions.

ADR-0010 decides what a Session Event means: IO events carry session index only; control Session Events carry a Session Handle; pool generation is omitted from the event ABI.

This ADR decides how those events move under concurrent producers: the Session Message Queue is a VPP-shaped, producer-locked multi-ring queue (descriptor queue plus IO and CTRL rings). Infrastructure owns the session-neutral elsize rings; typed Session Event APIs and ring choice (`enqueue_io` / `enqueue_ctrl`) live at the app/session boundary. Local and SVM backends share the same logical layout; wake signaling remains backend-specific.

Do not re-bundle these concerns. Changing identity rules does not imply changing queue shape, and hardening multi-producer safety does not require generation-bearing Session Events.

## Single-producer control queues

App↔Session control queues (the Application's request queue and the daemon's reply queue) use the same multi-ring queue in a `SingleProducer` mode, shaped after VPP `svm_msg_q`: the same descriptor queue and IO/CTRL rings, with the CTRL ring holding fixed `SESSION_CTRL_MSG_MAX_SIZE` control slots. The producer capability is claimed exactly once per mapping through a CAS on the shared header; a second claim is a typed error, never a panic. This replaces VPP's producer mutex: the control queues have one designated producer by construction, so a lock would guard nothing. Worker fan-in queues remain `MultiProducer` and keep the producer-locked free list.

Single-producer rings use VPP cursor head/tail in the same generic ring layout (no free list, no ABA): publish order is payload → descriptor → `q_tail` (Release); the consumer reads `q_tail` (Acquire) and frees slots in order, with one outstanding borrowed slot enforced by `&mut self` (no heap copy). Full checks read the ring head with Acquire, and the consumer is signaled only on the true empty → nonempty transition, decided before the publish. The queue mode tag lives in the shared header and every mapping validates it (`ModeMismatch`).
