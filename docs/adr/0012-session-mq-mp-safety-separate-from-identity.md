# Session Message Queue multi-producer safety is separate from event identity

Session event identity (ADR-0010) and Session Message Queue multi-producer safety are separate decisions.

ADR-0010 decides what a Session Event means: IO events carry session index only; control Session Events carry a Session Handle; pool generation is omitted from the event ABI.

This ADR decides how those events move under concurrent producers: the Session Message Queue is a VPP-shaped, producer-locked multi-ring queue (descriptor queue plus IO and CTRL rings). Infrastructure owns the session-neutral elsize rings; typed Session Event APIs and ring choice (`enqueue_io` / `enqueue_ctrl`) live at the app/session boundary. Local and SVM backends share the same logical layout; wake signaling remains backend-specific.

Do not re-bundle these concerns. Changing identity rules does not imply changing queue shape, and hardening multi-producer safety does not require generation-bearing Session Events.
