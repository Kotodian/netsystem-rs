# Session events follow VPP identity rules

Hammer app↔session message-queue events follow VPP `session_event_t` identity rules rather than inventing generation-safe event tokens.

IO events carry session index only. Control close/connect events carry a VPP-shaped Session Handle (`session_index` packed with worker/thread index). Consume paths drop free or unmapped session slots and drop Close events whose worker index does not match the draining worker.

Pool generation is intentionally omitted from the event ABI. After a free slot is reused, a stale index-only IO event may still target the replacement session; that window matches VPP and is accepted here.
