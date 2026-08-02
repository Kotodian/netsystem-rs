# Session App callbacks replace AppSessionProtocol

Status: accepted

Hammer replaces the `AppSessionProtocol` seam with VPP-shaped Session App
callbacks. Session owns exact Session event dispatch, FIFOs, lifecycle, and
publication; each plugin registers a concrete static callback table and owns
its worker-local protocol state through the Session entry's `app_session`
opaque. The ordered `AppSessionPolicy` chain is removed, and Application
endpoints select a registered `SessionAppId` plus transport/crypto
configuration, matching VPP's `application_t`/`session_cb_vft_t` and
`session_endpoint_cfg_t` model rather than a generic protocol trait.
