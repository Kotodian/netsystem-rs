# Active Session connect completion is asynchronous

Status: accepted

An accepted outbound Session connect request creates a pending Application Connection identified by `ApplicationConnectionId`. Request acceptance does not mean that a layered transport handshake has completed. Session retains that identity until the owning Data Worker publishes exact success or failure.

On success, Session publishes the app-facing Session and completes the Application Connection. On a failure before Session publication, completion targets the Application Connection because no valid `SessionId` exists. A layered transport such as QUIC reports its owner-local handshake failure facts at the plugin boundary; Session translates them once into the application-visible completion contract.

The application-facing carrier is a VPP `session_connected_msg_t`-shaped variant of the existing `ApplicationSessionReply` protocol on the existing Application CTRL reply queue. It carries the pending Application Connection as the connect context, a typed completion status, and the published Session handle on success. The existing App Session publication path carries FIFO/segment descriptors. Hammer does not add another `SessionEvtType`: the existing `SessionEvtType::Connect` remains the Session-targeted event after publication. This preserves the VPP split between the worker's `SESSION_CTRL_EVT_CONNECTED` and the application callback/message produced from it without creating another queue or event enum.

This follows VPP's `app_worker_connect_notify` model. It keeps TLS handshake work on the Data Worker, avoids blocking the Main Thread control request, and gives active-connect failure a stable application-owned identity. The rejected alternatives are making the Main Thread wait for the handshake and treating a post-acceptance handshake failure as unobservable.

The public failure categories remain owner-local and are translated once at the application control seam. This ADR establishes the asynchronous lifetime, VPP-shaped identity, carrier, and queue contract; it does not expand the generic Session Event ABI.
