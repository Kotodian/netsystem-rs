# QUIC enters through UDP destination-port registration

Status: accepted

The UDP plugin remains the authority for UDP parsing and destination-port dispatch, and the QUIC plugin consumes only validated UDP datagrams from `udp-input` through `quic-input`. UDP publishes a cross-plugin `UdpLocal` capability whose operations follow VPP's `udp_register_dst_port` and `udp_unregister_dst_port` vocabulary (`register_dst_port` and `unregister_dst_port` in Rust), including the IPv4/IPv6 distinction. QUIC loads after UDP, registers each bound or active local destination port with this capability, and unregisters it when the final owner releases the port; QUIC does not parse UDP independently, statically embed another UDP plugin instance, or own UDP dispatch state.
