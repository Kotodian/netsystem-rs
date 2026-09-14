use hammer_ipc::binary_api::{Api, deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Api)]
#[api(returns = null)]
struct InterfaceState {
    id: u16,
    sw_if_index: u32,
    enabled: bool,
}

#[test]
fn interface_state_serde_round_trip() {
    let message = InterfaceState {
        id: 7,
        sw_if_index: 42,
        enabled: true,
    };
    let mut bytes = [0; 7];
    assert_eq!(serialize(&message, &mut bytes).unwrap(), bytes.len());
    assert_eq!(bytes, [0, 7, 0, 0, 0, 42, 1]);
    assert_eq!(deserialize::<InterfaceState>(&bytes).unwrap(), message);
}
