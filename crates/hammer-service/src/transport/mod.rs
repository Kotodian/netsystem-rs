use hammer_runtime::RuntimeResult;

pub mod congestion;

#[hammer_component_macros::init_function(name = "transport_init")]
fn init_transport() -> RuntimeResult<()> {
    Ok(())
}
