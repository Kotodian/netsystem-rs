//! VPE protocol declarations shared by the server handler and external
//! language bindings.
use super::Api;

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "show_version", returns = ShowVersionReply)]
pub struct ShowVersion {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
}

#[derive(Clone, Debug, Api)]
#[api(name = "show_version_reply")]
pub struct ShowVersionReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    #[api(string = 32)]
    pub program: String,
    #[api(string = 32)]
    pub version: String,
    #[api(string = 32)]
    pub build_date: String,
    #[api(string = 256)]
    pub build_directory: String,
}
