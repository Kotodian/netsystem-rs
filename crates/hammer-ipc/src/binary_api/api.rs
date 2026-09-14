//! Message identity and installation. VPP's range failure, replacement and
//! duplicate-name behaviors are intentionally distinct operations.
use super::{Api, codec};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("API message range `{name}` is already installed")]
    MessageRangeExists { name: std::string::String },
    #[error("invalid API message range count {count}")]
    MessageCountInvalid { count: u16 },
    #[error("API message `{name_crc}` is not installed")]
    MessageNameCrcMissing { name_crc: std::string::String },
}

pub struct ApiMsgConfig {
    pub id: u16,
    pub name: &'static str,
    pub crc: u32,
    pub handler: Option<fn(&mut ApiMain, &[u8], &mut [u8]) -> Result<usize, codec::Error>>,
    pub is_mp_safe: bool,
    pub traced: bool,
    pub replay: bool,
}
impl ApiMsgConfig {
    pub fn new<T: Api>(id: u16) -> Self {
        Self {
            id,
            name: T::NAME,
            crc: T::CRC,
            handler: None,
            is_mp_safe: false,
            traced: !T::FLAGS.contains(&"dont_trace"),
            replay: true,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct ApiMsgData {
    pub name: Option<&'static str>,
    pub handler: Option<fn(&mut ApiMain, &[u8], &mut [u8]) -> Result<usize, codec::Error>>,
    pub is_mp_safe: bool,
    pub trace_enable: bool,
    pub replay_allowed: bool,
}

pub struct ApiMsgRange {
    pub name: std::string::String,
    pub first_msg_id: u16,
    pub last_msg_id: u16,
}

pub struct ApiVersion {
    pub name: &'static str,
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

pub struct ApiMain {
    msg_data: Vec<ApiMsgData>,
    msg_id_by_name: HashMap<&'static str, u16>,
    msg_index_by_name_and_crc: HashMap<std::string::String, u16>,
    first_available_msg_id: u16,
    msg_ranges: Vec<ApiMsgRange>,
    msg_range_by_name: HashMap<std::string::String, usize>,
    api_version_list: Vec<ApiVersion>,
}

impl ApiMain {
    pub fn new(first_available_msg_id: u16) -> Self {
        Self {
            msg_data: Vec::new(),
            msg_id_by_name: HashMap::new(),
            msg_index_by_name_and_crc: HashMap::new(),
            first_available_msg_id,
            msg_ranges: Vec::new(),
            msg_range_by_name: HashMap::new(),
            api_version_list: Vec::new(),
        }
    }

    pub fn get_msg_ids(&mut self, name: &str, count: u16) -> Result<u16, Error> {
        if self.msg_range_by_name.contains_key(name) {
            return Err(Error::MessageRangeExists {
                name: name.to_owned(),
            });
        }
        if count > 1024 {
            return Err(Error::MessageCountInvalid { count });
        }
        // VPP stores the counter in u16 and permits zero count. Use explicit
        // wrapping, rather than adding a protocol error absent from that API.
        let base = self.first_available_msg_id;
        let end = base.wrapping_add(count);
        self.msg_ranges.push(ApiMsgRange {
            name: name.to_owned(),
            first_msg_id: base,
            last_msg_id: end.wrapping_sub(1),
        });
        self.msg_range_by_name
            .insert(name.to_owned(), self.msg_ranges.len() - 1);
        self.first_available_msg_id = end;
        Ok(base)
    }

    pub fn msg_config(&mut self, config: ApiMsgConfig) {
        if config.id == 0 {
            tracing::warn!(
                name = config.name,
                "API message id is zero; skipping registration"
            );
            return;
        }
        let index = usize::from(config.id);
        self.msg_data
            .resize(self.msg_data.len().max(index + 1), ApiMsgData::default());
        if let Some(handler) = self.msg_data[index].handler
            && config
                .handler
                .is_none_or(|next| !std::ptr::fn_addr_eq(handler, next))
        {
            tracing::warn!(name = config.name, "replacing an API message handler");
        }
        self.msg_data[index] = ApiMsgData {
            name: Some(config.name),
            handler: config.handler,
            is_mp_safe: config.is_mp_safe,
            trace_enable: config.traced,
            replay_allowed: config.replay,
        };
        self.msg_id_by_name.insert(config.name, config.id);
    }

    pub fn add_msg_name_crc(&mut self, name_crc: &str, id: u16) {
        if self.msg_index_by_name_and_crc.contains_key(name_crc) {
            tracing::warn!(name_crc, "duplicate API identity ignored");
            return;
        }
        self.msg_index_by_name_and_crc
            .insert(name_crc.to_owned(), id);
    }
    pub fn get_msg_index(&self, name_crc: &str) -> Result<u16, Error> {
        self.msg_index_by_name_and_crc
            .get(name_crc)
            .copied()
            .ok_or_else(|| Error::MessageNameCrcMissing {
                name_crc: name_crc.to_owned(),
            })
    }
    pub fn msg_id_by_name(&self, name: &str) -> Option<u16> {
        self.msg_id_by_name.get(name).copied()
    }
    pub fn get_msg_data(&self, id: u16) -> Option<&ApiMsgData> {
        self.msg_data.get(usize::from(id))
    }
    pub fn message_range(&self, name: &str) -> Option<&ApiMsgRange> {
        self.msg_range_by_name
            .get(name)
            .map(|index| &self.msg_ranges[*index])
    }
    pub fn add_version(&mut self, version: ApiVersion) {
        self.api_version_list.push(version);
    }
    pub fn versions(&self) -> &[ApiVersion] {
        &self.api_version_list
    }
    pub fn message_table(&self) -> impl Iterator<Item = (&str, u16)> + '_ {
        self.msg_index_by_name_and_crc
            .iter()
            .map(|(name, id)| (name.as_str(), *id))
    }
}
