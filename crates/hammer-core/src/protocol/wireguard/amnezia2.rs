use std::convert::TryInto;

use serde::{Deserialize, Serialize};

use crate::error::HammerError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum Amnezia2Version {
    #[serde(rename = "2.0")]
    #[default]
    V2_0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageTypeRange {
    pub min: u32,
    pub max: u32,
}

impl MessageTypeRange {
    pub fn contains(&self, value: u32) -> bool {
        self.min <= value && value <= self.max
    }

    pub fn is_zero(&self) -> bool {
        self.min == 0 && self.max == 0
    }

    fn validate(&self, field: &str) -> Result<(), HammerError> {
        if self.min > self.max {
            return Err(HammerError::config_validation(format!(
                "{field}.min must be <= {field}.max"
            )));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for MessageTypeRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Fixed(u32),
            Text(String),
            Pair { min: u32, max: u32 },
        }

        let repr = Repr::deserialize(deserializer)?;
        match repr {
            Repr::Fixed(value) => Ok(Self {
                min: value,
                max: value,
            }),
            Repr::Text(value) => parse_message_type_range(&value).map_err(serde::de::Error::custom),
            Repr::Pair { min, max } => Ok(Self { min, max }),
        }
    }
}

impl Serialize for MessageTypeRange {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MessageTypeRange", 2)?;
        state.serialize_field("min", &self.min)?;
        state.serialize_field("max", &self.max)?;
        state.end()
    }
}

fn parse_message_type_range(value: &str) -> Result<MessageTypeRange, String> {
    let value = value.trim();
    let Some((min, max)) = value.split_once('-') else {
        let fixed = value
            .parse::<u32>()
            .map_err(|err| format!("invalid message type range '{value}': {err}"))?;
        return Ok(MessageTypeRange {
            min: fixed,
            max: fixed,
        });
    };
    let min = min
        .trim()
        .parse::<u32>()
        .map_err(|err| format!("invalid message type range '{value}': {err}"))?;
    let max = max
        .trim()
        .parse::<u32>()
        .map_err(|err| format!("invalid message type range '{value}': {err}"))?;
    Ok(MessageTypeRange { min, max })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmneziaPacketKind {
    HandshakeInit,
    HandshakeResponse,
    CookieReply,
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Amnezia2Options {
    pub enabled: bool,
    #[serde(default)]
    pub version: Amnezia2Version,
    pub h1: MessageTypeRange,
    pub h2: MessageTypeRange,
    pub h3: MessageTypeRange,
    pub h4: MessageTypeRange,
    pub s1: u16,
    pub s2: u16,
    pub s3: u16,
    pub s4: u16,
    pub jc: u16,
    pub jmin: u16,
    pub jmax: u16,
    pub i1: Option<String>,
    pub i2: Option<String>,
    pub i3: Option<String>,
    pub i4: Option<String>,
    pub i5: Option<String>,
}

impl Amnezia2Options {
    pub fn validate(&self, field: &str) -> Result<(), HammerError> {
        if !self.enabled {
            return Ok(());
        }

        for (name, range) in [
            ("h1", &self.h1),
            ("h2", &self.h2),
            ("h3", &self.h3),
            ("h4", &self.h4),
        ] {
            range.validate(&format!("{field}.{name}"))?;
        }

        let ranges = [
            ("h1", self.h1),
            ("h2", self.h2),
            ("h3", self.h3),
            ("h4", self.h4),
        ];
        for idx in 0..ranges.len() {
            for other_idx in (idx + 1)..ranges.len() {
                let (name, left) = ranges[idx];
                let (other_name, right) = ranges[other_idx];
                if left.is_zero() || right.is_zero() {
                    continue;
                }
                if left.min <= right.max && right.min <= left.max {
                    return Err(HammerError::config_validation(format!(
                        "{field}.{name} overlaps {field}.{other_name}"
                    )));
                }
            }
        }

        for (name, value) in [("s1", self.s1), ("s2", self.s2), ("s3", self.s3)] {
            if value > 64 {
                return Err(HammerError::config_validation(format!(
                    "{field}.{name} must be in 0..=64"
                )));
            }
        }
        if self.s4 > 32 {
            return Err(HammerError::config_validation(format!(
                "{field}.s4 must be in 0..=32"
            )));
        }

        if self.jc > 10 {
            return Err(HammerError::config_validation(format!(
                "{field}.jc must be in 0..=10"
            )));
        }
        if self.jc > 0 {
            if !(64..=1024).contains(&self.jmin) || !(64..=1024).contains(&self.jmax) {
                return Err(HammerError::config_validation(format!(
                    "{field}.jmin and {field}.jmax must be in 64..=1024 when jc > 0"
                )));
            }
        }
        if self.jmin > self.jmax {
            return Err(HammerError::config_validation(format!(
                "{field}.jmin must be <= {field}.jmax"
            )));
        }

        for (name, value) in [
            ("i1", self.i1.as_ref()),
            ("i2", self.i2.as_ref()),
            ("i3", self.i3.as_ref()),
            ("i4", self.i4.as_ref()),
            ("i5", self.i5.as_ref()),
        ] {
            let Some(value) = value else {
                continue;
            };
            if value.trim().is_empty() {
                return Err(HammerError::config_validation(format!(
                    "{field}.{name} must not be empty"
                )));
            }
        }

        Ok(())
    }

    pub fn prefix_len(&self, kind: AmneziaPacketKind) -> usize {
        usize::from(match kind {
            AmneziaPacketKind::HandshakeInit => self.s1,
            AmneziaPacketKind::HandshakeResponse => self.s2,
            AmneziaPacketKind::CookieReply => self.s3,
            AmneziaPacketKind::Data => self.s4,
        })
    }

    pub fn classify_wireguard_packet(&self, packet: &[u8]) -> Option<AmneziaPacketKind> {
        if !self.enabled {
            return None;
        }
        let checks = [
            (AmneziaPacketKind::HandshakeInit, self.h1, 148usize),
            (AmneziaPacketKind::HandshakeResponse, self.h2, 92usize),
            (AmneziaPacketKind::CookieReply, self.h3, 64usize),
            (AmneziaPacketKind::Data, self.h4, 32usize),
        ];
        for (kind, header, base_len) in checks {
            let prefix_len = self.prefix_len(kind);
            let min_len = prefix_len + base_len;
            if packet.len() < min_len || packet.len() < prefix_len + 4 {
                continue;
            }
            let header_value =
                u32::from_le_bytes(packet[prefix_len..prefix_len + 4].try_into().ok()?);
            if !header.contains(header_value) {
                continue;
            }
            if kind != AmneziaPacketKind::Data && packet.len() != min_len {
                continue;
            }
            return Some(kind);
        }
        None
    }
}
