mod build;
mod options;
mod parse;
mod raw;

pub use options::*;

use crate::error::HammerError;
use raw::RawConfig;

pub fn check_config(content: &str) -> Result<(), HammerError> {
    parse_config(content).map(|_| ())
}

pub fn format_config(content: &str) -> Result<String, HammerError> {
    let raw = decode_raw(content)?;
    toml::to_string(&raw).map_err(|e| HammerError::internal(format!("encode TOML: {e}")))
}

pub fn parse_config(content: &str) -> Result<Options, HammerError> {
    let raw = decode_raw(content)?;
    build::build_options(raw)
}

fn decode_raw(content: &str) -> Result<RawConfig, HammerError> {
    toml::from_str::<RawConfig>(content).map_err(translate_toml_error)
}

fn translate_toml_error(err: toml::de::Error) -> HammerError {
    let msg = err.message();
    if let Some(field) = extract_unknown_field(msg) {
        return HammerError::config_validation(format!("unsupported config key: {field}"));
    }
    HammerError::config_parse(format!("parse TOML: {msg}"))
}

fn extract_unknown_field(msg: &str) -> Option<String> {
    let needle = "unknown field ";
    let i = msg.find(needle)?;
    let rest = &msg[i + needle.len()..];
    let mut chars = rest.chars();
    let opener = chars.next()?;
    if opener != '`' && opener != '\'' && opener != '"' {
        return None;
    }
    let inner = &rest[opener.len_utf8()..];
    let close = inner.find(opener)?;
    Some(inner[..close].to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_unknown_field_with_backticks() {
        assert_eq!(
            extract_unknown_field("unknown field `profile`, expected one of `log`"),
            Some("profile".to_owned())
        );
    }

    #[test]
    fn extracts_unknown_field_with_single_quotes() {
        assert_eq!(
            extract_unknown_field("unknown field 'profile' at line 5"),
            Some("profile".to_owned())
        );
    }
}
