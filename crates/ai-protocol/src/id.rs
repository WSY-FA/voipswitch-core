use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                validate_id(stringify!($name), &value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl TryFrom<String> for $name {
            type Error = anyhow::Error;

            fn try_from(value: String) -> Result<Self> {
                Self::new(value)
            }
        }
    };
}

fn validate_id(kind: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 {
        bail!("{kind} must contain 1..=128 bytes");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        bail!("{kind} contains unsupported characters");
    }
    Ok(())
}

string_id!(MessageId);
string_id!(ConnectorInstanceId);
string_id!(TenantId);
string_id!(ConversationId);
string_id!(ParticipantId);
string_id!(StreamId);
string_id!(OperationId);
string_id!(JobId);
string_id!(ProfileId);
string_id!(ProviderId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_ids_at_construction() {
        assert_eq!(JobId::new("job-1").unwrap().as_str(), "job-1");
        assert!(JobId::new("").is_err());
        assert!(JobId::new("bad/id").is_err());
    }
}
