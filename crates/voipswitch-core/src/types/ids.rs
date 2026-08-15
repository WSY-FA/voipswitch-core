use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }
    };
}

string_id!(DomainId);
string_id!(EndpointId);
string_id!(TrunkId);
string_id!(CallId);
string_id!(SessionId);
string_id!(AdapterCallLegId);
string_id!(AdapterTransactionId);
string_id!(CalleeAttemptId);
string_id!(MediaBridgeId);
string_id!(MediaLegId);
string_id!(ActionId);
string_id!(BridgeRequestId);
string_id!(BridgeId);
string_id!(BusinessOperationId);
string_id!(CollectorId);
string_id!(MediaCapabilityLeaseId);
