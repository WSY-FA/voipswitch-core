use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

const MAX_EXTENSION_NUMBER_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumberError {
    Empty,
    TooLong { max: usize },
    InvalidCharacter { index: usize, character: char },
}

impl Display for NumberError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("extension number must not be empty"),
            Self::TooLong { max } => {
                write!(f, "extension number must not exceed {max} characters")
            }
            Self::InvalidCharacter { index, character } => write!(
                f,
                "extension number contains invalid character {character:?} at index {index}"
            ),
        }
    }
}

impl std::error::Error for NumberError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExtensionNumber(String);

impl ExtensionNumber {
    pub fn parse(input: &str) -> Result<Self, NumberError> {
        if input.is_empty() {
            return Err(NumberError::Empty);
        }
        if input.len() > MAX_EXTENSION_NUMBER_LEN {
            return Err(NumberError::TooLong {
                max: MAX_EXTENSION_NUMBER_LEN,
            });
        }
        if let Some((index, character)) = input
            .char_indices()
            .find(|(_, character)| !character.is_ascii_digit())
        {
            return Err(NumberError::InvalidCharacter { index, character });
        }
        Ok(Self(input.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ExtensionNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_ascii_digit_extension_numbers() {
        let number = ExtensionNumber::parse("1001").unwrap();
        assert_eq!(number.as_str(), "1001");
    }

    #[test]
    fn rejects_business_number_characters() {
        assert!(matches!(
            ExtensionNumber::parse("*100"),
            Err(NumberError::InvalidCharacter {
                index: 0,
                character: '*'
            })
        ));
    }
}
