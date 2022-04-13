use thiserror::Error;

/// Errors that can happen while using this crate.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Flattening the JSON failed")]
    Flattening(#[from] flatten_json_object::error::Error),

    #[error(
        "Two objects have keys that should be different but end looking the same after flattening"
    )]
    FlattenedKeysCollision,

    #[error("Writting a CSV record failed")]
    WrittingCSV(#[from] csv::Error),

    #[error("Parsing JSON failed")]
    ParsingJson(#[from] serde_json::Error),

    #[error("Input/output error")]
    InputOutput(#[from] std::io::Error),
}
