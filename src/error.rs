use std::fs::File;
use std::io::BufWriter;
use thiserror::Error;

/// Errors that can happen while using this crate.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Flattening the JSON failed: {0}")]
    Flattening(#[from] flatten_json_object::Error),

    #[error(
        "Two objects have keys that should be different but end looking the same after flattening"
    )]
    FlattenedKeysCollision,

    #[error("Writting a CSV record failed: {0}")]
    WrittingCSV(#[from] csv::Error),

    #[error("Parsing JSON failed: {0}")]
    ParsingJson(#[from] serde_json::Error),

    #[error("Input/output error: {0}")]
    InputOutput(#[from] std::io::Error),

    #[error("Could not extract the inner file from a BufWriter: {0}")]
    IntoFile(#[from] std::io::IntoInnerError<BufWriter<File>>),
}
