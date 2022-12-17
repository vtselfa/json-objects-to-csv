//! ## Robust Rust library for converting JSON objects into CSV rows
//!
//! Given an array of JSON objects or a file that contains JSON objects one after the other, it
//! produces a CSV file with one row per JSON processed. In order to transform a JSON object into a
//! CSV row, this library "flattens" the objects, converting them into equivalent ones without nested
//! objects or arrays. The rules used for flattening objects are configurable, but by default an
//! object like this:
//!
//! ```json
//! {"a": {"b": [1,2,3]}}
//! ```
//!
//! is transformed into the flattened JSON object:
//!
//! ```json
//! {
//!   "a.b.0": 1,
//!   "a.b.1": 2,
//!   "a.b.2": 3
//! }
//! ```
//!
//! and then used to generate the following CSV output:
//! ```csv
//! a.b.0,a.b.1,a.b.2
//! 1,2,3
//! ```
//!
//! ### Configuring output
//!
//! This library relies on
//! [`flatten-json-object`](https://docs.rs/flatten-json-object/latest/flatten_json_object/)
//! for JSON object flattering and [`csv`](https://docs.rs/csv/latest/csv/) for CSV file generation.
//! Please check their respective documentation if you want to adjust how the output looks.
//!
//! ### Notes
//!
//! - How objects are flattened and the CSV format (e.g. the field separator) can be configured.
//! - Each top level object in the input will be transformed into a CSV row.
//! - The headers are sorted alphabetically and are the union of all the keys in all the objects in
//!   the input after they are flattened.
//! - Key collisions after flattening the input will be reported as errors, i.e. if two objects have
//!   keys that should be different but end looking the same after flattening. For example,
//!   flattening a file that contains `{"a": {"b": 1}} {"a.b": 2}` results by default in an error.
//! - Any instance of `{}` (when not a top level object), `[]` or `Null` results in an empty CSV
//!   field.
//!
//! ### Example reading from a `Read` implementer
//!
//!```rust
//!# use std::error::Error;
//!#
//!# fn main() -> Result<(), Box<dyn Error>> {
//!#
//! use csv;
//! use flatten_json_object::ArrayFormatting;
//! use flatten_json_object::Flattener;
//! use json_objects_to_csv::Json2Csv;
//! use std::io::{Read, Write};
//! use std::str;
//!
//! // Anything supported by the `Flattener` object is possible.
//! let flattener = Flattener::new()
//!     .set_key_separator(".")
//!     .set_array_formatting(ArrayFormatting::Surrounded{
//!         start: "[".to_string(),
//!         end: "]".to_string()
//!     })
//!     .set_preserve_empty_arrays(false)
//!     .set_preserve_empty_objects(false);
//!
//! // The output can be anything that implements `Write`. In this example we use a vector but
//! // this could be a `File`.
//! let mut output = Vec::<u8>::new();
//!
//! // Anything that implements `Read`. Usually a file, but we will use a byte array in this example.
//! let input = r#"{"a": {"b": 1}} {"c": [2]} {"d": []} {"e": {}}"#.as_bytes();
//!
//! // The CSV rows that we should get from this input and config. Note that since we are not
//! // preserving empty arrays or objects `d` and `e` are not part of the final headers.
//! // However, an empty row is generate for their object. If empty objects and arrays were
//! // preserved both `e` and `d` would be part of the headers, but their column would be empty.
//! let expected = ["a.b,c[0]", "1,", ",2", ",", ","];
//!
//! // Here we can configure another field separator, like `;` or use any other CSV builder
//! // configuration.
//! let csv_writer = csv::WriterBuilder::new()
//!     .delimiter(b',')
//!     .from_writer(&mut output);
//!
//! Json2Csv::new(flattener).convert_from_reader(input, csv_writer)?;
//!
//! assert_eq!(str::from_utf8(&output)?, expected.join("\n") + "\n");
//!#
//!#     Ok(())
//!# }
//! ```
//!
//! ### Example converting a slice of JSON objects
//!
//!```rust
//!# use std::error::Error;
//!#
//!# fn main() -> Result<(), Box<dyn Error>> {
//!#
//! use csv;
//! use flatten_json_object::ArrayFormatting;
//! use flatten_json_object::Flattener;
//! use json_objects_to_csv::Json2Csv;
//! use serde_json::json;
//! use std::str;
//!
//! // We changed the array formatting and we preserve empty arrays and objects now, compared to
//! // the previous example.
//! let flattener = Flattener::new()
//!     .set_key_separator(".")
//!     .set_array_formatting(ArrayFormatting::Plain)
//!     .set_preserve_empty_arrays(true)
//!     .set_preserve_empty_objects(true);
//!
//! // The output can be anything that implements `Write`. In this example we use a vector but
//! // this could be a `File`.
//! let mut output = Vec::<u8>::new();
//!
//! let input = [
//!     json!({"a": {"b": 1}}),
//!     json!({"c": [2]}),
//!     json!({"d": []}),
//!     json!({"e": {}})
//! ];
//!
//! // This time the separator is `;`
//! let csv_writer = csv::WriterBuilder::new()
//!     .delimiter(b';')
//!     .from_writer(&mut output);
//!
//! // The CSV rows that we should get from this input and config. We are preserving empty arrays
//! // and objects so `d` and `e` are part of the final headers. Since they are empty and no other
//! // object has those headers these columns have no value in any of the rows.
//! let expected = ["a.b;c.0;d;e", "1;;;", ";2;;", ";;;", ";;;"];
//!
//! Json2Csv::new(flattener).convert_from_array(&input, csv_writer)?;
//!
//! assert_eq!(str::from_utf8(&output)?, expected.join("\n") + "\n");
//!#
//!#     Ok(())
//!# }
//! ```

use flatten_json_object::ArrayFormatting;
use serde_json::{Deserializer, Value};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::io::{Read, Write};
use tempfile::tempfile;

pub use csv;
pub use error::Error;
pub use flatten_json_object;

mod error;

/// Basic struct of this crate. It contains the configuration.Instantiate it and use the method
/// `convert_from_array` or `convert_from_file` to convert the JSON input into a CSV file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Json2Csv {
    /// The flattener that we use internally.
    flattener: flatten_json_object::Flattener,
    /// The flattener provided by the user of the library.
    original_flattener: flatten_json_object::Flattener,
}

impl Json2Csv {
    /// Creates a JSON to CSV object with the flattening config provided.
    #[must_use]
    pub fn new(flattener: flatten_json_object::Flattener) -> Self {
        // We use replace the separators provided with control characters (which should not be
        // present in normal input) to be able to detect collisions like the one that happens when
        // converting `[{"a": {"b": 1}} {"a.b": 2}]` to CSV with a `.` separator.
        let key_sep = "␝";
        let array_start = "␞";
        let array_end = "␟";
        Json2Csv {
            flattener: match flattener.array_formatting() {
                ArrayFormatting::Plain => flattener.clone().set_key_separator(key_sep),
                ArrayFormatting::Surrounded { start: _, end: _ } => flattener
                    .clone()
                    .set_key_separator(key_sep)
                    .set_array_formatting(ArrayFormatting::Surrounded {
                        start: array_start.to_string(),
                        end: array_end.to_string(),
                    }),
            },
            original_flattener: flattener,
        }
    }

    /// The library uses internally a different key separator and potentially array formatting
    /// rules compared to what the user specified. This method is used to undo the transformation
    /// before presenting the results to the user.
    fn transform_key(&self, key: &str) -> String {
        let key = key.replace(
            self.flattener.key_separator(),
            self.original_flattener.key_separator(),
        );

        match self.original_flattener.array_formatting() {
            ArrayFormatting::Plain => key,
            ArrayFormatting::Surrounded { start: os, end: oe } => {
                match self.flattener.array_formatting() {
                    ArrayFormatting::Surrounded { start: s, end: e } => {
                        key.replace(e, oe).replace(s, os)
                    }
                    ArrayFormatting::Plain => {
                        unreachable!(
                            "We cloned the original flattener so both should have the same \
                            array formatting enum variant"
                        )
                    }
                }
            }
        }
    }

    /// Flattens each one of the objects in the array slice and transforms each of them into a CSV
    /// row.
    ///
    /// The headers of the CSV are the union of all the keys that result from flattening the
    /// objects in the input.
    ///
    /// # Errors
    /// Will return `Err` if `objects` does not contain actual JSON objects. It will also report an
    /// error if two objects have keys that should be different but end looking the same after
    /// flattening, and if writing the CSV fails.
    pub fn convert_from_array(
        self,
        objects: &[Value],
        mut csv_writer: csv::Writer<impl Write>,
    ) -> Result<(), error::Error> {
        // We have to flatten the JSON object since there is no other way to convert nested objects to CSV
        let mut orig_flat_maps = Vec::<serde_json::value::Map<String, Value>>::new();

        for obj in objects {
            let obj = self.flattener.flatten(obj)?;
            if let Value::Object(map) = obj {
                orig_flat_maps.push(map);
            } else {
                unreachable!("Flattening a JSON object always produces a JSON object");
            }
        }
        let orig_flat_maps = orig_flat_maps;

        let mut flat_maps = Vec::<serde_json::value::Map<String, Value>>::new();

        // The headers are the union of the keys of the flattened objects, sorted.
        // We collect the headers with our magic separators, and the headers with the separators that the user requested.
        let mut orig_headers = BTreeSet::<String>::new();
        let mut headers = BTreeSet::<String>::new();
        for orig_map in orig_flat_maps {
            let mut map = serde_json::value::Map::new();
            for (orig_key, value) in orig_map {
                let key = self.transform_key(&orig_key);
                map.insert(key.clone(), value);
                orig_headers.insert(orig_key);
                headers.insert(key);
            }
            flat_maps.push(map);
        }

        // If we could not extract headers there is nothing to write to the CSV file
        if headers.is_empty() {
            return Ok(());
        }

        // Check that there are no collisions between flattened keys in different objects
        if headers.len() != orig_headers.len() {
            return Err(Error::FlattenedKeysCollision);
        }

        csv_writer.write_record(&headers)?;
        for map in flat_maps {
            csv_writer.write_record(build_record(&headers, map))?;
        }

        Ok(())
    }

    /// Flattens the JSON objects in the file, transforming each of them into a CSV row.
    ///
    /// The headers of the CSV are the union of all the keys that result from flattening the objects
    /// in the input. The file must contain JSON objects one immediately after the other or
    /// separated by whitespace. Note that it uses a temporary file to store the flattened input,
    /// which is automatically deleted when lo longer necessary.
    ///
    /// # Errors
    /// Will return `Err` if parsing the file fails or if the JSONs there are not objects. It will
    /// also report an error if two objects have keys that should be different but end looking the
    /// same after flattening, and if writing the CSV or to the temporary file fails.
    pub fn convert_from_reader(
        self,
        reader: impl Read,
        mut csv_writer: csv::Writer<impl Write>,
    ) -> Result<(), error::Error> {
        // We have to flatten the JSON objects into a file because it can potentially be a really big
        // stream. We cannot directly convert into CSV because we cannot be sure about all the objects
        // resulting in the same headers.
        let mut tmp_file = BufWriter::new(tempfile()?);

        // The headers are the union of the keys of the flattened objects, sorted.
        // We collect the headers with our magic separators, and the headers with the separators that the user requested.
        let mut orig_headers = BTreeSet::<String>::new();
        let mut headers = BTreeSet::<String>::new();

        for obj in Deserializer::from_reader(reader).into_iter::<Value>() {
            let obj = obj?; // Ensure that we can parse the input properly
            let obj = self.flattener.flatten(&obj)?;

            let orig_map = match obj {
                Value::Object(map) => map,
                _ => unreachable!("Flattening a JSON object always produces a JSON object"),
            };

            let mut map = BTreeMap::new();
            for (orig_key, value) in orig_map {
                let key = self.transform_key(&orig_key);
                map.insert(key.clone(), value);
                orig_headers.insert(orig_key);
                headers.insert(key);
            }
            serde_json::to_writer(&mut tmp_file, &map)?;
        }

        // If we could not extract headers there is nothing to write to the CSV file
        if headers.is_empty() {
            return Ok(());
        }

        // Check that there are no collisions between flattened keys in different objects
        if headers.len() != orig_headers.len() {
            return Err(Error::FlattenedKeysCollision);
        }

        tmp_file.seek(SeekFrom::Start(0))?;
        let tmp_file = BufReader::new(tmp_file.into_inner()?);

        csv_writer.write_record(&headers)?;
        for obj in Deserializer::from_reader(tmp_file).into_iter::<Value>() {
            let map = match obj? {
                Value::Object(map) => map,
                _ => unreachable!("Flattening a JSON object always produces a JSON object"),
            };
            csv_writer.write_record(build_record(&headers, map))?;
        }

        Ok(())
    }
}

fn build_record(
    headers: &BTreeSet<String>,
    mut map: serde_json::Map<String, Value>,
) -> Vec<String> {
    let mut record: Vec<String> = vec![];
    for header in headers {
        if let Some(val) = map.remove(header) {
            match val {
                Value::String(s) => record.push(s),
                // _ => record.push(val.to_string()),
                Value::Bool(_) | Value::Number(_) => record.push(val.to_string()),
                // Any array or object here must be empty, because it would have been flattened
                // otherwise. In addition, to reach this for arrays and objects the flattener must
                // have been set to preserve them when empty. Makes no sense to add them or `Null`
                // to the CSV output, so we replace them with the empty string.
                Value::Null | Value::Array(_) | Value::Object(_) => record.push("".to_string()),
            }
        } else {
            record.push("".to_string());
        }
    }
    record
}

#[cfg(test)]
mod tests {
    use super::*;
    use error::Error;
    use flatten_json_object::{ArrayFormatting, Flattener};
    use rstest::rstest;
    use std::str;

    struct ExecutionResult {
        input: Vec<Value>,
        output: String,
    }

    fn execute_expect_err(input: &str, flattener: &Flattener) -> Vec<error::Error> {
        let mut output_from_file = Vec::<u8>::new();
        let csv_writer_from_file = csv::WriterBuilder::new()
            .delimiter(b',')
            .from_writer(&mut output_from_file);

        let result_from_file = Json2Csv::new(flattener.clone())
            .convert_from_reader(input.as_bytes(), csv_writer_from_file);

        let input_from_array: Result<Vec<_>, _> =
            Deserializer::from_str(input).into_iter::<Value>().collect();
        let input_from_array = input_from_array.unwrap();

        let mut output_from_array = Vec::<u8>::new();
        let csv_writer_from_array = csv::WriterBuilder::new()
            .delimiter(b',')
            .from_writer(&mut output_from_array);
        let result_from_array = Json2Csv::new(flattener.clone())
            .convert_from_array(&input_from_array, csv_writer_from_array);

        // We expect both to produce the same error
        let error_from_file = result_from_file.err().unwrap();
        let error_from_array = result_from_array.err().unwrap();

        vec![error_from_file, error_from_array]
    }

    fn execute(input: &str, flattener: &Flattener) -> ExecutionResult {
        let mut output_from_file = Vec::<u8>::new();
        let csv_writer_from_file = csv::WriterBuilder::new()
            .delimiter(b',')
            .from_writer(&mut output_from_file);
        Json2Csv::new(flattener.clone())
            .convert_from_reader(input.as_bytes(), csv_writer_from_file)
            .unwrap();

        let input_from_array: Result<Vec<_>, _> =
            Deserializer::from_str(input).into_iter::<Value>().collect();
        let input_from_array = input_from_array.unwrap();

        let mut output_from_array = Vec::<u8>::new();
        let csv_writer_from_array = csv::WriterBuilder::new()
            .delimiter(b',')
            .from_writer(&mut output_from_array);
        Json2Csv::new(flattener.clone())
            .convert_from_array(&input_from_array, csv_writer_from_array)
            .unwrap();

        let output_from_file = str::from_utf8(&output_from_file).unwrap();
        let output_from_array = str::from_utf8(&output_from_array).unwrap();

        assert_eq!(output_from_file, output_from_array);

        ExecutionResult {
            input: input_from_array,
            output: output_from_array.to_string(),
        }
    }

    #[rstest]
    #[case::nesting_and_array(r#"{"a": {"b": 1}}{"c": [2]}"#, &["a.b,c.0", "1,", ",2"])]
    #[case::spaces_end(r#"{"a": {"b": 1}}{"c": [2]}   "#, &["a.b,c.0", "1,", ",2"])]
    #[case::spaces_begin(r#"      {"a": {"b": 1}}{"c": [2]}"#, &["a.b,c.0", "1,", ",2"])]
    #[case::key_repeats_consistently(r#"{"a": 3}{"a": 4}{"a": 5}"#, &["a", "3", "4", "5"])]
    #[case::reordering(r#"{"b": 3, "a": 1}{"a": 4, "b": 2}"#, &["a,b", "1,3", "4,2"])]
    #[case::reordering_with_empty_array(r#"{"b": 3, "a": 1, "c": 0}{"c": [], "a": 4, "b": 2}"#, &["a,b,c", "1,3,0", "4,2,"])]
    #[case::reordering_with_empty_object(r#"{"b": 3, "a": 1, "c": 0}{"c": {}, "a": 4, "b": 2}"#, &["a,b,c", "1,3,0", "4,2,"])]
    #[case::reordering_with_missing(r#"{"b": 3, "a": 1, "c": 0}{"a": 4, "b": 2}"#, &["a,b,c", "1,3,0", "4,2,"])]
    fn simple_input(
        #[case] input: &str,
        #[case] expected: &[&str],
        #[values(true, false)] preserve_empty_arrays: bool,
        #[values(true, false)] preserve_empty_objects: bool,
    ) {
        let flattener = Flattener::new()
            .set_key_separator(".")
            .set_array_formatting(ArrayFormatting::Plain)
            .set_preserve_empty_arrays(preserve_empty_arrays)
            .set_preserve_empty_objects(preserve_empty_objects);
        let result = execute(input, &flattener);
        assert_eq!(result.output, expected.join("\n") + "\n");
    }

    #[test]
    fn duplicated_keys_last_wins() {
        let flattener = Flattener::new()
            .set_key_separator(".")
            .set_array_formatting(ArrayFormatting::Plain)
            .set_preserve_empty_arrays(true)
            .set_preserve_empty_objects(true);
        let result = execute(
            r#"{"a": [1,2,3], "a": {"b": 2}, "c": 1, "c": 2}"#,
            &flattener,
        );
        let expected = &["a.b,c", "2,2"];
        assert_eq!(result.output, expected.join("\n") + "\n");
    }

    /// We use internal separators that later are replaced by the user provided ones.
    /// This checks that the replacement does not make the headers and the data be in a different order.
    #[test]
    fn no_reordering_on_non_default_separators() {
        let flattener = Flattener::new()
            .set_key_separator("]")
            .set_array_formatting(ArrayFormatting::Surrounded {
                start: ".".to_string(),
                end: "".to_string(),
            })
            .set_preserve_empty_arrays(true)
            .set_preserve_empty_objects(true);
        let result = execute(r#"{"a": [1,2,3]} {"a": {"b": 2}}"#, &flattener);
        let expected = &["a.0,a.1,a.2,a]b", "1,2,3,", ",,,2"];
        assert_eq!(result.output, expected.join("\n") + "\n");
    }

    /// An error must be reported when flattening makes two keys in an object look the same.
    #[rstest]
    #[case::in_one_object(r#"{"a": {"b": 1}, "a.b": 2}"#)]
    #[case::in_different_objects(r#"{"a": {"b": 1}}{"a.b": 2}"#)]
    fn error_on_collision(#[case] input: &str) {
        let flattener = Flattener::new()
            .set_key_separator(".")
            .set_array_formatting(ArrayFormatting::Plain)
            .set_preserve_empty_arrays(false)
            .set_preserve_empty_objects(false);
        for err in execute_expect_err(input, &flattener) {
            assert!(
                matches!(err, Error::FlattenedKeysCollision),
                "Unexpected error: {}",
                err
            );
        }
    }

    /// An error must be reported when flattening makes two keys in an object look the same, even
    /// when it's due to array formatting.
    #[rstest]
    #[case::in_one_object(r#"{"a[0]": 1, "a": [2]}"#, "[", "]")]
    #[case::in_different_objects(r#"{"a[0]": 1} {"a": [2]}"#, "[", "]")]
    fn error_on_collision_array_formatting(
        #[case] input: &str,
        #[case] start: &str,
        #[case] end: &str,
    ) {
        let flattener = Flattener::new()
            .set_key_separator(".")
            .set_array_formatting(ArrayFormatting::Surrounded {
                start: start.to_string(),
                end: end.to_string(),
            })
            .set_preserve_empty_arrays(false)
            .set_preserve_empty_objects(false);
        for err in execute_expect_err(input, &flattener) {
            assert!(
                matches!(err, Error::FlattenedKeysCollision),
                "Unexpected error: {}",
                err
            );
        }
    }

    /// In all those cases there are no headers after flattening the input, so the resulting CSV is
    /// empty.
    #[rstest]
    #[case::empty_string("")]
    #[case::empty_json_doc("{}")]
    #[case::multiple_empty_json_docs("{}{}{}{}")]
    #[case::empty_array(r#"{"a": []}"#)]
    #[case::empty_obj(r#"{"b": {}}"#)]
    #[case::empty_array_obj_and_json_doc(r#"{"a": []} {"b": {}} {}"#)]
    fn empty_csv_when_no_headers(#[case] input: &str) {
        let expected = "";
        let flattener = Flattener::new()
            .set_key_separator(".")
            .set_array_formatting(ArrayFormatting::Plain)
            .set_preserve_empty_arrays(false)
            .set_preserve_empty_objects(false);
        let result = execute(input, &flattener);
        assert_eq!(result.output, expected);
    }

    #[rstest]
    #[case::empty_array(r#"{"a": []}"#)]
    #[case::empty_array_extra_obj(r#"{"a": []} {} {}"#)]
    #[case::empty_obj(r#"{"a": {}}"#)]
    #[case::empty_obj_extra_obj(r#"{"a": {}} {}"#)]
    fn preserved_empty(#[case] input: &str) {
        let flattener = Flattener::new()
            .set_key_separator(".")
            .set_array_formatting(ArrayFormatting::Plain)
            .set_preserve_empty_arrays(true)
            .set_preserve_empty_objects(true);
        let result = execute(input, &flattener);

        let mut expected = vec!["a"];

        // Extend the vector with as many rows as objects has the input
        expected.extend(vec![r#""""#; result.input.len()]);

        assert_eq!(result.output, expected.join("\n") + "\n");
    }

    #[rstest]
    #[case::empty_array(r#"{"a": [], "b": 3}"#, &["b", "3"])]
    #[case::empty_array_extra_obj(r#"{"a": [], "b": 3} {} {}"#, &["b", "3", r#""""#, r#""""#])]
    #[case::empty_obj(r#"{"a": {}, "b": 3}"#, &["b", "3"])]
    #[case::empty_obj_extra_obj(r#"{"a": {}} {} {"b": 3} {}"#, &["b", r#""""#, r#""""#, "3", r#""""#])]
    #[case::empty_obj_extra_obj(r#"{"a": {}} {} {"b": 3} {"c": 4}"#, &["b,c", ",", ",", "3,", ",4"])]
    fn not_preserved_empty(#[case] input: &str, #[case] expected: &[&str]) {
        let flattener = Flattener::new()
            .set_key_separator(".")
            .set_array_formatting(ArrayFormatting::Plain)
            .set_preserve_empty_arrays(false)
            .set_preserve_empty_objects(false);
        let result = execute(input, &flattener);

        assert_eq!(result.output, expected.join("\n") + "\n");
    }
}
