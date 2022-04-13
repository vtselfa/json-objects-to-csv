use serde_json::{Deserializer, Value};
use std::collections::BTreeSet;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::{Read, Write};
use tempfile::tempfile;

pub use csv;
pub use error::Error;
pub use flatten_json_object;

mod error;

pub fn json_objects_array_to_csv(
    objects: &[Value],
    flattener: &flatten_json_object::Flattener,
    mut csv_writer: csv::Writer<impl Write>,
) -> Result<(), error::Error> {
    // We have to flatten the JSON object sine there is no other way to convert nested objects to CSV
    let mut flat_maps = Vec::<serde_json::value::Map<String, Value>>::new();

    // We use the unit separator character (a control character) to be able to detect collisions
    // like the one that happens when converting `[{"a": {"b": 1}} {"a.b": 2}]` with a `.` separator.
    let key_separator_orig = flattener.key_separator();
    let key_separator_repl = "␟";
    let flattener = flattener.clone().set_key_separator(key_separator_repl);
    for obj in objects {
        let obj = flattener.flatten(obj)?;
        if let Value::Object(map) = obj {
            flat_maps.push(map);
        } else {
            unreachable!("Flattening a JSON object always produces a JSON object");
        }
    }
    let flat_maps = flat_maps;

    // The headers are the union of the keys of the flattened objects, sorted
    let mut headers = BTreeSet::<String>::new();
    for map in &flat_maps {
        for key in map.keys() {
            if !headers.contains(key) {
                headers.insert(key.to_string());
            }
        }
    }

    // If we could not extract headers there is nothing to write to the CSV file
    if headers.is_empty() {
        return Ok(());
    }

    // Check that there are no collisions between flattened keys in different objects
    let headers_row: BTreeSet<String> = headers
        .clone()
        .into_iter()
        .map(|x| x.replace(key_separator_repl, key_separator_orig))
        .collect();
    if headers.len() != headers_row.len() {
        return Err(Error::FlattenedKeysCollision);
    }

    csv_writer.write_record(headers_row)?;
    for map in flat_maps {
        csv_writer.write_record(build_record(&headers, map))?;
    }

    Ok(())
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

// The file must contain JSON objects one immediately after the other or separated by whitespace.
pub fn json_objects_from_file_to_csv(
    reader: impl Read,
    flattener: &flatten_json_object::Flattener,
    mut csv_writer: csv::Writer<impl Write>,
) -> Result<(), error::Error> {
    // We have to flatten the JSON objects into a file because it can potentially be a really big
    // stream. We cannot directly convert into CSV because we cannot be sure about all the objects
    // resulting in the same headers.
    let mut tmp_file = tempfile()?;

    // We use the unit separator character (a control character) to be able to detect collisions
    // like the one that happens when converting `[{"a": {"b": 1}} {"a.b": 2}]` with a `.` separator.
    let key_separator_orig = flattener.key_separator();
    let key_separator_repl = "␟";
    let flattener = flattener.clone().set_key_separator(key_separator_repl);

    // The headers are the union of the keys of the flattened objects, sorted
    let mut headers = BTreeSet::<String>::new();

    for obj in Deserializer::from_reader(reader).into_iter::<Value>() {
        let obj = obj?; // Ensure that we can parse the input properly
        let obj = flattener.flatten(&obj)?;

        let map = match obj {
            Value::Object(ref map) => map,
            _ => unreachable!("Flattening a JSON object always produces a JSON object"),
        };
        for key in map.keys() {
            if !headers.contains(key) {
                headers.insert(key.to_string());
            }
        }
        serde_json::to_writer(&mut tmp_file, &obj)?;
    }
    tmp_file.seek(SeekFrom::Start(0))?;

    // If we could not extract headers there is nothing to write to the CSV file
    if headers.is_empty() {
        return Ok(());
    }

    // Check that there are no collisions between flattened keys in different objects
    let headers_row: BTreeSet<String> = headers
        .clone()
        .into_iter()
        .map(|x| x.replace(key_separator_repl, key_separator_orig))
        .collect();
    if headers.len() != headers_row.len() {
        return Err(Error::FlattenedKeysCollision);
    }

    csv_writer.write_record(headers_row)?;
    for obj in Deserializer::from_reader(tmp_file).into_iter::<Value>() {
        let map = match obj? {
            Value::Object(map) => map,
            _ => unreachable!("Flattening a JSON object always produces a JSON object"),
        };
        csv_writer.write_record(build_record(&headers, map))?;
    }

    Ok(())
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

        let result_from_file =
            json_objects_from_file_to_csv(input.as_bytes(), flattener, csv_writer_from_file);

        let input_from_array: Result<Vec<_>, _> =
            Deserializer::from_str(input).into_iter::<Value>().collect();
        let input_from_array = input_from_array.unwrap();

        let mut output_from_array = Vec::<u8>::new();
        let csv_writer_from_array = csv::WriterBuilder::new()
            .delimiter(b',')
            .from_writer(&mut output_from_array);
        let result_from_array =
            json_objects_array_to_csv(&input_from_array, flattener, csv_writer_from_array);

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

        json_objects_from_file_to_csv(input.as_bytes(), flattener, csv_writer_from_file).unwrap();

        let input_from_array: Result<Vec<_>, _> =
            Deserializer::from_str(input).into_iter::<Value>().collect();
        let input_from_array = input_from_array.unwrap();

        let mut output_from_array = Vec::<u8>::new();
        let csv_writer_from_array = csv::WriterBuilder::new()
            .delimiter(b',')
            .from_writer(&mut output_from_array);
        json_objects_array_to_csv(&input_from_array, flattener, csv_writer_from_array).unwrap();

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
                "Unexpected error"
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
