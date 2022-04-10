use serde_json::{Deserializer, Value};
use std::collections::BTreeSet;
use std::error::Error;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::{Read, Write};
use tempfile::tempfile;

pub use csv;
pub use flatten_json_object as flattener;

pub fn json_objects_array_to_csv(
    objects: &[Value],
    writer: impl Write,
    delimiter: u8,
    key_separator: &str,
    array_formatting: flatten_json_object::ArrayFormatting,
) -> Result<(), Box<dyn Error>> {
    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .from_writer(writer);

    let flattener = flattener::Flattener::new()
        .set_key_separator(key_separator)
        .set_array_formatting(array_formatting)
        .set_preserve_empty_arrays(false)
        .set_preserve_empty_objects(false);

    // We have to flatten the JSON object sine there is no other way to convert nested objects to CSV
    let mut flat_maps = Vec::<serde_json::value::Map<String, Value>>::new();
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
    let headers: Vec<_> = headers.into_iter().collect();

    csv_writer.write_record(&headers)?;
    for mut map in flat_maps {
        let mut record: Vec<String> = vec![];
        for header in &headers {
            if let Some(val) = map.remove(header) {
                match val {
                    Value::String(s) => record.push(s),
                    _ => record.push(val.to_string()),
                }
            } else {
                record.push("".to_string());
            }
        }
        csv_writer.write_record(record)?;
    }

    Ok(())
}

// The file must contain JSON objects one immediately after the other or separated by whitespace.
pub fn json_objects_from_file_to_csv(
    reader: impl Read,
    writer: impl Write,
    delimiter: u8,
    key_separator: &str,
    array_formatting: flatten_json_object::ArrayFormatting,
) -> Result<(), Box<dyn Error>> {
    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .from_writer(writer);

    let flattener = flattener::Flattener::new()
        .set_key_separator(key_separator)
        .set_array_formatting(array_formatting)
        .set_preserve_empty_arrays(false)
        .set_preserve_empty_objects(false);

    // We have to flatten the JSON objects into a file because it can potentially be a really big stream.
    // We cannot directly convert into CSV because we cannot be sure about all the objects
    // resulting in the same headers.
    let mut tmp_file = tempfile()?;

    // The headers are the union of the keys of the flattened objects, sorted
    let mut headers = BTreeSet::<String>::new();

    for obj in Deserializer::from_reader(reader).into_iter::<Value>() {
        let obj = flattener.flatten(&obj?)?;
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

    let headers: Vec<_> = headers.into_iter().collect();

    tmp_file.seek(SeekFrom::Start(0))?;

    csv_writer.write_record(&headers)?;
    for obj in Deserializer::from_reader(tmp_file).into_iter::<Value>() {
        let mut map = match obj? {
            Value::Object(map) => map,
            _ => unreachable!("Flattening a JSON object always produces a JSON object"),
        };
        let mut record: Vec<String> = vec![];
        for header in &headers {
            if let Some(val) = map.remove(header) {
                match val {
                    Value::String(s) => record.push(s),
                    _ => record.push(val.to_string()),
                }
            } else {
                record.push("".to_string());
            }
        }
        csv_writer.write_record(record)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // use super::Flattener;
    use super::*;
    use std::str;

    #[test]
    fn simple_file_to_csv() {
        let input = r#"{"a": {"b": 1}}{"c": [2]}"#;
        let mut output = Vec::<u8>::new();

        json_objects_from_file_to_csv(
            input.as_bytes(),
            &mut output,
            b',',
            ".",
            flatten_json_object::ArrayFormatting::Plain,
        )
        .unwrap();

        let csv = str::from_utf8(&output).unwrap();

        assert_eq!(
            csv,
            "a.b,c.0\n\
             1,\n\
             ,2\n"
        );
    }
}
