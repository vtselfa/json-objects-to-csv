[![licence](https://img.shields.io/crates/l/json-objects-to-csv?style=flat-square)](https://github.com/vtselfa/json-objects-to-csv/blob/master/LICENSE.md)
[![crates.io](https://img.shields.io/crates/v/json-objects-to-csv?style=flat-square)](https://crates.io/crates/json-objects-to-csv)
[![docs.rs](https://img.shields.io/docsrs/json-objects-to-csv?style=flat-square)](https://docs.rs/json-objects-to-csv/latest/flatten_json_object/)

<!-- cargo-rdme start -->

## Robust Rust library for converting JSON objects into CSV rows

Given an array of JSON objects or a file that contains JSON objects one after the other, it
produces a CSV file with one row per JSON processed. In order to transform a JSON object into a
CSV row, this library "flattens" the objects, converting them into equivalent ones without nested
objects or arrays. The rules used for flattening objects are configurable, but by default an
object like this:

```json
{"a": {"b": [1,2,3]}}
```

is transformed into the flattened JSON object:

```json
{
  "a.b.0": 1,
  "a.b.1": 2,
  "a.b.2": 3
}
```

and then used to generate the following CSV output:
```csv
a.b.0,a.b.1,a.b.2
1,2,3
```

### Configuring output

This library relies on
[`flatten-json-object`](https://docs.rs/flatten-json-object/latest/flatten_json_object/)
for JSON object flattering and [`csv`](https://docs.rs/csv/latest/csv/) for CSV file generation.
Please check their respective documentation if you want to adjust how the output looks.

### Notes

- How objects are flattened and the CSV format (e.g. the field separator) can be configured.
- Each top level object in the input will be transformed into a CSV row.
- The headers are sorted alphabetically and are the union of all the keys in all the objects in
  the input after they are flattened.
- Key collisions after flattening the input will be reported as errors, i.e. if two objects have
  keys that should be different but end looking the same after flattening. For example,
  flattening a file that contains `{"a": {"b": 1}} {"a.b": 2}` results by default in an error.
- Any instance of `{}` (when not a top level object), `[]` or `Null` results in an empty CSV
  field.

### Example reading from a `Read` implementer

```rust
use csv;
use flatten_json_object::ArrayFormatting;
use flatten_json_object::Flattener;
use json_objects_to_csv::Json2Csv;
use std::io::{Read, Write};
use std::str;

// Anything supported by the `Flattener` object is possible.
let flattener = Flattener::new()
    .set_key_separator(".")
    .set_array_formatting(ArrayFormatting::Surrounded{
        start: "[".to_string(),
        end: "]".to_string()
    })
    .set_preserve_empty_arrays(false)
    .set_preserve_empty_objects(false);

// The output can be anything that implements `Write`. In this example we use a vector but
// this could be a `File`.
let mut output = Vec::<u8>::new();

// Anything that implements `Read`. Usually a file, but we will use a byte array in this example.
let input = r#"{"a": {"b": 1}} {"c": [2]} {"d": []} {"e": {}}"#.as_bytes();

// The CSV rows that we should get from this input and config. Note that since we are not
// preserving empty arrays or objects `d` and `e` are not part of the final headers.
// However, an empty row is generate for their object. If empty objects and arrays were
// preserved both `e` and `d` would be part of the headers, but their column would be empty.
let expected = ["a.b,c[0]", "1,", ",2", ",", ","];

// Here we can configure another field separator, like `;` or use any other CSV builder
// configuration.
let csv_writer = csv::WriterBuilder::new()
    .delimiter(b',')
    .from_writer(&mut output);

Json2Csv::new(flattener).convert_from_reader(input, csv_writer)?;

assert_eq!(str::from_utf8(&output)?, expected.join("\n") + "\n");
```

### Example converting a slice of JSON objects

```rust
use csv;
use flatten_json_object::ArrayFormatting;
use flatten_json_object::Flattener;
use json_objects_to_csv::Json2Csv;
use serde_json::json;
use std::str;

// We changed the array formatting and we preserve empty arrays and objects now, compared to
// the previous example.
let flattener = Flattener::new()
    .set_key_separator(".")
    .set_array_formatting(ArrayFormatting::Plain)
    .set_preserve_empty_arrays(true)
    .set_preserve_empty_objects(true);

// The output can be anything that implements `Write`. In this example we use a vector but
// this could be a `File`.
let mut output = Vec::<u8>::new();

let input = [
    json!({"a": {"b": 1}}),
    json!({"c": [2]}),
    json!({"d": []}),
    json!({"e": {}})
];

// This time the separator is `;`
let csv_writer = csv::WriterBuilder::new()
    .delimiter(b';')
    .from_writer(&mut output);

// The CSV rows that we should get from this input and config. We are preserving empty arrays
// and objects so `d` and `e` are part of the final headers. Since they are empty and no other
// object has those headers these columns have no value in any of the rows.
let expected = ["a.b;c.0;d;e", "1;;;", ";2;;", ";;;", ";;;"];

Json2Csv::new(flattener).convert_from_array(&input, csv_writer)?;

assert_eq!(str::from_utf8(&output)?, expected.join("\n") + "\n");
```

<!-- cargo-rdme end -->
