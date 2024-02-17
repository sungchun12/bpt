use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::sync::Mutex;

#[derive(Debug, Serialize, Deserialize)]
struct Column {
    name: String,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Model {
    name: String,
    columns: HashMap<String, Column>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    nodes: HashMap<String, Model>,
}

fn main() -> io::Result<()> {
    let file = File::open("test_artifacts/manifest.json")?;
    let reader = BufReader::new(file);
    let manifest: Manifest = serde_json::from_reader(reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let schema_path = File::create("schema.yml")?;
    let writer = BufWriter::new(schema_path);
    let writer_mutex = Mutex::new(writer);

    writeln!(writer_mutex.lock().unwrap(), "version: 2\n\nmodels:")?;

    manifest.nodes.par_iter().for_each(|(_, model)| {
        let mut model_schema = format!("  - name: {}\n    columns:\n", model.name);

        model.columns.iter().for_each(|(name, column)| {
            let column_schema = format!(
                "      - name: {}\n        tests:\n           - not_null\n           - unique\n{}",
                name,
                if column.tags.is_empty() {
                    String::new()
                } else {
                    format!("        tags: {:?}\n", column.tags)
                }
            );
            model_schema.push_str(&column_schema);
        });

        let mut writer = writer_mutex.lock().unwrap();
        writeln!(writer, "{}", model_schema).expect("Failed to write model schema");
    });

    // Ensure all data is flushed before ending the program
    writer_mutex.lock().unwrap().flush()?;

    Ok(())
}
