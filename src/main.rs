use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::Path;

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
    compiled_code: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    nodes: HashMap<String, Value>,
}

fn main() -> io::Result<()> {
    let file_path = "./test_artifacts/manifest.json";
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let manifest: Manifest = serde_json::from_reader(reader).unwrap();

    let model_prefix = Regex::new(r"^model\.").unwrap();
    let dialect = GenericDialect {};

    manifest.nodes.par_iter().for_each(|(key, value)| {
        if model_prefix.is_match(key) {
            if let Ok(model) = serde_json::from_value::<Model>(value.clone()) {
                let parsed_sql = Parser::parse_sql(&dialect, &model.compiled_code).expect("Failed to parse SQL");
                let schema_file_path = format!("{}_schema.yml", model.name);
                let schema_path = Path::new(&schema_file_path);

                if let Some(parent) = schema_path.parent() {
                    fs::create_dir_all(parent).expect("Failed to create directories for schema file");
                }

                let schema_file = File::create(schema_file_path).expect("Failed to create schema file");
                let mut writer = BufWriter::new(schema_file);

                writeln!(writer, "version: 2\n\nmodels:").expect("Failed to write header");
                writeln!(writer, "  - name: {}", model.name).expect("Failed to write model name");
                writeln!(writer, "    columns:").expect("Failed to write columns");

                for column in model.columns.values() {
                    writeln!(
                        writer,
                        "      - name: {}\n        tests:\n          - not_null\n          - unique\n        tags: {:?}",
                        column.name, column.tags
                    ).expect("Failed to write column schema");
                }

                // Placeholder for detailed SQL AST processing
                for _stmt in parsed_sql {
                    // Implement detailed AST processing here if needed
                }

                writer.flush().expect("Failed to flush writer");
            }
        }
    });

    Ok(())
}