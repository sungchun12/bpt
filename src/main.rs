use clap::{App, Arg};
use duckdb::{Connection, Result as DuckResult};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Write};

#[derive(Debug, Serialize, Deserialize)]
struct Column {
    name: String,
    data_type: String,
    description: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Model {
    database: String,
    schema: String,
    name: String,
    compiled_code: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    nodes: HashMap<String, Value>,
}

fn main() -> DuckResult<()> {
    let matches = App::new("DBT Schema Extractor")
        .version("1.0")
        .author("Your Name")
        .about("Extracts schema information from DBT models for DuckDB")
        .arg(
            Arg::new("manifest")
                .about("Sets the input file to use")
                .value_name("FILE")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let manifest_path = matches.value_of("manifest").unwrap();
    let file = File::open(manifest_path)?;
    let reader = BufReader::new(file);
    let manifest: Manifest = serde_json::from_reader(reader)?;

    manifest.nodes.par_iter().for_each(|(key, value)| {
        if key.starts_with("model.") {
            if let Ok(model) = serde_json::from_value::<Model>(value.clone()) {
                let connection_string = format!("{}::memory:", model.database);
                let conn = Connection::open(&connection_string).expect("Failed to connect to DuckDB");

                let query = format!(
                    "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = '{}' AND table_schema = '{}' ORDER BY ordinal_position",
                    model.name, model.schema
                );

                let mut stmt = conn.prepare(&query).expect("Failed to prepare query");
                let rows = stmt.query([]).expect("Failed to execute query");

                let mut columns: Vec<Column> = vec![];
                for row in rows {
                    let column = Column {
                        name: row.get("column_name").unwrap(),
                        data_type: row.get("data_type").unwrap(),
                        character_maximum_length: row.get("character_maximum_length").ok().flatten(),
                        numeric_precision: row.get("numeric_precision").ok().flatten(),
                        numeric_scale: row.get("numeric_scale").ok().flatten(),
                    };
                    columns.push(column);
                }

                let schema = serde_yaml::to_string(&columns).expect("Failed to serialize schema to YAML");
                let schema_file_path = format!("schemas/{}_schema.yml", model.name);
                let mut schema_file = File::create(&schema_file_path).expect("Failed to create schema file");
                schema_file.write_all(schema.as_bytes()).expect("Failed to write schema to file");
            }
        }
    });

    Ok(())
}
