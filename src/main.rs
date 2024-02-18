use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value; // Add missing import statement

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};

#[derive(Debug, Serialize, Deserialize)]
struct Column {
    name: String,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Model {
    name: String,
    columns: ModelColumns,
    compiled_code: String,
}

// Struct to hold column names extracted from compiled SQL after running query that returns column names as the result. We need to push these into the vector to be able to serialize it to YAML.
#[derive(Debug, Serialize, Deserialize)]
struct ModelColumns {
    column_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    nodes: HashMap<String, Value>,
}

pub enum SupportedAdapters {
    BigQuery,
    Postgres,
    Snowflake,
    Duckdb,
}

fn main() -> io::Result<()> {
    let file_path = "./jaffle_shop_duckdb/target/manifest.json";
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let manifest: Manifest = serde_json::from_reader(reader).unwrap();

    let model_prefix = Regex::new(r"^model\.").unwrap();

    manifest.nodes.par_iter().for_each(|(key, value)| {
        if model_prefix.is_match(key) {
            if let Ok(model) = serde_json::from_value::<Model>(value.clone()) {
                let column_names: ModelColumns = ModelColumns {
                    column_names: vec![
                        "column1".to_string(),
                        "column2".to_string(),
                        "column3".to_string(),
                    ],
                };
                let model_info: Model = Model {
                    name: model.name,
                    columns: column_names,
                    compiled_code: model.compiled_code,
                };
                println!("Model: {}", model_info.name);
                for column in &model_info.columns.column_names {
                    println!("column_name: {}", column);
                }
            } else if let Err(e) = serde_json::from_value::<Model>(value.clone()) {
                println!("Failed to deserialize value into Model, error: {:?}", e);
            }
        }
    });

    Ok(())
}
