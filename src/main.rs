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

// having the `columns` field did NOT work because there are times where the `columns` field is not present in the JSON. So, we need to define that later
#[derive(Debug, Serialize, Deserialize)]
struct Model {
    name: String,
    compiled_code: String,
}

// Struct to hold column names extracted from compiled SQL after running query that returns column names as the result. We need to push these into the vector to be able to serialize it to YAML.
#[derive(Debug, Serialize, Deserialize)]
struct ModelColumns {
    column_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    dbt_version: String,
    generated_at: String,
    adapter_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    metadata: Metadata,
    nodes: HashMap<String, Value>,
}

// TODO: if the adapter_type field matches these values, we can run the query to get the column names and then it'll dynamically switch the database query runner that's supported
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

    if manifest.metadata.adapter_type == "duckdb".to_string() {
        println!("Adapter type is DuckDB");
    };

    manifest.nodes.par_iter().for_each(|(key, value)| {
        if model_prefix.is_match(key) {
            if let Ok(model) = serde_json::from_value::<Model>(value.clone()) {
                // TODO: Add code to run the query and get the column names
                let column_names: ModelColumns = ModelColumns {
                    column_names: vec![
                        "column1".to_string(),
                        "column2".to_string(),
                        "column3".to_string(),
                    ],
                };
                let model_info: Model = Model {
                    name: model.name,
                    compiled_code: model.compiled_code,
                };
                println!("Model Name: {}", model_info.name);
                // println!("Compiled Code: {}", model_info.compiled_code);
                println!("Columns: {:?}", column_names);
            } else if let Err(e) = serde_json::from_value::<Model>(value.clone()) {
                println!("Failed to deserialize value into Model, error: {:?}", e);
            }
        }
    });

    Ok(())
}
