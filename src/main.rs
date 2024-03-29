use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value; // Add missing import statement

// use duckdb::prelude::*;
// use duckdb::Connection;
// use duckdb::{params::NullIs, Config, Connection, Result}; // Add missing import statements // Add missing import statement

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};

// TODO: probably don't need this
// #[derive(Debug, Serialize, Deserialize)]
// struct Column {
//     name: String,
//     #[serde(default)]
//     tags: Vec<String>,
// }

// having the `columns` field did NOT work because there are times where the `columns` field is not present in the JSON. So, we need to define that later
#[derive(Debug, Serialize, Deserialize)]
struct Model {
    name: String,
    resource_type: String,
    compiled_code: String,
    database: String,
    schema: String,
    original_file_path: String,
    alias: String,
}

// Struct to hold column names extracted from compiled SQL after running query that returns column names as the result. We need to push these into the vector to be able to serialize it to YAML.
// TODO: I think we can remove this struct and just use a vector of strings
#[derive(Debug, Serialize, Deserialize)]
struct ModelColumns {
    column_names: Vec<String>,
}

// this will store the values from the query result that returns the column names and data types
#[derive(Debug, Serialize, Deserialize)]
struct ColumnMetadata {
    column_name: String,
    data_type: String,
    character_maximum_length: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
}
#[derive(Debug, Serialize, Deserialize)]
struct AllColumnMetadata {
    column_metadata: Vec<ColumnMetadata>,
}

// this will store the results of the query that returns the column names and data types and will be serialized to YAML the key will be the model name and the value will be a hashmap of the column names and data types
#[derive(Debug, Serialize, Deserialize)]
struct ColumnMetadataResult {
    column_metadata: HashMap<String, AllColumnMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    dbt_version: String,
    generated_at: String,
    adapter_type: String,
}

// TODO: implement a method on nodes to only read in nodes with resource_type = "model"
#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    metadata: Metadata,
    nodes: HashMap<String, Value>,
}

// TODO: if the adapter_type field matches these values, we can run the query to get the column names and then it'll dynamically switch the database query runner that's supported#

#[derive(Debug, PartialEq, Eq)]
pub enum SupportedAdapters {
    BigQuery,
    Postgres,
    Snowflake,
    Duckdb,
}

impl SupportedAdapters {
    fn from_str(adapter_type: &str) -> Option<Self> {
        match adapter_type.to_lowercase().as_str() {
            "bigquery" => Some(Self::BigQuery),
            "postgres" => Some(Self::Postgres),
            "snowflake" => Some(Self::Snowflake),
            "duckdb" => Some(Self::Duckdb),
            _ => None,
        }
    }
}

fn main() -> io::Result<()> {
    let file_path = "./jaffle_shop_duckdb/target/manifest.json";
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let manifest: Manifest = serde_json::from_reader(reader).unwrap();

    if let Some(adapter_type_enum) = SupportedAdapters::from_str(&manifest.metadata.adapter_type) {
        println!("Adapter Type: {:?}", adapter_type_enum);
    } else {
        println!("Unsupported adapter type");
    }

    manifest
        .nodes
        .par_iter()
        .filter_map(|(_, value)| {
            let node: Result<Model, _> = serde_json::from_value(value.clone());
            node.ok()
        })
        .for_each(|node| {
            // TODO: run a query and store the results in this struct
            // Run the compiled query to get the column names and data types. It should be an empty table result based on the compiled_code associated with this model and should work even if the table isn't officially materialized yet.
            // verify the duckdb adapter is supported with the above logic
            // if it is, then run the query and store the results in the struct
            // The struct should be a hashmap with the key being the model name and the value being a hashmap of the column names and data types

            if node.resource_type == "model" {
                let column_names: ModelColumns = ModelColumns {
                    column_names: vec![
                        "column1".to_string(),
                        "column2".to_string(),
                        "column3".to_string(),
                    ],
                };
                println!("Model Name: {}", node.name);
                println!("Model database: {}", node.database);
                println!("Model Schema: {}", node.schema);
                println!("Model alias: {}", node.alias);
                println!("Model original_file_path: {}", node.original_file_path);
                println!("Columns: {:?}", column_names);
                // println!("Compiled Code: {:?}", node.compiled_code); // Uncomment this line to see the compiled code, but it's too long to print for regular debugging
            }
        });

    Ok(())
}
