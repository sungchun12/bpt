use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value; // Add missing import statement

// use duckdb::prelude::*;
use duckdb::{Connection, Result};
// use duckdb::{params::NullIs, Config, Connection, Result}; // Add missing import statements // Add missing import statement

use std::collections::HashMap;
use std::error::Error;
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

trait QueryRunner {
    fn fetch_column_metadata(
        &self,
        model: &Model,
    ) -> Result<AllColumnMetadata, Box<dyn std::error::Error>>;
}

struct DuckDBQueryRunner {
    connection: Connection,
}

impl DuckDBQueryRunner {
    pub fn new(database_path: &str) -> Result<Self, Box<dyn Error>> {
        // Corrected: Directly using `Connection::open` from the duckdb crate
        let connection = Connection::open(database_path)?;

        Ok(Self { connection })
    }
}

impl QueryRunner for DuckDBQueryRunner {
    fn fetch_column_metadata(
        &self,
        model: &Model,
    ) -> Result<AllColumnMetadata, Box<dyn std::error::Error>> {
        let sql = format!(
            "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale \
             FROM system.information_schema.columns \
             WHERE table_name = '{}' \
             AND table_schema = '{}' \
             AND table_catalog = '{}' \
             ORDER BY ordinal_position;",
            model.name,
            model.schema,
            model.database,
        );
        println!("SQL: {}", sql); // Uncomment this line to see the SQL query that's being run (for debugging purposes only

        let mut stmt = self.connection.prepare(&sql)?;
        let column_metadata_results = stmt.query_map([], |row| {
            Ok(ColumnMetadata {
                column_name: row.get(0)?,
                data_type: row.get(1)?,
                character_maximum_length: row.get(2)?,
                numeric_precision: row.get(3)?,
                numeric_scale: row.get(4)?,
            })
        })?;

        let column_metadata: Vec<ColumnMetadata> =
            column_metadata_results.collect::<Result<_, _>>()?;

        println!("Query Results: {:?}", column_metadata); // Print query results for debugging

        Ok(AllColumnMetadata { column_metadata })
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
                let database_path = "./jaffle_shop_duckdb/jaffle_shop.duckdb"; // TODO: error handling if it doesn't find a database file
                let query_runner = DuckDBQueryRunner::new(database_path).unwrap();
                let column_metadata = query_runner.fetch_column_metadata(&node).unwrap();
                let column_metadata_result = ColumnMetadataResult {
                    column_metadata: {
                        let mut map = HashMap::new();
                        map.insert(node.name.clone(), column_metadata);
                        map
                    },
                };
                println!("Column Metadata Result: {:?}", column_metadata_result);
                println!("Model Name: {}", node.name);
                println!("Model database: {}", node.database);
                println!("Model Schema: {}", node.schema);
                println!("Model alias: {}", node.alias);
                println!("Model original_file_path: {}", node.original_file_path);
                // println!("Compiled Code: {:?}", node.compiled_code); // Uncomment this line to see the compiled code, but it's too long to print for regular debugging

                // TODO: take the column_metadata_result and serialize it to YAML based on the original_file_path of the node
                // The YAML file should be named the same as the original_file_path of the node, but with a _schema.yml extension. Example: `models/orders.sql` would be stored in this directory `models/orders_schema.yml` within the subdirectory of the first argument of this path: "./jaffle_shop_duckdb/target/manifest.json" which is "./jaffle_shop_duckdb/" . This is the actual files name: orders_schema.yml
                // The structuring of the YAML file should be like this: in the schema_template.yml file

                let yaml = serde_yaml::to_string(&column_metadata_result).unwrap();
                println!("YAML: {}", yaml);
                let file_path = format!(
                    "./jaffle_shop_duckdb/{}_schema.yml",
                    node.original_file_path.trim_end_matches(".sql")
                );
                let file = File::create(file_path).unwrap(); // Unwrap the File from the Result
                serde_yaml::to_writer(&file, &column_metadata_result).unwrap(); // Pass the unwrapped File to serde_yaml::to_writer
                println!("YAML: {}", yaml);
            }
        });

    Ok(())
}
