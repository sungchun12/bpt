use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::Write;

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

fn main() -> std::io::Result<()> {
    let mut file = File::open("test_artifacts/manifest.json")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let manifest: Manifest = serde_json::from_str(&contents).unwrap();

    let mut out_file = File::create("schema.yml")?;
    write!(out_file, "version: 2\n\nmodels:\n").unwrap();

    for (_key, model) in manifest.nodes.iter() {
        write!(out_file, "  - name: {}\n    columns:\n", model.name).unwrap();
        for (name, column) in model.columns.iter() {
            write!(
                out_file,
                "      - name: {}\n        tests:\n           - not_null\n           - unique\n",
                name
            )
            .unwrap();
            if !column.tags.is_empty() {
                write!(out_file, "        tags: {:?}", column.tags).unwrap();
            }
        }
    }

    Ok(())
}
