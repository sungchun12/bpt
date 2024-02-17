use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::Path;
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};

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

fn extract_column_names(sql: &str) -> Vec<String> {
    let dialect = GenericDialect {};
    let mut column_names = Vec::new();

    match Parser::parse_sql(&dialect, sql) {
        Ok(parsed_sql) => {
            for statement in parsed_sql {
                if let Statement::Query(query) = statement {
                    if let Some(with) = &query.with {
                        for cte in &with.cte_tables {
                            if let SetExpr::Select(select) = &*cte.query.body {
                                for item in &select.projection {
                                    match item {
                                        SelectItem::UnnamedExpr(expr) => {
                                            if let Expr::Identifier(ident) = expr {
                                                column_names.push(ident.value.clone());
                                            }
                                        },
                                        SelectItem::ExprWithAlias { alias, .. } => {
                                            column_names.push(alias.value.clone());
                                        },
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Err(e) => eprintln!("Failed to parse SQL: {}", e),
    }

    column_names
}

fn main() -> io::Result<()> {
    let file_path = "./test_artifacts/manifest.json";
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let manifest: Manifest = serde_json::from_reader(reader).unwrap();

    let model_prefix = Regex::new(r"^model\.").unwrap();

    manifest.nodes.par_iter().for_each(|(key, value)| {
        if model_prefix.is_match(key) {
            if let Ok(model) = serde_json::from_value::<Model>(value.clone()) {
                let column_names = extract_column_names(&model.compiled_code);
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

                let mut written_columns = HashSet::new();
                
                for (column_name, column) in &model.columns {
                    if written_columns.insert(column_name.clone()) {
                        writeln!(
                            writer,
                            "      - name: {}\n        tests:\n          - not_null\n          - unique\n        tags: {:?}",
                            column_name, column.tags
                        ).expect("Failed to write column schema");
                    }
                }

                for column_name in column_names {
                    if written_columns.insert(column_name.clone()) {
                        writeln!(writer, "      - name: {}", column_name).expect("Failed to write additional column schema");
                    }
                }

                writer.flush().expect("Failed to flush writer");
            }
        }
    });

    Ok(())
}