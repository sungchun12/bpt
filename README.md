# bpt
blueprint: write and update schemas for your dbt models, less time doing toil == more time feeling royal

## Technical Notes

> Naive implementation of the `bpt` tool. This is a first pass at the tool and is not optimized for performance. It is meant to be a proof of concept and a starting point for future development. This assumes no existing schema files for now.

1. Read in the `manifest.json` file from the dbt run within the `target/` directory. Assumes this is being run from the root of the dbt project.
2. Parse the `manifest.json` file to get the `nodes` and `compiled_code` keys and store those in a `HashSet` for quick lookups.
3. Run the queries concurrenctly to get the schema for each model from the database.
4. For each model, write the schema to a file in the `schemas/` directory with the name of the node and the schema as the contents. Ex: `dim_orgs_schema.yml`

