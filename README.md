# bpt
blueprint: write and update schemas for your dbt models, less time doing toil == more time feeling royal

## Technical Notes

> Naive implementation of the `bpt` tool. This is a first pass at the tool and is not optimized for performance. It is meant to be a proof of concept and a starting point for future development. This assumes no existing schema files for now.

1. Read in the `manifest.json` file from the dbt run within the `target/` directory. Assumes this is being run from the root of the dbt project.
2. Parse the `manifest.json` file to get the `nodes` and `compiled_code` keys and store those in a `HashSet` for quick lookups.
- This step should store the `compiled_code` for each model in a `HashMap` with the key being the `id` of the model along with these other keys nested: `database`, `schema`, `name`. This information will be used to run the queries to get the schema for each model.
3. Run the queries concurrenctly to get the schema for each model from the database. This assumes the table/view already exists in the database. 
4. For each model, write the schema to a file in the `schemas/` directory with the name of the node and the schema as the contents. Ex: `dim_orgs_schema.yml`


Alright, Sung, now you get that you can't one shot this code with chatgpt. You're going to have to do it hard way (read: right way) and build it up piece by piece. 

Remember, this is less "look how cool my rust code is" and more "look how fast I can solve this problem for you". 