# Open.Ink US Voter Roll Normalization

The Open.Ink US Voter Roll Normalization project aims to consolidate
disparate voter registration lists from various sources across
the United States into a single, normalized structure.
By merging these datasets, we aim to create a comprehensive and standardized
database that facilitates efficient analysis and querying.

## Why do this?

- Data Cleaning and Standardization: Normalize the collected data to ensure consistency in formatting, resolving
  discrepancies, and standardizing field names and data types.
- Schema Design: Design a unified schema that accommodates the merged data while maintaining flexibility for future
  updates and expansions.
- Documentation and Deployment: Document the entire process, including data sources, transformations, and schema
  definitions, to facilitate reproducibility and future maintenance. Deploy the finalized dataset for analysis and
  reporting purposes.

## Getting Started

This project uses DBT to normalize the voter rolls in Snowflake. We use dagster to orchestrate loading
the data files and running our dbt model. The steps below will get you started

### Obtaining and loading the voter registration lists

You will have to reach out to the states themselves to obtain the voter lists. The scripts to create the
raw voter tables are in the `scripts` directory.

You can load the data into these tables however you like. We recommend either loading the files
into an external stage in Snowflake and using the 'COPY INTO' command or using a tool
like [Sling](https://slingdata.io/)

### Running the project

Navigate into the project directory and create a python virtual environment and install the project dependencies.
You can do this by running

```bash
python -m venv .venv
pip install -f requirements.txt
```

Navigate to the src directory and run the project by running

```bash
dbt build
```

That's it! the voter files will be normalized and loaded into the cleaned.all_voters and cleaned.all_voter_history
tables.

## Frequently Asked Questions

### How to do a partial load

We're working on a more intuitive way to execute only a single state. It is in our roadmap. For now, navigate
to `models/all_voters.sql` and `models/all_voter_history.sql` and remove the lines that reference the states that you
don't have.

For example, if you only have data from Georgia and Michigan, update the all_voters.sql file to the following

```sql
{{ add_table_to_all_voters(ref('georgia_voters')) }}
UNION
{{ add_table_to_all_voters(ref('michigan_voters')) }}
```

Update the all_voter_history.sql to the following

```sql
{{ add_table_to_voter_history(ref("georgia_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("michigan_voter_history")) }}
```

### Have you loaded all 50 states?

Our goal is to process all 50 states, however there are a few states that we have not normalized
we are unable to get the data

1. North Dakota
2. Massachusetts
3. Kentucky
4. Indiana
5. Minnesota

We will publish updates to this repository if we get updates to raw data of those states.

### How do I load multiple files for the same state

Simple!, Load the data into snowflake and then open the `cleaned/{state_name}_voters` file and add a UNION statement to
merge the 2 data sets. Our system is capable of handling multiple versions of the file for a single state.

## Roadmap

- Make it easier to load the data into snowflake.
- Allow loading individual states for users who don't have all the data.