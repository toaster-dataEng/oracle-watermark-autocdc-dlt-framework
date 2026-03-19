# oracle-watermark-autocdc-dlt-framework

Cell-by-Cell Explanation
Cell 2 — Utilities (%run)
Executes the shared utility notebook Oracle_Connection, which defines three helper functions:

get_oracle_connection_details() — fetches Oracle JDBC credentials (host, SID, user, password) from Azure Key Vault via dbutils.secrets.
get_tracker_table_schema() — returns the fully qualified name and Spark schema for the watermark tracker table (oracle_scn_tracker).
get_target_catalog_schema() — returns the Unity Catalog catalog + schema for Bronze target tables.
Running this cell makes those functions (and the pre-built conn_details dict) available to every subsequent cell.

Cell 3 — Retrieves Data from Helper Functions
Calls the three utility functions and stores results in local variables:

dbc — Oracle connection dictionary (hostname, SID, credentials, driver class).
tracker_info — dict with the tracker table name and its StructType schema.
catalog, schema — the target Bronze catalog (dna_dev_accenture_cdc_sandbox) and schema (human_resources).
These variables are referenced by all downstream cells.

Cell 4 — select control
A diagnostic / verification cell. It simply displays the full contents of the watermark tracker table so you can visually inspect which tables are registered, their last watermark timestamps, primary keys, and version numbers.

Cell 5 — For Testing - select data Oracle
A testing / exploration cell. It loops through every tracked table and reads from Oracle via JDBC, but instead of ingesting data it only fetches MAX(partition_column) to verify connectivity and see the latest available timestamp in Oracle. Contains several commented-out query variants used during development. Not part of the production flow.

Cell 6 — Ensure CDF Enabled on Bronze Tables
A pre-flight check that iterates through every tracked table and:

Skips tables whose Bronze Delta table doesn't exist yet.
For existing tables, reads SHOW TBLPROPERTIES to check if delta.enableChangeDataFeed is true.
If CDF is not enabled, runs ALTER TABLE ... SET TBLPROPERTIES to turn it on.
This ensures downstream DLT APPLY CHANGES pipelines can consume row-level changes from Bronze.

Cell 7 — Bronze Ingestion - Loop through ⭐ Main Production Cell
The core ingestion logic. For every table in the tracker:

Step 1 — Read from Oracle:

Determines mode: FULL LOAD (watermark = 1900-01-01) or INCREMENTAL (watermark > last run).
Builds a JDBC sub-query with an Oracle TO_TIMESTAMP filter for incremental reads.
Reads via spark.read.format("jdbc") with fetchSize=10000 and an Oracle NLS session init.
Step 2 — Transform & MERGE into Bronze:

Adds _ingest_ts (current timestamp) and _row_hash (SHA-256 of non-date columns).
Persists the DataFrame to avoid re-reading Oracle on multiple actions.
If the target table doesn't exist → creates it with overwrite mode + CDF + auto-optimize.
If the target exists → performs a Delta MERGE:
Joins on primary keys from the tracker (PKeys column).
Uses _row_hash to skip unchanged rows (hash comparison against existing Bronze data).
whenMatchedUpdate for changed rows, whenNotMatchedInsertAll for new rows.
Step 3 — Update Watermark:

Computes MAX(partition_column) from the Oracle data just read.
Counts total rows in the Bronze table.
Increments version_number and writes the new watermark back to the tracker table via UPDATE.
Cell 8 — Bronze Ingestion (single-table draft)
An earlier draft of the ingestion logic that operates on a single hardcoded table (ESMT_DOC_LINE). It reads from Oracle, displays the data, and was the precursor to the loop in Cell 7. Contains commented-out partitioning logic. Superseded by Cell 7.

Cell 9 — Load to Bronze (single-table draft)
Another earlier draft that handles the Bronze write + MERGE for a single hardcoded table (ESMT_DOC_LINE). Same pattern as Cell 7's Step 2 — adds _ingest_ts / _row_hash, creates or merges into the target table. Superseded by Cell 7.

Cell 10 — Update Watermark Tracker (single-table draft, %skip)
The watermark update logic for a single table, marked with %skip so it won't run. Reads MAX(DOC_LAST_DT), increments version_number, and updates the tracker. Includes a verification query to display the updated tracker row. Superseded by Cell 7's Step 3.

Execution Flow Summary
For production, only cells 2 → 3 → 4 (optional) → 6 → 7 need to run in order:

[Cell 2: Load utilities] → [Cell 3: Init variables] → [Cell 6: CDF pre-flight] → [Cell 7: Ingest all tables]
Cells 4, 5, 8, 9, and 10 are development/testing artifacts.
