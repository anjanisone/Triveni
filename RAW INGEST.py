# --- Notebook A: RAW INGEST (SQL Server -> Delta landing) ---

from pyspark.sql import functions as F, types as T
from notebookutils import mssparkutils
import time

# ====== CONFIG ======
kv_uri = "https://dsi-key-vault.vault.azure.net/"
SERVER   = mssparkutils.credentials.getSecret(kv_uri, "stratus-big-data-server-name")
DATABASE = mssparkutils.credentials.getSecret(kv_uri, "stratus-big-data-database-name")
USER     = mssparkutils.credentials.getSecret(kv_uri, "stratus-big-data-user-name")
PASSWORD = mssparkutils.credentials.getSecret(kv_uri, "stratus-big-data-password")

REGISTRY_TABLE_PATH = "Tables/dbo/stratus_table_registry"   # delta path (NOT a SQL name)
LANDING_BASE_PATH   = "Tables/dbo"                          # base for raw tables
RAW_PREFIX          = "raw__"                               # raw table name prefix
MAX_FETCHSIZE       = "200000"

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def jdbc_url():
    return f"jdbc:sqlserver://{SERVER};databaseName={DATABASE};encrypt=true;trustServerCertificate=true"

def table_exists(delta_path: str) -> bool:
    try:
        spark.read.format("delta").load(delta_path).limit(1)
        return True
    except:
        return False

def build_incremental_clause(colname: str, last_max_value):
    # Build a simple > predicate; works for numeric & temporal types
    if last_max_value is None:
        return None
    # If stringy timestamp, pass through; rely on SQL Server to coerce
    if isinstance(last_max_value, (int, float)):
        return f"WHERE [{colname}] > {last_max_value}"
    else:
        return f"WHERE [{colname}] > '{last_max_value}'"

def get_last_max_from_raw(raw_path: str, inc_col: str):
    if not inc_col or not table_exists(raw_path):
        return None
    df = spark.read.format("delta").load(raw_path)
    if inc_col not in df.columns:
        # If the raw kept original casing, try a case-insensitive hit
        cols_ci = {c.lower(): c for c in df.columns}
        if inc_col.lower() in cols_ci:
            inc_col = cols_ci[inc_col.lower()]
        else:
            return None
    return df.agg(F.max(F.col(inc_col))).collect()[0][0]

def read_sql_table_or_query(src_table: str, where_sql: str | None):
    if where_sql:
        query = f"(SELECT * FROM [{src_table}] {where_sql}) q"
        return (spark.read.format("jdbc")
                .option("url", jdbc_url())
                .option("user", USER)
                .option("password", PASSWORD)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("dbtable", query)
                .option("fetchsize", MAX_FETCHSIZE)
                .load())
    else:
        return (spark.read.format("jdbc")
                .option("url", jdbc_url())
                .option("user", USER)
                .option("password", PASSWORD)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("dbtable", f"[{src_table}]")
                .option("fetchsize", MAX_FETCHSIZE)
                .load())

# ----- RUN -----
print("RAW INGEST: start")
reg = spark.read.format("delta").load(REGISTRY_TABLE_PATH).collect()

for e in reg:
    src_tbl  = e["source_table"]          # ex: "Parts"
    dst_tbl  = e["destination_table"]     # ex: "dsi_parts"
    inc_col  = getattr(e, "incremental_column", None)  # optional
    raw_path = f"{LANDING_BASE_PATH}/{RAW_PREFIX}{dst_tbl}"

    print(f"\n-- RAW: {src_tbl} -> {raw_path} --")
    last_max = get_last_max_from_raw(raw_path, inc_col) if inc_col else None
    where_sql = build_incremental_clause(inc_col, last_max) if inc_col else None

    t0 = time.time()
    df = read_sql_table_or_query(src_tbl, where_sql).withColumn("ingested_at", F.current_timestamp())
    rows = df.count()
    print(f"Read {rows:,} rows from {src_tbl} (incremental on {inc_col} = {last_max}).")

    if not table_exists(raw_path):
        df.write.mode("overwrite").format("delta").save(raw_path)
        print(f"Created landing table at {raw_path}")
    else:
        if rows:
            df.write.mode("append").format("delta").save(raw_path)
            print(f"Appended {rows:,} rows to {raw_path}")
        else:
            print("No new rows — skipped append.")

    print(f"Elapsed: {time.time()-t0:.1f}s")

print("\nRAW INGEST: complete.")
