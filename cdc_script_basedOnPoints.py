from pyspark.sql import functions as F, types as T
from datetime import datetime

# ============================
# CONFIGURATION
# ============================
CATALOG_SCHEMA      = "dbo"
CONN_REGISTRY_TABLE = f"{CATALOG_SCHEMA}.vp_connection_registry"
REGISTRY_TABLE      = f"{CATALOG_SCHEMA}.vp_cdc_table_registry"
WATERMARK_TABLE     = f"{CATALOG_SCHEMA}.vp_cdc_watermark"

# -----------------------------------------------------------
# 1. ** Extract CDC Tables **
# -----------------------------------------------------------
# Read from SQL Server system CDC tables via JDBC
def read_sql(jdbc_url, user, pwd, sql_text):
    return (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("user", user)
        .option("password", pwd)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("dbtable", f"({sql_text}) AS x")
        .load())

sql_change_tables = """
SELECT
  s.name  AS source_schema,
  t.name  AS source_table,
  ct.capture_instance,
  ct.start_lsn,
  ct.supports_net_changes
FROM cdc.change_tables ct
JOIN sys.tables t    ON t.object_id = ct.source_object_id
JOIN sys.schemas s   ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0;
"""
change_tables_df = read_sql(jdbc_url, user, pwd, sql_change_tables)

# -----------------------------------------------------------
# 2. ** Register Tables **
# -----------------------------------------------------------
sql_captured_cols = """
SELECT
  cc.capture_instance,
  c.name AS column_name,
  c.column_id
FROM cdc.captured_columns cc
JOIN cdc.change_tables   ct ON ct.capture_instance = cc.capture_instance
JOIN sys.columns         c  ON c.object_id = ct.source_object_id AND c.column_id = cc.column_id
ORDER BY cc.capture_instance, c.column_id;
"""
cols_df = read_sql(jdbc_url, user, pwd, sql_captured_cols)

cols_agg = (cols_df.groupBy("capture_instance")
    .agg(F.collect_list(F.struct("column_id","column_name")).alias("cols"))
    .withColumn("cols", F.expr("transform(array_sort(cols), x -> x.column_name)"))
    .withColumn("source_column_names", F.col("cols"))
    .drop("cols"))

registry_df = (change_tables_df.join(cols_agg, "capture_instance", "left")
    .withColumnRenamed("capture_instance","cdc_table_name")
    .withColumn("source_table_name", F.concat_ws(".",F.col("source_schema"),F.col("source_table")))
    .withColumn("query_built", F.lit(False).cast("boolean"))
    .select("source_table_name","cdc_table_name","source_column_names","query_built",
            "source_schema","source_table","supports_net_changes"))

registry_df.write.mode("overwrite").saveAsTable(REGISTRY_TABLE)

# -----------------------------------------------------------
# 3. ** Add Destination Table **
# -----------------------------------------------------------
# Add column destination_table into registry
reg_with_dest = (registry_df
    .withColumn("destination_table",
        F.expr(f"concat('{CATALOG_SCHEMA}.', lower(source_schema), '_', lower(source_table), '__changes')"))
)

reg_with_dest.write.mode("overwrite").saveAsTable(REGISTRY_TABLE)

# -----------------------------------------------------------
# 4. ** Build ingest_query **
# -----------------------------------------------------------
# Build CDC function call query strings dynamically per capture instance
def get_lsn_hex(jdbc_url, user, pwd, sql_text):
    df = read_sql(jdbc_url,user,pwd,sql_text).limit(1)
    if df.rdd.isEmpty(): return None
    return df.collect()[0][0]

max_lsn_hex = get_lsn_hex(jdbc_url, user, pwd,
    "SELECT master.dbo.fn_varbintohexstr(sys.fn_cdc_get_max_lsn()) AS max_lsn_hex;")

wm_df = (spark.table(WATERMARK_TABLE) if spark.catalog.tableExists(WATERMARK_TABLE)
         else spark.createDataFrame([],T.StructType([
            T.StructField("cdc_table_name",T.StringType(),False),
            T.StructField("last_end_lsn_hex",T.StringType(),True),
            T.StructField("updated_at",T.TimestampType(),True)])))

wm_map = {r["cdc_table_name"]:r["last_end_lsn_hex"] for r in wm_df.collect()}

def build_ingest_query(cap_inst, cols, from_hex, to_hex, src_schema, src_table):
    select_cols = ", ".join([f"[{c}]" for c in cols]) if cols else "*"
    select_list = f"""{select_cols},
                      __$start_lsn AS start_lsn, __$end_lsn AS end_lsn,
                      __$seqval AS seqval, __$operation AS operation,
                      __$update_mask AS update_mask,
                      '{src_schema}.{src_table}' AS source_table_name,
                      '{cap_inst}' AS cdc_table_name"""
    fn_name = f"cdc.fn_cdc_get_all_changes_{cap_inst}"
    return f"""
      SELECT {select_list}
      FROM {fn_name}(CONVERT(binary(10), {from_hex}), CONVERT(binary(10), {to_hex}), 'all') AS C
    """

# -----------------------------------------------------------
# 5. ** Execute Queries **
# -----------------------------------------------------------
reg_rows = reg_with_dest.collect()
for r in reg_rows:
    cap_inst = r["cdc_table_name"]
    dest_tbl = r["destination_table"]
    src_schema = r["source_schema"]
    src_table  = r["source_table"]
    cols       = r["source_column_names"] or []

    from_hex = wm_map.get(cap_inst) or get_lsn_hex(jdbc_url,user,pwd,
        f"SELECT master.dbo.fn_varbintohexstr(sys.fn_cdc_get_min_lsn('{cap_inst}'))")

    if not from_hex or not max_lsn_hex:
        print(f"[SKIP] {cap_inst}: no LSN window")
        continue

    sql_q = build_ingest_query(cap_inst, cols, from_hex, max_lsn_hex, src_schema, src_table)
    changes_df = read_sql(jdbc_url, user, pwd, sql_q)

    if changes_df.rdd.isEmpty():
        print(f"[NO DATA] {cap_inst}")
    else:
        changes_df = changes_df.withColumn("ingested_at",F.current_timestamp())
        if not spark.catalog.tableExists(dest_tbl):
            changes_df.limit(0).write.mode("overwrite").saveAsTable(dest_tbl)
        changes_df.write.mode("append").saveAsTable(dest_tbl)
        print(f"[APPEND] {cap_inst} -> {dest_tbl}: {changes_df.count()} rows")

    # update watermark
    new_wm = spark.createDataFrame(
        [(cap_inst,max_lsn_hex,datetime.utcnow())],
        schema=T.StructType([
            T.StructField("cdc_table_name",T.StringType(),False),
            T.StructField("last_end_lsn_hex",T.StringType(),True),
            T.StructField("updated_at",T.TimestampType(),True)]))
    new_wm.write.mode("append").saveAsTable(WATERMARK_TABLE)

print("CDC ingestion complete.")
