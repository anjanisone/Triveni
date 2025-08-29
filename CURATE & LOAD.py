# --- Notebook B: CURATE & LOAD (Raw Delta -> Curated Delta) ---

from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
import re, time
from notebookutils import mssparkutils

# ====== CONFIG ======
REGISTRY_TABLE_PATH   = "Tables/dbo/stratus_table_registry"
LANDING_BASE_PATH     = "Tables/dbo"     # where raw__* are sitting
RAW_PREFIX            = "raw__"
CURATED_BASE_PATH     = "Tables/dbo"     # curated home
DESTINATION_SCHEMA    = "dbo"

PFI_WALL_THICKNESS_FACTORS_PATH = f"{CURATED_BASE_PATH}/{DESTINATION_SCHEMA}/dsi_pfi_wall_thickness_factors"
PFI_DIM_NPS_PATH                = f"{CURATED_BASE_PATH}/{DESTINATION_SCHEMA}/dsi_pfi_dim_nps"

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# ===== helpers =====
def camel_to_snake(name):
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def trim_all_string_columns(df):
    out = df
    for c, t in df.dtypes:
        if t == "string":
            out = out.withColumn(c, F.trim(F.col(c)))
    return out

def table_exists(path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1)
        return True
    except:
        return False

# -- PFI helpers (same logic as your original) --
from pyspark.sql.types import FloatType

@F.udf(FloatType())
def nps_str_to_float_udf(nps_str):
    if nps_str is None: return None
    try:
        return float(str(nps_str).replace('"','').strip())
    except:
        return None

def preload_pfi():
    wall = spark.read.format("delta").load(PFI_WALL_THICKNESS_FACTORS_PATH)
    dim  = spark.read.format("delta").load(PFI_DIM_NPS_PATH)
    dim  = dim.withColumn("nps_numeric", nps_str_to_float_udf(F.col("nps"))) \
              .select("od_in","nps_numeric").distinct()
    return wall.cache(), dim.cache()

def get_wall_factor_for_thickness(df, wall_ref_df, thickness_col="part_material_thickness", out_col="part_wall_factor"):
    wf = wall_ref_df.alias("wf")
    joined = df.join(
        wf,
        (F.col(thickness_col) >= wf.wall_min_in) & (F.col(thickness_col) <= wf.wall_max_in),
        "left"
    )
    return joined.drop("wall_min_in","wall_max_in").withColumnRenamed("wall_factor", out_col)

def calculate_factored_diameter_inch(df_in, wall_ref_df, dim_ref_df):
    df = df_in
    # Join to get NPS from OD
    df = df.join(dim_ref_df.alias("dim"), F.col("outside_diameter") == F.col("dim.od_in"), "left") \
           .drop("dim.od_in").withColumnRenamed("dim.nps_numeric", "nps_numeric")
    # Wall factor
    df = get_wall_factor_for_thickness(df, wall_ref_df, "part_material_thickness", "part_wall_factor")
    # Material factor
    df = df.withColumn(
        "material_factor",
        F.when(F.upper(F.col("material_group")).contains("CARBON STEEL"), F.lit(1.0))
         .when(F.upper(F.col("material_group")).contains("STAINLESS STEEL"), F.lit(2.0))
         .otherwise(F.lit(None).cast("double"))
    )
    # FDI
    df = df.withColumn(
        "factored_diameter_inch",
        (F.col("nps_numeric") * F.col("part_wall_factor") * F.col("material_factor")).cast("float")
    )
    return df

# ===== run =====
print("CURATE: start")

pfi_wall, pfi_dim = preload_pfi()
reg = spark.read.format("delta").load(REGISTRY_TABLE_PATH).collect()

for e in reg:
    src_tbl  = e["source_table"]
    dst_tbl  = e["destination_table"]
    load_type = e["load_type"]                       # "overwrite" | "append" | "merge"
    inc_col   = getattr(e, "incremental_column", None)  # PK for merge (if provided)

    raw_path      = f"{LANDING_BASE_PATH}/{RAW_PREFIX}{dst_tbl}"
    curated_path  = f"{CURATED_BASE_PATH}/{DESTINATION_SCHEMA}/{dst_tbl}"

    print(f"\n-- CURATE: {src_tbl} ({load_type}) -> {curated_path} --")

    if not table_exists(raw_path):
        print(f"[SKIP] Raw table missing: {raw_path}")
        continue

    t0 = time.time()
    df = spark.read.format("delta").load(raw_path)

    # Clean/normalize
    df = trim_all_string_columns(df)
    for c in list(df.columns):
        df = df.withColumnRenamed(c, camel_to_snake(c))

    # Type touches (example: timestamps)
    for c, t in df.dtypes:
        if t in ("timestamp", "string") and c.endswith("_dt"):
            try:
                df = df.withColumn(c, F.to_timestamp(F.col(c)))
            except:
                pass

    # PFI only for Parts
    if src_tbl == "Parts":
        needed = set(["outside_diameter","part_material_thickness","material_group"])
        if needed.issubset(set(map(str.lower, df.columns))):
            df = calculate_factored_diameter_inch(df, pfi_wall, pfi_dim)
        else:
            print("Parts table lacks columns for PFI; skipping FDI calc.")

    rows = df.count()
    print(f"Rows prepared: {rows:,}")

    # Write curated
    if load_type == "overwrite" or not table_exists(curated_path):
        df.write.mode("overwrite").format("delta").save(curated_path)
        print("Curated overwrite complete.")
    elif load_type == "append":
        df.write.mode("append").format("delta").save(curated_path)
        print("Curated append complete.")
    elif load_type == "merge":
        if not inc_col:
            raise ValueError(f"Merge load_type requires incremental_column (primary key) for {dst_tbl}")
        # Try natural snake-case PK
        pk_snake = camel_to_snake(inc_col) if inc_col not in df.columns else inc_col
        if pk_snake not in df.columns:
            # common default
            if "id" in df.columns:
                pk_snake = "id"
            else:
                raise ValueError(f"Primary key '{inc_col}' (or 'id') not present in curated DataFrame for {dst_tbl}")

        tgt = DeltaTable.forPath(spark, curated_path)
        cond = f"d.{pk_snake} = s.{pk_snake}"
        action = tgt.alias("d").merge(df.alias("s"), cond).whenMatchedUpdateAll()
        # your original notebook deletes orphans for Parts only
        if src_tbl == "Parts":
            action = action.whenNotMatchedInsertAll().whenNotMatchedBySourceDelete()
        else:
            action = action.whenNotMatchedInsertAll()
        res = action.execute()
        if res:
            print("Merge metrics:", res.operationMetrics)
    else:
        raise ValueError(f"Unknown load_type: {load_type}")

    print(f"Elapsed: {time.time()-t0:.1f}s")

# optional: mark refresh (mirror of your %sql cell)
spark.sql("""
INSERT INTO dsi_analytics.dbo.dsi_data_sources (source_name, refresh_date_time)
VALUES ('Stratus', current_timestamp())
""")

# cleanup caches
pfi_wall.unpersist()
pfi_dim.unpersist()

print("\nCURATE: complete.")
