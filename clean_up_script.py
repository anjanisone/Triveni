from pyspark.sql import functions as F

# ---- source/target ----
TABLE = "dbo.Stratus"   # Fabric / Lakehouse table

# words to strip entirely (case-insensitive, whole-word)
STRIP_WORDS = ["export","dhv","dsi","duct","stratus","UT","PSYCH","DMP","DPL","DWV"]
strip_regex = r"(?i)\b(?:{})\b".format("|".join(STRIP_WORDS))

df = spark.table(TABLE)

clean = (
    df
    # Start with original col into cleaned col
    .withColumn("part_model_name_cleaned", F.col("part_model_name"))

    # 2) Replace '-' and '_' with a space
    .withColumn("part_model_name_cleaned", F.regexp_replace("part_model_name_cleaned", r"[-_]+", " "))

    # 1) Remove listed words
    .withColumn("part_model_name_cleaned", F.regexp_replace("part_model_name_cleaned", strip_regex, ""))

    # 3) Remove any leading non-letters (e.g., job numbers like 4946-…)
    .withColumn("part_model_name_cleaned", F.regexp_replace("part_model_name_cleaned", r"^[^A-Za-z]+", ""))

    # 4) Remove if starts with 2+ consecutive X's (XX, XXX, XXXX, etc.)
    .withColumn("part_model_name_cleaned", F.regexp_replace("part_model_name_cleaned", r"(?i)^X{2,}\s*", ""))

    # 5) Collapse adjacent duplicate words: "alpha alpha beta" -> "alpha beta"
    .withColumn("part_model_name_cleaned", F.regexp_replace("part_model_name_cleaned", r"(?i)\b(\w+)(?:\s+\1)+\b", r"\1"))

    # Final tidy whitespace
    .withColumn("part_model_name_cleaned", F.regexp_replace("part_model_name_cleaned", r"\s+", " "))
    .withColumn("part_model_name_cleaned", F.trim("part_model_name_cleaned"))
)

# Overwrite the same table with cleaned data
clean.write.mode("overwrite").saveAsTable(TABLE)

# (Optional) Preview cleaned values
display(spark.table(TABLE).select("part_model_name").limit(25))
