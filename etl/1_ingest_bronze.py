from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
import sys

def run_ingest(input_csv, output_parquet):
    # Init Spark
    spark = SparkSession.builder \
        .appName("OFF_Ingest_Bronze") \
        .getOrCreate()

    # Lecture avec header et tabulations (Configuration OFF standard)
    # On utilise l'option permissive pour ne pas bloquer sur les lignes corrompues
    df_raw = spark.read.option("header", "true") \
                       .option("sep", "\t") \
                       .option("inferSchema", "false") \
                       .csv(input_csv)

    # Sélection et Cast explicite (Schéma imposé)
    # Gestion du mapping environmental_score_grade -> ecoscore_grade
    try:
        df_selected = df_raw.select(
            col("code"),
            col("url"),
            col("product_name"),
            col("brands"),
            col("categories_tags"),
            col("countries_tags"),
            col("last_modified_t").cast(LongType()),
            col("nutriscore_grade"),
            col("nova_group"),
            col("environmental_score_grade").alias("ecoscore_grade"),
            col("energy-kcal_100g").cast(FloatType()),
            col("fat_100g").cast(FloatType()),
            col("saturated-fat_100g").cast(FloatType()),
            col("sugars_100g").cast(FloatType()),
            col("salt_100g").cast(FloatType()),
            col("sodium_100g").cast(FloatType()),
            col("proteins_100g").cast(FloatType()),
            col("fiber_100g").cast(FloatType()),
            col("additives_n").cast(IntegerType())
        )

        print(f"Writing Bronze data to {output_parquet}")
        df_selected.write.mode("overwrite").parquet(output_parquet)

    except Exception as e:
        print(f"Error during ingestion: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Chemins par défaut ou via arguments
    input_path = "/data/raw/en.openfoodfacts.org.products.csv"
    output_path = "/data/bronze"
    run_ingest(input_path, output_path)
