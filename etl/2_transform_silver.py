from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, row_number, desc
from pyspark.sql.window import Window

def run_transform(input_path, output_path):
    spark = SparkSession.builder.appName("OFF_Transform_Silver").getOrCreate()

    df_bronze = spark.read.parquet(input_path)

    # 1. Nettoyage standard
    df_clean = df_bronze.withColumn("product_name", trim(col("product_name"))) \
                        .withColumn("brands", trim(col("brands"))) \
                        .withColumn("code", trim(col("code")))

    # 2. Règle Métier : Harmonisation Sel (Salt vs Sodium)
    # Approximation : Sel = Sodium * 2.5
    df_clean = df_clean.withColumn(
        "salt_100g_final",
        when(col("salt_100g").isNotNull(), col("salt_100g"))
        .otherwise(col("sodium_100g") * 2.5)
    )

    # 3. Dédoublonnage (Gardons la version la plus récente)
    # Partition par Code Produit, Tri par date de modification desc
    window_spec = Window.partitionBy("code").orderBy(col("last_modified_t").desc())

    df_dedup = df_clean.withColumn("rn", row_number().over(window_spec)) \
                       .filter(col("rn") == 1) \
                       .drop("rn")

    # 4. Filtrage Qualité (Exclusion des produits sans identifiant ou nom)
    df_silver = df_dedup.filter(
        col("code").isNotNull() &
        (col("product_name").isNotNull()) &
        (col("product_name") != "")
    )

    print(f"Writing Silver data to {output_path}")
    df_silver.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    run_transform("/data/bronze", "/data/silver")
