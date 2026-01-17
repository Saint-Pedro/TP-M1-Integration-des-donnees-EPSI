from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, year, month, dayofmonth, weekofyear, col, lit, current_timestamp

def run_load(input_path, db_url, db_props, limit_rows=None):
    spark = SparkSession.builder \
        .appName("OFF_Load_Gold") \
        .config("spark.jars", "/libs/mysql-connector.jar") \
        .getOrCreate()

    print("Reading Silver data...")
    df_silver = spark.read.parquet(input_path)

    # OPTIMISATION PERFORMANCE
    # Sur un cluster de prod, mettre limit_rows à None.
    # Sur environnement restreint (ex: local/colab), utiliser 50000.
    if limit_rows:
        print(f"WARNING: Processing limited to {limit_rows} rows for performance.")
        df_silver = df_silver.limit(limit_rows)

    # --- 1. DIMENSION TIME ---
    dim_time = df_silver.select(col("last_modified_t").alias("time_sk")) \
        .distinct() \
        .withColumn("full_date", from_unixtime(col("time_sk"))) \
        .select(
            col("time_sk"),
            to_date(col("full_date")).alias("date"),
            year(col("full_date")).alias("year"),
            month(col("full_date")).alias("month"),
            dayofmonth(col("full_date")).alias("day"),
            weekofyear(col("full_date")).alias("week")
        ).na.drop()

    # --- 2. DIMENSION PRODUCT (SCD Type 1 logic for init) ---
    dim_product = df_silver.select(
        col("code").alias("product_code"),
        col("product_name"),
        col("brands"),
        col("categories_tags"),
        col("countries_tags"),
        col("ecoscore_grade"),
        col("nova_group"),
        lit(1).alias("is_current"),
        current_timestamp().alias("effective_from")
    ).dropDuplicates(["product_code"])

    # --- 3. FACT NUTRITION ---
    fact_nutrition = df_silver.select(
        col("code").alias("product_code"),
        col("last_modified_t").alias("time_sk"),
        col("energy-kcal_100g"),
        col("sugars_100g"),
        col("salt_100g_final").alias("salt_100g"),
        col("fat_100g"),
        col("proteins_100g"),
        col("nutriscore_grade"),
        col("additives_n")
    )

    # --- 4. ECRITURE JDBC ---
    tables = {
        "dim_time": dim_time,
        "dim_product": dim_product,
        "fact_nutrition_snapshot": fact_nutrition
    }

    for name, df in tables.items():
        print(f"Writing table {name} to MySQL...")
        try:
            df.write.jdbc(url=db_url, table=name, mode="overwrite", properties=db_props)
            print(f"{name} written successfully.")
        except Exception as e:
            print(f"Error writing {name}: {str(e)}")

if __name__ == "__main__":
    # Configuration DB
    jdbc_url = "jdbc:mysql://localhost:3306/off_datamart?useSSL=false&allowPublicKeyRetrieval=true"
    db_properties = {
        "user": "spark_user",
        "password": "spark_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Lancement avec limite de sécurité pour dev
    run_load("/data/silver", jdbc_url, db_properties, limit_rows=50000)
