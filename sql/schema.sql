
CREATE DATABASE IF NOT EXISTS off_datamart;
USE off_datamart;

-- Dimension Produit
-- Ref: 
CREATE TABLE IF NOT EXISTS dim_product (
    product_code VARCHAR(255) PRIMARY KEY,
    product_name TEXT,
    brands TEXT,
    categories_tags TEXT,
    countries_tags TEXT,
    ecoscore_grade VARCHAR(10),
    nova_group VARCHAR(10),
    is_current BOOLEAN DEFAULT 1,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Temps
-- Ref: 
CREATE TABLE IF NOT EXISTS dim_time (
    time_sk BIGINT PRIMARY KEY,
    date DATE,
    year INT,
    month INT,
    day INT,
    week INT
);

-- Table de Faits Nutrition
-- Ref: 
CREATE TABLE IF NOT EXISTS fact_nutrition_snapshot (
    product_code VARCHAR(255),
    time_sk BIGINT,
    energy_kcal_100g FLOAT,
    sugars_100g FLOAT,
    salt_100g FLOAT,
    fat_100g FLOAT,
    proteins_100g FLOAT,
    nutriscore_grade VARCHAR(5),
    additives_n INT,
    FOREIGN KEY (product_code) REFERENCES dim_product(product_code),
    FOREIGN KEY (time_sk) REFERENCES dim_time(time_sk)
);
