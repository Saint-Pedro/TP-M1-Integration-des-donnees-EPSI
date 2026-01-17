
-- Requêtes Analytiques / KPIs
-- Ref: 

-- 1. Top Marques avec Nutriscore A/B
SELECT p.brands, COUNT(*) as count 
FROM dim_product p 
JOIN fact_nutrition_snapshot f ON p.product_code = f.product_code 
WHERE f.nutriscore_grade IN ('a', 'b') 
GROUP BY p.brands ORDER BY count DESC LIMIT 10;

-- 2. Moyenne de sucre par catégorie (Approx via tags)
SELECT SUBSTRING_INDEX(p.categories_tags, ',', 1) as cat, AVG(f.sugars_100g) as avg_sugar
FROM dim_product p
JOIN fact_nutrition_snapshot f ON p.product_code = f.product_code
GROUP BY cat ORDER BY avg_sugar DESC LIMIT 20;

-- 3. Détection d'anomalies (Sucre > 100g)
SELECT p.product_name, f.sugars_100g 
FROM dim_product p 
JOIN fact_nutrition_snapshot f ON p.product_code = f.product_code
WHERE f.sugars_100g > 100;
