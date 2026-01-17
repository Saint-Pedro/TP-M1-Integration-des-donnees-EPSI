
# Projet Data Engineering - OpenFoodFacts

## Description
Pipeline ETL complet Spark (PySpark) vers MySQL réalisé sur Google Colab.

## Auteur
Généré automatiquement par le Pipeline.

## Structure du Livrable
- /etl : Scripts de traitement (Code Spark)
- /sql : Scripts de structure BDD (reconstitués)
- /docs : Documentation
- /data : (Echantillons de données Bronze/Silver si présents)

## Architecture
1. Ingestion : CSV -> Parquet (Bronze)
2. Transformation : Cleaning & Dédoublonnage (Silver)
3. Chargement : Modélisation en étoile -> MySQL (Gold)
