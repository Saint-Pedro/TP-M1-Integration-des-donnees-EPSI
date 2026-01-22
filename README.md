# Projet Data Engineering : OpenFoodFacts Datamart

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-orange)
![MySQL](https://img.shields.io/badge/MySQL-8.0-lightgrey)

Ce projet implémente un pipeline ETL (Extract, Transform, Load) complet pour traiter les données massives d'OpenFoodFacts. L'objectif est de construire un Datamart décisionnel (Schéma en étoile) pour analyser la qualité nutritionnelle des produits alimentaires.

## Contexte et Objectifs
Dans le cadre du module **TRDE703 - Atelier Intégration des Données**, ce projet répond à une problématique Big Data :
1.  **Ingérer** des données brutes volumineuses (CSV/JSON).
2.  **Nettoyer et Harmoniser** les données (Dédoublonnage, conversions d'unités).
3.  **Modéliser** un entrepôt de données pour l'analyse métier.
4.  **Restituer** des indicateurs clés (KPIs) via SQL.

## Architecture Technique

Le pipeline suit une architecture **Bronze / Silver / Gold** :

1.  **Bronze (Ingestion)** : Lecture du CSV source OpenFoodFacts, typage strict des données, sauvegarde au format **Parquet** pour la performance.
2.  **Silver (Transformation)** :
    * Nettoyage (Trim, gestion des NULL).
    * Règle métier : Calcul du Sel (`Salt = Sodium * 2.5`) si manquant.
    * Dédoublonnage : Conservation de la version la plus récente (`last_modified_t`) par code-barres.
3.  **Gold (Chargement)** :
    * Modélisation en étoile (**Star Schema**).
    * Injection dans **MySQL** via JDBC.

**Technologies utilisées :**
* **Langage :** Python (PySpark)
* **ETL :** Apache Spark (Traitement distribué)
* **Base de données :** MySQL 8.0 (Datamart cible)
* **Environnement :** Google Colab (Simulation Cluster)

## Structure du Projet

```text
/
├── conf/            # Configuration Spark (mémoire, drivers)
├── docs/            # Documentation et Dictionnaire de données
├── etl/             # Scripts PySpark
│   ├── 1_ingest_bronze.py    # Ingestion Raw -> Bronze
│   ├── 2_transform_silver.py # Cleaning Bronze -> Silver
│   └── 3_load_gold.py        # Chargement Silver -> MySQL
├── sql/             # Scripts SQL
│   ├── schema.sql            # DDL : Création des tables (Dim/Fact)
│   └── analysis.sql          # DML : Requêtes analytiques (KPIs)
└── tests/           # Tests unitaires et qualité (placeholders)
```
## Modèle de Données (Datamart)
Le modèle cible est un schéma en étoile **(Star Schema)** conçu pour optimiser les requêtes analytiques.


**Table de Faits : fact_nutrition_snapshot**

* Contient les mesures quantitatives : sucres, sel, énergie, score nutritionnel.

* Granularité : Une ligne par produit à un instant T (historisation possible via time_sk).

**Dimensions :**


* dim_product : Attributs descriptifs (Code EAN, Nom, Marque, Catégories, EcoScore).


* dim_time : Axe temporel (Année, Mois, Semaine) permettant l'analyse de l'évolution de la qualité des données.

## Installation et Exécution
Ce projet a été conçu pour s'exécuter dans un environnement **Google Colab** (ou tout cluster Spark avec support JDBC).


**Pré-requis**
* Python 3.x & Java 8+ (OpenJDK)

* Apache Spark & PySpark (ETL) 


* Serveur MySQL 8.0 (Datamart cible) 


* Driver JDBC MySQL (mysql-connector-j)

**Étapes de reproduction**
1. Cloner ce dépôt.

2. Placer le fichier source en.openfoodfacts.org.products.csv (Export complet) dans le dossier /data/raw.

3. Exécuter les scripts ETL dans l'ordre séquentiel :

```text
python etl/1_ingest_bronze.py    # Extraction et typage
python etl/2_transform_silver.py # Nettoyage et règles métier
python etl/3_load_gold.py        # Chargement dimensionnel
```
4.  Consulter la base de données MySQL off_datamart pour voir les résultats.

***Note technique** : Pour des raisons de performance sur l'environnement de développement (Colab), le chargement MySQL est par défaut limité à un échantillon de 50 000 produits. Cette limite est configurable via le paramètre limit_rows dans le script de chargement pour passer en mode production (Full Load).*

## Exemples d'Analyses (KPIs)
Le dossier /sql/analysis.sql contient les requêtes pour répondre aux questions métiers:


* Qualité Nutritionnelle : Top 10 marques avec la plus forte proportion de produits Nutri-Score A/B.


* Santé Publique : Teneur moyenne en sucre (g/100g) par grande catégorie de produits.


* Qualité des Données : Détection d'anomalies (Exemple : Produits déclarant > 100g de sucre pour 100g).

## Auteurs
* Astin LAURENS
* Célian SUBIRANA
* Melina HADDAD

Projet réalisé dans le cadre du Master 1 EPSI- Module TRDE703 Atelier Intégration des Données.
