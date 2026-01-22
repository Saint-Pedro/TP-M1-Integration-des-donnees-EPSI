# Cahier de Qualité des Données (Data Quality Report)

Ce document synthétise les règles de gestion appliquées et les anomalies détectées lors de l'exécution du pipeline ETL.

## 1. Règles de Nettoyage et Standardisation (Silver)
* **Nettoyage syntaxique :** Suppression des espaces superflus (`trim`) sur les codes-barres, noms de produits et marques.
* **Harmonisation des unités :**
    * Règle : `Salt` est souvent manquant alors que `Sodium` est présent.
    * Action : Si `salt_100g` est NULL, calcul via `salt = sodium * 2.5`.
* **Dédoublonnage (Golden Record) :**
    * Problème : Plusieurs lignes pour le même code-barres (mises à jour successives).
    * Résolution : Utilisation d'une fenêtre temporelle (`Window.partitionBy("code").orderBy("last_modified_t" DESC)`) pour ne conserver que l'enregistrement le plus récent.

## 2. Métriques de Complétude (Couche Gold)
* **Volume traité (Échantillon Dev) :** 50 000 produits chargés.
* **Volume traité (Production théorique) :** ~3.9 Millions de produits (disponible via paramétrage `limit_rows=None`).

## 3. Anomalies Détectées (Exemples)
Les requêtes SQL d'analyse ont permis d'identifier des incohérences fonctionnelles dans la base OpenFoodFacts :

| Type d'anomalie | Description | Exemple trouvé | Action recommandée |
| :--- | :--- | :--- | :--- |
| **Valeurs Hors Bornes** | Teneur > 100g pour 100g de produit | *Sucres = 105g* | Rejet ou plafonnement à 100g |
| **Données Manquantes** | Nutriscore absent sur produits majeurs | *Marques populaires sans score* | Enrichissement via API externe |
| **Incohérence Marque** | Noms de marques non normalisés | *"Nestle" vs "Nestlé"* | Création d'un référentiel de marques (Master Data) |

## 4. Stratégie d'amélioration
Pour passer en production, nous recommandons :
1.  L'implémentation de **Great Expectations** pour automatiser les tests unitaires de données.
2.  Le rejet des lignes critiques (ex: Code barre NULL) dans une table de rejet (`quarantine_table`).

## 5. Observabilité et Monitoring

Dans cette version prototype, les métriques d'exécution (nombre de lignes lues, rejetées, insérées) sont exposées via les **logs standards (stdout)** du driver Spark pour un debugging immédiat.

**Format des métriques (Exemple de structure JSON cible pour la prod) :**
```json
{
  "run_id": "2023-10-27_10-00-00",
  "status": "SUCCESS",
  "metrics": {
    "rows_read": 50000,
    "rows_written": 48950,
    "quality_pass_rate": 97.9
  },
  "anomalies": {
    "missing_code": 120,
    "invalid_values": 930
  }
}
