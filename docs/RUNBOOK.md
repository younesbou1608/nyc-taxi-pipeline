# Runbook

## Les tables ont disparu / Looker Studio est vide

Normal dans le sandbox : les tables expirent 60 jours après leur création.
```bash
make rebuild YEAR=2024 MONTHS="1 2 3"
```
Les Parquet de `data/` sont réutilisés s'ils sont encore là ; sinon ils sont retéléchargés.

## `LoadVerificationError` au chargement

Le nombre de lignes en base diffère de la sortie Spark. Le message indique les trois compteurs
(`spark`, `charge`, `en_base`).
1. Vérifier le rapport `data/clean/_quality/<année>-<mois>.json`.
2. Relancer `make load` : le chargement remplace la partition, il est rejouable sans risque.
3. Si `en_base` vaut 0 alors que `charge` est positif : la table n'est probablement pas
   partitionnée par `year_month` (une ancienne table partitionnée par date expire les données).
   `load_bq.py` la recrée automatiquement ; sinon, supprimer `trips_clean` et relancer.

## Erreur « DML queries are not allowed in the free tier »

Une requête `DELETE`, `UPDATE`, `INSERT` ou `MERGE` a été introduite (souvent un modèle dbt
passé en `incremental`). Le sandbox n'autorise que les `SELECT`, le DDL et les load jobs :
repasser le modèle en `materialized='table'`.

## Le téléchargement échoue (404)

La TLC peut modifier l'URL de son CDN ou publier en retard.
1. Ouvrir la page officielle « TLC Trip Record Data » et copier le lien d'un fichier mensuel.
2. Surcharger la base : `export TLC_BASE_URL=<nouvelle base>`.
3. Relancer `make download`.

## Le job Spark échoue avec OutOfMemory

- Augmenter `SPARK_DRIVER_MEMORY` (par défaut 4g).
- Réduire le volume avec `--sample` pendant l'investigation.

## Taux de rejet anormalement élevé

Consulter `data/clean/_quality/<année>-<mois>.json`. Au-delà de 25 %, comparer le schéma
du mois avec les mois précédents : la TLC a déjà changé le nom et le type de certaines
colonnes entre deux années.

## Warning dbt sur l'unicité de `trip_key`

Attendu : la source TLC contient des doublons exacts. Pour les mesurer :
```sql
SELECT trip_key, COUNT(*) AS n
FROM `<projet>.nyc_taxi_analytics.fct_trips`
GROUP BY 1 HAVING n > 1
ORDER BY n DESC LIMIT 20;
```

## Quota BigQuery dépassé

Le sandbox offre 10 Go de stockage et 1 To de requêtes par mois. Réduire le nombre de mois
chargés et filtrer les requêtes sur `year_month` pour profiter du partitionnement.
