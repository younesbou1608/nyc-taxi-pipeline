# Runbook

## Incident : le téléchargement échoue (404)

La TLC peut modifier l'URL de son CDN ou publier en retard.
1. Ouvrir la page officielle "TLC Trip Record Data" et copier le lien d'un fichier mensuel.
2. Surcharger la base : `export TLC_BASE_URL=<nouvelle base>`.
3. Relancer `make download`.

## Incident : le job Spark échoue avec OutOfMemory

- Augmenter `SPARK_DRIVER_MEMORY` (par défaut 4g).
- Réduire le volume avec `--sample` pendant l'investigation.
- Vérifier que le broadcast join ne porte que sur la table des zones (265 lignes).

## Incident : taux de rejet anormalement élevé

Consulter `data/clean/_quality/<année>-<mois>.json`.
Si le taux dépasse 25 %, comparer le schéma du mois avec les mois précédents :
la TLC a déjà changé le nom et le type de certaines colonnes entre deux années.

## Incident : doublons dans fct_trips

Le pipeline est idempotent à deux niveaux (DELETE du mois côté chargement,
`merge` sur `trip_key` côté dbt). En cas de doublon :
```sql
SELECT trip_key, COUNT(*) FROM analytics.fct_trips GROUP BY 1 HAVING COUNT(*) > 1;
```
Puis relancer `dbt build --full-refresh --select fct_trips+`.

## Quota BigQuery dépassé

Le sandbox a des quotas mensuels. Réduire le nombre de mois chargés,
et vérifier que les requêtes filtrent bien sur `pickup_date` pour profiter du partitionnement.
