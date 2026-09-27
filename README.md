# NYC Taxi Analytics — Pipeline Big Data & Data Warehouse

Pipeline de données de bout en bout sur les trajets de taxis jaunes de New York
(NYC TLC, ~3 M de lignes par mois) : ingestion, nettoyage distribué avec **PySpark**,
chargement dans **BigQuery**, modélisation dimensionnelle et tests de qualité avec **dbt**,
le tout orchestré par **Airflow** et validé en CI par **GitHub Actions**.

---

## Architecture

```
                 ┌──────────────────────────┐
                 │  NYC TLC (Parquet CDN)   │
                 └────────────┬─────────────┘
                              │  src/download.py
                              ▼
                 ┌──────────────────────────┐
                 │  data/raw/*.parquet      │   zone lookup CSV (265 zones)
                 └────────────┬─────────────┘
                              │  src/transform.py   (PySpark)
                              │  · typage & renommage
                              │  · règles qualité (durée, distance, montant…)
                              │  · colonnes dérivées (durée, vitesse, tip_rate)
                              │  · broadcast join sur les zones
                              ▼
                 ┌──────────────────────────┐
                 │  data/clean/trips/       │  partitionné year=/month=
                 │  data/clean/_quality/    │  métriques de rejet (JSON)
                 └────────────┬─────────────┘
                              │  src/load_bq.py  (idempotent : DELETE mois + APPEND)
                              ▼
        ┌────────────────────────────────────────────┐
        │  BigQuery  ·  dataset raw                  │
        │  trips_clean (partition pickup_date,       │
        │  cluster pickup_zone_id, payment_type)     │
        └────────────────────┬───────────────────────┘
                             │  dbt build
                             ▼
        ┌────────────────────────────────────────────┐
        │  BigQuery  ·  dataset analytics            │
        │  staging : stg_trips, stg_zones            │
        │  marts   : fct_trips (incrémental, merge)  │
        │            dim_zone, dim_date              │
        │            agg_daily_zone, agg_hourly_…    │
        └────────────────────┬───────────────────────┘
                             ▼
                  Looker Studio / SQL ad hoc

Orchestration : Airflow (DAG mensuel)   ·   CI : ruff + pytest + dbt parse
```

### Modèle en étoile

| Table | Grain | Rôle |
|---|---|---|
| `fct_trips` | 1 trajet | Faits : distance, durée, montants, pourboire |
| `dim_zone` | 1 zone TLC (265) | Quartier, nom de zone, service zone |
| `dim_date` | 1 jour | Année, mois, jour de semaine, week-end |
| `agg_daily_zone` | jour × zone | Volume et revenus par zone |
| `agg_hourly_demand` | heure × week-end | Profil de demande horaire |

---

## Prérequis

- Python 3.11
- **Java 17** (requis par PySpark) — vérifier avec `java -version`
- Un projet Google Cloud avec le **sandbox BigQuery** activé (sans carte bancaire)
- `gcloud` CLI pour l'authentification locale

---

## Installation

```bash
git clone <votre-repo> && cd nyc-taxi-pipeline
make setup && source .venv/bin/activate
cp .env.example .env    # renseigner GCP_PROJECT_ID
set -a && source .env && set +a

gcloud auth application-default login
```

---

## Exécution manuelle, étape par étape

```bash
# 1. Télécharger un mois de données + le référentiel des zones
make download YEAR=2024 MONTH=1

# 2. Nettoyer et enrichir avec Spark (écrit data/clean/, partitionné)
make transform YEAR=2024 MONTH=1

# 3. Charger dans BigQuery (rejouable sans doublon)
make load YEAR=2024 MONTH=1

# 4. Construire les modèles dbt + lancer tous les tests
make dbt-build

# Ou tout d'un coup
make pipeline YEAR=2024 MONTH=1
```

Pendant le développement, limiter la volumétrie :

```bash
python -m src.transform --year 2024 --month 1 --sample 100000
```

---

## Orchestration Airflow

```bash
docker compose up -d      # http://localhost:8080
```

Le DAG `nyc_taxi_pipeline` s'exécute le 5 de chaque mois et traite le mois M-2
(délai de publication de la TLC) :

```
resolve_period → download → spark_transform → load_bigquery → dbt_build
```

Backfill sur une période passée :

```bash
docker compose exec airflow airflow dags backfill \
    -s 2024-03-01 -e 2024-06-01 nyc_taxi_pipeline
```

---

## Qualité des données

Deux niveaux de contrôle :

**1. En amont, dans Spark** (`src/config.py` → `QualityRules`) : les lignes invalides sont
écartées, et un rapport JSON est écrit dans `data/clean/_quality/` avec le nombre de lignes
entrantes, sortantes et le taux de rejet. Un taux supérieur à 25 % déclenche un avertissement.

Règles appliquées : durée entre 1 et 360 minutes, distance entre 0,1 et 200 miles,
montant total entre 0 et 5 000 $, 1 à 8 passagers, date dans le mois traité, zones non nulles.

**2. En aval, dans dbt** : environ 20 tests (`not_null`, `unique`, `relationships`,
`accepted_values`, `accepted_range`, unicité de combinaisons) plus deux tests singuliers
métier (pas de revenu négatif, pourboire inférieur au total).

```bash
cd dbt && dbt test --profiles-dir .
```

---

## Tests unitaires

```bash
make test     # pytest : job Spark sur micro-datasets + module de téléchargement
make lint     # ruff
```

---

## Choix techniques

**Pourquoi Spark et pas Pandas ?** Un mois tient en mémoire, mais pas plusieurs années.
Le job est écrit pour passer à l'échelle sans réécriture : les mêmes transformations
tournent en local ou sur un cluster.

**Pourquoi un schéma en étoile ?** Les requêtes analytiques filtrent par date et par zone.
Séparer faits et dimensions évite de dupliquer les libellés de zone sur des millions de
lignes et rend les jointures BI naturelles.

**Pourquoi partitionner par `pickup_date` et clusteriser ?** Dans BigQuery, la facturation
dépend des octets lus. Partitionner par date réduit fortement le volume scanné sur les
requêtes filtrées par période, et le clustering accélère les filtres par zone et mode de paiement.

**Pourquoi un modèle incrémental avec `merge` ?** Le rechargement d'un mois ne doit pas créer
de doublons. La clé de substitution `trip_key` assure l'idempotence de bout en bout,
en complément du `DELETE` par mois côté chargement.

**Pourquoi Airflow en dernier dans la conception ?** Chaque étape est un module Python
testable et exécutable seul. Le DAG ne fait qu'enchaîner ces modules, ce qui simplifie
le débogage et rend le pipeline rejouable à la main.

---

## Limites connues et pistes d'amélioration

- Le chargement passe par des fichiers locaux. En production, on écrirait dans Cloud Storage
  et on chargerait depuis un external stage.
- Le sandbox BigQuery impose des quotas ; se limiter à 3 à 6 mois de données.
- Pistes : exposition d'un dashboard Looker Studio, alerting sur le taux de rejet,
  déploiement de Spark sur Dataproc Serverless.

---

## Structure du dépôt

```
nyc-taxi-pipeline/
├── src/
│   ├── config.py          # configuration centralisée, règles qualité
│   ├── download.py        # téléchargement atomique des sources
│   ├── transform.py       # job PySpark (nettoyage, enrichissement)
│   └── load_bq.py         # chargement idempotent dans BigQuery
├── dbt/
│   ├── models/staging/    # stg_trips, stg_zones (+ sources, tests)
│   ├── models/marts/      # fct_trips, dim_zone, dim_date, agrégats
│   └── tests/             # tests singuliers métier
├── dags/
│   └── nyc_taxi_pipeline.py
├── tests/                 # pytest (Spark local + mocks HTTP)
├── .github/workflows/ci.yml
├── docker-compose.yml     # Airflow 2.9 + Postgres
└── Makefile
```
