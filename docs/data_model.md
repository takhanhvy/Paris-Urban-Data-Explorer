# Modèle de données — Urban Data Explorer

---

## 1. Schéma relationnel PostgreSQL — schéma `ude` (C1.1)

### Relations entre tables

```
ude.arrondissements  (PK: arrondissement SMALLINT 1–20)
        │
        ├──< ude.transactions_dvf      (FK: arrondissement)
        ├──< ude.logements_sociaux     (FK: arrondissement)
        ├──< ude.delinquance           (FK: arrondissement)
        ├──< ude.revenus_iris          (FK: arrondissement — via code commune IRIS)
        └──< ude.indicateurs_gold      (FK: arrondissement) ← table de faits, alimentée par Spark
```

### `ude.arrondissements` — dimension géographique

```sql
arrondissement  SMALLINT PRIMARY KEY   -- 1 à 20
code_insee      CHAR(5)                -- '75101' à '75120'
nom             VARCHAR(50)
code_postal     CHAR(5)
superficie_km2  NUMERIC(6,3)
population_2020 INTEGER
densite_population DOUBLE PRECISION   -- calculée par processor.py (population / superficie)
geom            GEOMETRY(MULTIPOLYGON, 4326)  -- PostGIS WGS84
```

### `ude.indicateurs_gold` — table de faits (alimentée par datamart.py)

Contrainte unique sur `(arrondissement, annee)`. Toutes les valeurs numériques sont en `DOUBLE PRECISION` sauf indication.

```sql
arrondissement      SMALLINT REFERENCES ude.arrondissements
annee               SMALLINT                    -- 2020 à 2025

-- DVF
nb_transactions     INTEGER
prix_m2_median      DOUBLE PRECISION
prix_m2_moyen       DOUBLE PRECISION
prix_m2_q1          DOUBLE PRECISION
prix_m2_q3          DOUBLE PRECISION
surface_mediane     DOUBLE PRECISION
part_appartements   DOUBLE PRECISION            -- % des transactions

-- Logements sociaux
nb_logements_sociaux   INTEGER
nb_plai                INTEGER
nb_plus_pluscd         INTEGER
nb_pls                 INTEGER

-- Délinquance
taux_cambriolages_pmille  DOUBLE PRECISION
taux_violences_pmille     DOUBLE PRECISION
taux_delinquance_global   DOUBLE PRECISION

-- Revenus (agrégé depuis IRIS vers arrondissement)
revenu_median_arr         DOUBLE PRECISION
indice_gini_arr           DOUBLE PRECISION
rapport_interdecile_arr   DOUBLE PRECISION

-- Score composite
score_attractivite        DOUBLE PRECISION       -- 0–100, Min-Max normalisé

computed_at  TIMESTAMP DEFAULT NOW()
```

### `ude.transactions_dvf` — silver relationnel

```sql
id              BIGSERIAL PRIMARY KEY
date_mutation   DATE
annee           SMALLINT
arrondissement  SMALLINT REFERENCES ude.arrondissements
code_commune    CHAR(5)
valeur_fonciere DOUBLE PRECISION
surface_reelle_bati DOUBLE PRECISION
nombre_pieces_principales SMALLINT
type_local      VARCHAR(50)
prix_m2         DOUBLE PRECISION
nature_mutation VARCHAR(100)
```

### `ude.logements_sociaux`

```sql
id              BIGSERIAL PRIMARY KEY
annee           SMALLINT
arrondissement  SMALLINT REFERENCES ude.arrondissements
adresse_programme VARCHAR(200)
code_postal     CHAR(5)
nb_logmt_total  INTEGER
nb_plai         INTEGER
nb_plus         INTEGER
nb_pluscd       INTEGER
nb_pls          INTEGER
mode_real       VARCHAR(100)
```

### `ude.delinquance`

```sql
id              BIGSERIAL PRIMARY KEY
annee           SMALLINT
arrondissement  SMALLINT REFERENCES ude.arrondissements
CODGEO_2025     CHAR(5)
indicateur      VARCHAR(200)
nombre          INTEGER
taux_pour_mille DOUBLE PRECISION
```

### `ude.revenus_iris`

```sql
id              BIGSERIAL PRIMARY KEY
IRIS            VARCHAR(9)
LIBIRIS         VARCHAR(200)
COM             CHAR(5)
arrondissement  SMALLINT REFERENCES ude.arrondissements
DEC_MED18       DOUBLE PRECISION    -- revenu médian par UC
DEC_GI18        DOUBLE PRECISION    -- indice de Gini
DEC_RD18        DOUBLE PRECISION    -- rapport interdécile
DEC_Q118        DOUBLE PRECISION    -- 1er quartile
DEC_Q318        DOUBLE PRECISION    -- 3e quartile
DEC_D118        DOUBLE PRECISION    -- décile 1 (les 9 déciles sont stockés)
-- ... DEC_D218 à DEC_D918
```

---

## 2. Data Lake MinIO — Parquet partitionné (C1.3)

Connexion Spark : `s3a://urban-data/` via `spark.hadoop.fs.s3a.endpoint=http://minio:9000`.

Partition : `ingestion_year / ingestion_month / ingestion_day` (predicate pushdown activé).

### Couche bronze — données brutes

Format Parquet, colonnes en string (DVF), structure JSON aplatie (logements sociaux).

```
s3a://urban-data/bronze/
├── dvf/                        ← CSV DVF toutes colonnes, types string
├── delinquance/                ← copie fidèle du Parquet INSEE national
├── logements_sociaux/          ← JSON opendata.paris.fr aplati
└── revenus/                    ← XLSX converti via pandas bridge
```

### Couche silver — nettoyée et typée

Types castés, filtrage Paris, colonnes métier calculées (`prix_m2`, `arrondissement`).

```
s3a://urban-data/silver/
├── transactions/               ← DVF filtre Paris (code_commune LIKE '751%'), prix_m2, types castés
├── delinquance/                ← filtré 75101–75120, arrondissement 1–20 extrait
├── logements_sociaux/          ← arrdt normalisé, volumes INTEGER
└── revenus/                    ← filtré COM LIKE '75%', déciles DOUBLE PRECISION
```

---

## 3. MongoDB — collection `air_quality` (C1.2)

Base : `urban_data_nosql`. Index TTL de 86400s sur `inserted_at` (expiration automatique 24h). Index simple sur `insee`.

```json
{
  "_id": "ObjectId(...)",
  "insee": "75107",
  "inserted_at": "2026-06-24T08:00:00Z",
  "timestamp": "2026-06-24T08:00:00+00:00",
  "data": [
    { "date": "2026-06-24", "label": "Bon",   "code_qual": 2 },
    { "date": "2026-06-25", "label": "Moyen", "code_qual": 3 }
  ]
}
```

Un document par arrondissement. L'API `/api/metrics` y lit le label du jour pour l'arrondissement demandé.

---

## 4. Score attractivité — calcul Spark

Calculé dans `datamart.py` par normalisation Min-Max sur fenêtre `Window.partitionBy("annee")`.

```
score = revenu_norm × 35
      + (1 - delinquance_norm) × 30
      + (1 - prix_norm) × 25
      + logements_norm × 10
```

Score dans [0, 100]. Un score élevé (>70) signifie revenus hauts, faible délinquance, prix accessibles et bonne mixité sociale.
