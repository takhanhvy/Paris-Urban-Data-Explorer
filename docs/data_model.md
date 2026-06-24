# Modèles de données — Urban Data Explorer

---

## 1. Schéma relationnel PostgreSQL (C1.1)

### Diagramme entité-relation

```
ude.arrondissements  (PK: arrondissement 1–20)
        │
        ├──< ude.transactions_dvf      (FK: arrondissement)
        ├──< ude.logements_sociaux     (FK: arrondissement)
        ├──< ude.delinquance           (FK: arrondissement)
        ├──< ude.revenus_iris          (FK: arrondissement — via code IRIS)
        └──< ude.indicateurs_gold      (FK: arrondissement) ← table de faits
```

### `ude.arrondissements` — dimension géographique

```sql
arrondissement  SMALLINT PK        -- 1 à 20
code_insee      CHAR(5)            -- '75101' à '75120'
nom             VARCHAR(50)
code_postal     CHAR(5)
superficie_km2  NUMERIC(6,3)       -- source IGN
population_2020 INTEGER            -- source INSEE
geom            GEOMETRY(MULTIPOLYGON, 4326)  -- PostGIS WGS84
```

### `ude.indicateurs_gold` — table de faits principale (alimentée par datamart.py)

Clé composite unique : `(arrondissement, annee)`

```sql
-- DVF transactions
nb_transactions         INTEGER
prix_m2_median          NUMERIC(10,2)
prix_m2_moyen           NUMERIC(10,2)
prix_m2_q1              NUMERIC(10,2)
prix_m2_q3              NUMERIC(10,2)
surface_mediane         NUMERIC(8,2)
part_appartements       NUMERIC(5,2)    -- % des transactions

-- Logements sociaux
nb_logements_sociaux    INTEGER
nb_plai                 INTEGER
nb_plus_pluscd          INTEGER
nb_pls                  INTEGER
taux_logements_sociaux  NUMERIC(6,2)

-- Délinquance
taux_cambriolages_pmille  NUMERIC(8,3)
taux_violences_pmille     NUMERIC(8,3)
taux_delinquance_global   NUMERIC(8,3)

-- Revenus (agrégé depuis IRIS)
revenu_median_arr         NUMERIC(10,2)
indice_gini_arr           NUMERIC(6,4)
rapport_interdecile_arr   NUMERIC(6,2)

-- Score composite
score_attractivite        NUMERIC(5,2)   -- 0–100, calculé par datamart.py Spark

computed_at  TIMESTAMP DEFAULT NOW()
```

### Requêtes dashboard typiques

```sql
-- Carte choroplèthe 2024
SELECT a.arrondissement, ig.prix_m2_median
FROM ude.indicateurs_gold ig
JOIN ude.arrondissements a USING (arrondissement)
WHERE ig.annee = 2024;

-- Timeline prix 7ème arrondissement
SELECT annee, prix_m2_median
FROM ude.indicateurs_gold
WHERE arrondissement = 7
ORDER BY annee;

-- Comparateur 2 arrondissements
SELECT arrondissement, prix_m2_median, score_attractivite,
       nb_logements_sociaux, taux_delinquance_global
FROM ude.indicateurs_gold
WHERE arrondissement IN (6, 11) AND annee = 2024;
```

---

## 2. Data Lake MinIO — format Parquet partitionné (C1.3)

### Couche raw — données brutes converties

Format : Parquet, aucune transformation métier, tous les champs en string (DVF).
Partition : `ingestion_year / ingestion_month / ingestion_day`

```
s3a://urban-data/raw/
├── dvf/
│   └── ingestion_year=2026/ingestion_month=06/ingestion_day=22/
│       └── part-00000-abc123.parquet    ← toutes colonnes CSV, tout en string
├── delinquance/
│   └── ingestion_year=2026/ingestion_month=06/ingestion_day=22/
│       └── part-00000-*.parquet         ← copie fidèle du parquet INSEE
├── logements_sociaux/
│   └── ingestion_year=2026/ingestion_month=06/ingestion_day=22/
│       └── part-00000-*.parquet         ← structure JSON aplatie par Spark
└── revenus/
    └── ingestion_year=2026/ingestion_month=06/ingestion_day=22/
        └── part-00000-*.parquet         ← XLSX converti via pandas bridge
```

### Couche silver — nettoyée et normalisée

Format : Parquet, types castés, champs utiles uniquement.
Même schéma de partition.

```
s3a://urban-data/silver/
├── transactions/                         ← DVF nettoyé
│   └── ingestion_year=2026/.../
│       colonnes : date_mutation, annee, arrondissement, code_commune,
│                  valeur_fonciere, surface_reelle_bati, nombre_pieces_principales,
│                  type_local, prix_m2, nature_mutation
├── delinquance/
│   └── ingestion_year=2026/.../
│       colonnes : annee, arrondissement, CODGEO_2025,
│                  indicateur, nombre, taux_pour_mille
├── logements_sociaux/
│   └── ingestion_year=2026/.../
│       colonnes : annee, arrondissement, adresse_programme, code_postal,
│                  nb_logmt_total, nb_plai, nb_plus, nb_pls, mode_real
└── revenus/
    └── ingestion_year=2026/.../
        colonnes : IRIS, LIBIRIS, COM, LIBCOM,
                   DEC_MED18, DEC_GI18, DEC_RD18, DEC_Q118, DEC_Q318,
                   DEC_D118..DEC_D918
```

---

## 3. MongoDB — documents NoSQL (C1.2)

### Collection `air_quality`

Cas d'usage : cache des prévisions Airparif avec expiration automatique après 24h.

Index :
- TTL sur `inserted_at` → `expireAfterSeconds: 86400`
- Index simple sur `insee`

```json
{
  "_id": ObjectId("..."),
  "insee": "75107",
  "inserted_at": ISODate("2026-06-22T08:00:00Z"),
  "timestamp": "2026-06-22T08:00:00+00:00",
  "data": [
    {
      "date": "2026-06-22",
      "label": "Bon",
      "code_qual": 2
    },
    {
      "date": "2026-06-23",
      "label": "Moyen",
      "code_qual": 3
    }
  ]
}
```

---

## 4. Score attractivité — calcul Spark

Le score est calculé dans `datamart.py` par normalisation Min-Max sur la fenêtre annuelle (`Window.partitionBy("annee")`).

```
score = revenu_norm × 35
      + (1 - delinquance_norm) × 30
      + (1 - prix_norm) × 25
      + logements_norm × 10
```

Chaque composante est normalisée entre 0 et 1 sur les 20 arrondissements pour l'année donnée.
Le score final est dans [0, 100].

Interprétation :
- Score élevé (>70) : arrondissement attractif — revenus hauts, faible délinquance, prix accessibles, mixité sociale
- Score faible (<30) : prix très élevés ou délinquance importante

---

## 5. Star schema analytique (vue d'ensemble)

```
                    ┌─────────────────────┐
                    │    FAIT             │
                    │  indicateurs_gold   │
                    │  (arrondissement,   │
                    │   annee)            │
                    └──────┬──────────────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
  ┌──────────────┐ ┌─────────────┐ ┌──────────────┐
  │   DIM        │ │   DIM       │ │   DIM        │
  │arrondissement│ │   temps     │ │  source      │
  │  geom PostGIS│ │  2020–2025  │ │  DVF/logts/  │
  │  population  │ │             │ │  délinquance │
  └──────────────┘ └─────────────┘ └──────────────┘
```

L'API FastAPI interroge directement `ude.indicateurs_gold` en jointure avec `ude.arrondissements` pour les endpoints dashboard. Aucune agrégation à la volée — tout est pré-calculé par Spark.
