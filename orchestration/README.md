# orchestration — Orchestration des pipelines (C2.4)

Planification et monitoring de l'exécution des pipelines via Apache Airflow.

## Structure cible (Phase 2–3)

```
orchestration/
└── airflow/
    └── dags/
        ├── dag_batch_daily.py      # Ingestion + Silver + Gold (quotidien)
        ├── dag_feeder_dvf.py       # Ingestion DVF (annuel)
        ├── dag_silver_all.py       # Tous les processors Silver
        └── dag_gold.py             # Agrégation Gold + chargement BDD
```

## Fréquences

| DAG | Fréquence | Sources |
|-----|-----------|---------|
| `dag_batch_daily` | Quotidien 2h | Logements sociaux API |
| `dag_feeder_dvf` | Annuel (manuel) | DVF CSV |
| `dag_silver_all` | Après batch | Tous les Silver |
| `dag_gold` | Après Silver | Gold + BDD |

## Mesure de performance (C2.4)

Chaque tâche Airflow appelle un opérateur Python décoré avec `@log_pipeline_run`.
Les métriques (durée, nb_lignes, volume_mb) sont écrites dans `ude.pipeline_runs` (PostgreSQL).

## Techno

`Apache Airflow 2.9` (image Docker officielle `apache/airflow:2.9.2`)

## Dépendances

- Services PostgreSQL, MongoDB, Kafka opérationnels
- Pipelines Silver et Gold implémentés (Phase 2)
