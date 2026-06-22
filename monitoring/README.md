# monitoring — Métriques et logs (C2.4)

Suivi des performances des pipelines et de la santé des services.

## Structure

```
monitoring/
├── logs/          → fichiers .log rotatifs (config dans config/logging.yaml)
└── metrics/       → dashboards et exports de métriques
```

## Source de métriques principale

La table PostgreSQL `ude.pipeline_runs` contient pour chaque run :
- `pipeline_name`, `source`, `layer`
- `duration_s`, `nb_lignes_in`, `nb_lignes_out`, `volume_mb`
- `statut` (`success` / `failed`)

Requête utile pour la soutenance :
```sql
SELECT pipeline_name, AVG(duration_s), AVG(nb_lignes_out), COUNT(*)
FROM ude.pipeline_runs
WHERE statut = 'success'
GROUP BY pipeline_name
ORDER BY AVG(duration_s) DESC;
```

## Logs

Les logs JSON sont écrits dans `monitoring/logs/ude.log` (rotatif 10 MB, 5 backups).
Format JSON pour intégration avec des outils d'analyse (Loki, ELK, etc.).

## Phase 3 (optionnel)

- Prometheus + Grafana pour les métriques temps réel
- Alertes sur taux d'échec des pipelines

## Techno

`structlog`, `logging` (stdlib), `prometheus-client` (Phase 3)
