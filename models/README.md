# models/

Artefactos de modelos entrenados (Fase 3). Este directorio **no se versiona** (ver `.gitignore`), salvo este README.

Cada artefacto se registra en la tabla `model_versions` con `model_name`, `model_version`, `feature_version`,
`dataset_version`, `training_date`, `git_commit` y sus parámetros, para que cualquier predicción pueda
reproducirse con `python -m apps.trading_engine verify`.

Hoy no hay artefactos: `jev-heuristic 0.1.0` es un sustituto determinista sin parámetros entrenados.
