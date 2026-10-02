# JEV — Real-Time Multi-Broker Trading Engine

> **Solo backtest / paper trading.** No hay ejecución con dinero real: el modo live está bloqueado en código.
> **Fase 2 completada**: el pipeline completo funciona en local sobre un mercado sintético y un broker simulado.

Plataforma para responder con evidencia una pregunta: *¿posee JEV una ventaja estadística reproducible después de
costes y bajo condiciones reales de mercado, y podemos ejecutarla de forma controlada y auditable?*

El sistema separa estrictamente `MARKET DATA → DATA QUALITY → FEATURES → JEV → SIGNAL → RISK → EXECUTION → BROKER →
ANALYTICS`, es multi-broker por diseño (Mock hoy; Alpaca Paper en la Fase 4; IBKR Paper en la Fase 8) y usa
exclusivamente APIs oficiales de brokers. TradingView y la automatización de navegador nunca forman parte de la ruta
de ejecución.

## Estado

| Fase | Contenido | Estado |
|---|---|---|
| 0 | Inspección (repositorio vacío; JEV no existía) | ✅ |
| 1 | Arquitectura, interfaces, entidades, configuración | ✅ |
| 2 | Mock market + mock broker + pipeline completo en local | ✅ |
| 3–10 | Backtesting, Alpaca Paper, API, dashboard, shadow, IBKR, comparación, live readiness | pendiente |

**Importante:** JEV aún no es un modelo validado. La versión incluida (`jev-heuristic 0.1.0`) es un sustituto
transparente para ejercitar el pipeline; no se le atribuye ninguna ventaja. Los datos actuales son sintéticos.

## Quickstart

Requiere Python 3.11+. Todo funciona sin red, sin claves y sin dinero real.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Simulación completa (backtest sobre el mercado sintético; SQLite en data/jev.db)
python -m apps.trading_engine simulate --start 2024-03-04 --end 2024-03-08

# Auditoría: últimas señales de un run y la cadena completa de una decisión
python -m apps.trading_engine trace --list 10 --run-id <run_id>
python -m apps.trading_engine trace <signal_id>

# Reproducir y comprobar todas las decisiones de un run desde la base de datos
python -m apps.trading_engine verify --run-id <run_id>

# Kill switch (estado, activación y reset manual identificado)
python -m apps.trading_engine kill-switch status
python -m apps.trading_engine kill-switch engage --by <nombre> --note "<motivo>"
python -m apps.trading_engine kill-switch reset --by <nombre> --note "<motivo>"

# Calidad
pytest            # unit, contract, integration y e2e (offline, ~1 min)
ruff check . && ruff format --check . && mypy
```

`run` (paper trading contra Alpaca) llega en la Fase 4: hoy informa de ello y sale con código 2.
Con Docker: `cp .env.example .env`, definir `POSTGRES_PASSWORD` y `docker compose up --build` (PostgreSQL + Redis +
motor ejecutando una simulación).

Opciones útiles de `simulate`: `--symbols MOCKA,MOCKB`, `--model baseline-random|baseline-flat`, `--run-id`,
`--max-events N` (simula una caída), `--json`. Configuración: `config/default.yaml`, perfiles con `--config`,
variables `JEV__SECCION__CLAVE` y `--db` / `DATABASE_URL`.

## Documentación

- [Arquitectura](docs/ARCHITECTURE.md)
- [Brokers y market data](docs/BROKER_ARCHITECTURE.md)
- [Riesgo y kill switch](docs/RISK.md)
- [Ejecución, idempotencia y recuperación](docs/EXECUTION.md)
- [Plan de implementación](docs/IMPLEMENTATION_PLAN.md)

## Seguridad

- Las credenciales van solo en variables de entorno / `.env` (no versionado). Ver [.env.example](.env.example).
- Nunca API keys en Git, en el frontend, en logs ni en la base de datos.

## Aviso

Nada en este repositorio es asesoramiento financiero ni de inversión. Los parámetros de riesgo son valores
iniciales de software. Los resultados históricos o simulados no garantizan resultados futuros.
