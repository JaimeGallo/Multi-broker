# JEV — Real-Time Multi-Broker Trading Engine

> **Solo backtest / paper trading.** No hay ejecución con dinero real: el modo live está bloqueado en código.
> **Trabajo en curso** (Fase 2).

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
| 2 | Mock market + mock broker + pipeline completo en local | 🚧 en curso |
| 3–10 | Backtesting, Alpaca Paper, API, dashboard, shadow, IBKR, comparación, live readiness | pendiente |

**Importante:** JEV aún no es un modelo validado. La versión incluida (`jev-heuristic 0.1.0`) es un sustituto
transparente para ejercitar el pipeline; no se le atribuye ninguna ventaja. Los datos actuales son sintéticos.

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
