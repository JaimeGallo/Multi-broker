# Plan de implementación

Secuencia obligatoria (spec §76):
`DATA → BACKTEST → WALK-FORWARD → REPLAY → REAL-TIME SHADOW → PAPER → MULTI-BROKER PAPER → LIVE READINESS → LIVE`.
Nunca `JEV → dinero real`.

## Estado

| Fase | Contenido | Estado |
|---|---|---|
| 0 | Inspección | ✅ repositorio vacío; JEV no existía |
| 1 | Arquitectura, interfaces, entidades, DB, eventos, configuración | ✅ |
| 2 | Mock market + mock broker + pipeline completo en local | ✅ |
| 3 | Backtesting real, walk-forward, baselines, JEV v1 entrenado | 🚧 en curso (datos, backtests y experimentos listos) |
| 4 | Alpaca Paper (market data + broker) + runner en tiempo real | pendiente — requiere claves paper del usuario |
| 5 | Motor en tiempo real endurecido + API + WebSockets + métricas | pendiente |
| 6 | Dashboard React | pendiente |
| 7 | Shadow sobre mercado real | pendiente |
| 8 | IBKR Paper | pendiente |
| 9 | Comparación entre brokers | pendiente |
| 10 | Live readiness (checklist) | pendiente — live sigue deshabilitado |

---

## Fase 0 — Inspección ✅

- Directorio vacío, sin git, sin datasets, sin código previo. Confirmado por el usuario: crear desde cero.
- Entorno local detectado: Python 3.14, git, Docker.
- Decisión: JEV se define como contrato; el modelo real se investiga en la Fase 3 (no se inventa).

## Fase 1 — Arquitectura ✅

Entregables: `ARCHITECTURE.md`, `BROKER_ARCHITECTURE.md`, `RISK.md`, `EXECUTION.md`, este plan; interfaces
`MarketDataAdapter`, `BrokerAdapter`, `ExecutionEngine`, `JEVModel`, `RiskEngine`; entidades normalizadas;
esquema de base de datos; bus de eventos; configuración validada; puerta de seguridad para live.

## Fase 2 — Mock market y pipeline local ✅

Entregables:

- `MockMarketDataAdapter` (generador sintético determinista con regímenes, sesiones y anomalías inyectables).
- `MockBrokerAdapter` (exchange simulado: bracket/OCO, fills sin look-ahead, costes, inyección de fallos).
- Pipeline completo: Market → Data Quality → Features → JEV → Régimen → Señal → Riesgo → Ejecución →
  Order events → Base de datos → Analytics.
- `SimulationRunner` (núcleo del futuro backtester), CLI (`simulate`, `trace`, `verify`, `kill-switch`).
- Tests unitarios, de contrato, de integración (auditoría completa, determinismo, idempotencia, reinicios,
  reconciliación, kill switch) y e2e de la CLI.

Criterio de salida: `pytest` en verde; una simulación deja la cadena de auditoría completa para cada orden y
`verify` reproduce exactamente las decisiones.

Resultado: cumplido. 124 tests (unit, contract, integration, e2e) en verde junto con `ruff` y `mypy`; una
simulación de un día con dos símbolos reproduce el 100 % de sus decisiones con `verify`, en SQLite y en
PostgreSQL 16. Hallazgos corregidos durante la fase: las salidas por horizonte no cancelaban el bracket
(el broker las rechazaba y se activaba el kill switch) y el `SqlOrderStore` reconciliaba órdenes de otros runs
de la misma base de datos.

## Fase 3 — Backtesting y validación de JEV 🚧

Decisiones del usuario (02/10/2026): universo SPY, QQQ, IWM, AAPL, MSFT, NVDA, AMZN, META, JPM, XOM
(`config/profiles/phase3.yaml`); histórico **SIP** de Alpaca (plan Basic gratuito, ajustado por splits); cortos
permitidos con resultados separados largo/corto; variante con decisiones de 5 minutos y horizonte de 30
(`config/profiles/phase3-5min.yaml`).

Hecho:

- Descarga Alpaca (`data download`): calendario oficial (feriados y cierres anticipados), solo sesión regular,
  paginación y reintentos; dataset versionado (`manifest.json`, SHA-256 por fichero, `dataset_version`).
  Formato CSV comprimido en lugar de Parquet para no añadir dependencias.
- `HistoricalMarketDataAdapter`: el mismo motor, broker simulado y auditoría que en paper; `verify` reproduce las
  decisiones también sobre datos históricos.
- Tasas regulatorias reales en `costs` (verificadas el 02/10/2026).
- Baseline de medias móviles (`baseline-ma`) junto a `baseline-random` y `baseline-flat`.
- `experiment`: varios modelos, mismos datos y folds mensuales, en paralelo; PnL diario con IC 95 % bootstrap,
  largo/corto, por fold y símbolo, comparación pareada contra un modelo de referencia; `results.json` y tabla
  `experiments`.
- `walk_forward` con embargo (utilidad lista para los modelos entrenados).
- Rendimiento: backtests ~2,3 veces más rápidos y lineales en la duración (índices de órdenes abiertas, caché
  write-through del store, escrituras de auditoría en una transacción).

Pendiente: modelos entrenados (regresión logística, random forest, gradient boosting, JEV v1) con walk-forward y
tests anti-leakage; universo dinámico por volumen; spreads reales (quotes); SSR y coste de préstamo; Buy & Hold.

Candidato ya preparado: el adaptador `typesafe-jev` (TypeSafe Jev, desactivado por defecto; ver
[JEV_TYPESAFE.md](JEV_TYPESAFE.md)). Entra en la Fase 3 como un modelo más a validar contra los baselines, no como
modelo de producción asumido.

1. **Datos históricos**: `HistoricalMarketDataAdapter` (Alpaca historical API → caché Parquet) con
   `dataset_version` = hash de (símbolos, rango, feed, ajustes). Registro de *corporate actions* y del universo
   usado en cada fecha para evitar **survivorship bias**.
2. **BacktestEngine**: `SimulationRunner` + adapter histórico + `MockBrokerAdapter` (misma lógica que paper).
   Persistencia en `backtest_runs` / `backtest_trades`.
3. **CostModel** con variantes (optimista / base / pesimista) y fills parciales; informes bruto/costes/neto.
4. **PerformanceAnalyzer**: métricas del spec §37 segmentadas por símbolo, hora, régimen, confianza, versión y broker.
5. **WalkForwardEngine**: ventanas temporales TRAIN/VALIDATION/TEST con *purging* y *embargo* ≥ horizonte;
   nunca *random split* como método principal.
6. **Baselines** con idénticas condiciones: Buy & Hold, Random, Moving Average, Logistic Regression,
   Random Forest, Gradient Boosting.
7. **JEV v1**: primer modelo entrenado (candidato: clasificador de dirección + regresor de retorno/volatilidad a
   15 min sobre las features v0.x), calibración de probabilidades y registro en `model_versions`
   (`model_name, model_version, feature_version, dataset_version, training_date, git_commit`).
8. **Experiment tracking**: tabla/registro con experimento, dataset, features, hiperparámetros, periodos,
   métricas, commit y fecha.
9. **Tests anti-leakage**: target calculado solo con datos futuros al *as-of*; features invariantes al truncar el
   futuro; separación temporal estricta; sin normalizaciones ajustadas con datos de test.

Criterio de salida: informe walk-forward **después de costes**, con intervalos de confianza y comparación contra
baselines. La conclusión "JEV no tiene ventaja" es un resultado válido y se documentaría como tal.

## Fase 4 — Alpaca Paper

Prerrequisito del usuario: crear claves de **paper trading** en Alpaca y guardarlas en `.env`
(`APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`). Nunca en el repositorio ni en el chat.

1. `AlpacaMarketDataAdapter` (stream de barras/quotes, históricos, reconexión con backoff, backfill).
2. `AlpacaBrokerAdapter` (cuenta, posiciones, órdenes bracket, `trade_updates`, get por `client_order_id`).
3. `RealtimeRunner`: tareas asyncio para market stream, order stream y temporizadores; *warm start* de features
   con históricos; calendario real (`/v2/calendar`) y reloj (`/v2/clock`).
4. Perfil `config/alpaca-paper.yaml`; verificación de cuenta paper al arrancar.
5. Tests de contrato contra Alpaca Paper (opt-in, con *skip* sin credenciales; algunos solo con mercado abierto).

Criterio de salida: una sesión completa de paper con auditoría completa, reconciliación limpia al reiniciar
y cero órdenes duplicadas.

## Fase 5 — Motor en tiempo real + API

FastAPI con los endpoints del spec §56 (`/health`, `/system/status`, `/market/{symbol}`, `/signals`, `/orders`,
`/positions`, `/trades`, `/portfolio`, `/risk/status`, `/brokers`, `/models`, `/backtests`, `/trading/pause|resume|kill-switch`)
y WebSockets §57 (`/ws/market|signals|orders|portfolio|system`); autenticación por token con rol para
endpoints de control; **ningún endpoint puede activar live**. Puente Redis del bus, Prometheus/Grafana,
OpenTelemetry, migraciones Alembic, servicio `api` en docker-compose.

## Fase 6 — Dashboard

React: SYSTEM, MARKET, JEV, SIGNALS, RISK, ORDERS, POSITIONS, TRADES, PnL, BACKTESTS, MODELS, BROKERS.
Banner permanente e inequívoco del modo (`BACKTEST / SHADOW / PAPER / LIVE`). Sección BROKERS con estado,
cuenta, poder de compra, equity, posiciones, órdenes, latencia y último evento.

## Fase 7 — Shadow

`RealtimeRunner` + datos reales + `MockBrokerAdapter` como ejecutor: ninguna orden sale al broker. Informe
"ejecución esperada vs mercado real".

## Fase 8 — IBKR Paper

`IBKRMarketDataAdapter` + `IBKRBrokerAdapter` con `ib_async`; mismos tests de contrato y de recuperación.

## Fase 9 — Comparación entre brokers

Mismas señales, mismas decisiones de riesgo; fill rate, slippage, latencias, comisiones, PnL y calidad de
ejecución por broker, separadas de la calidad del modelo.

## Fase 10 — Live readiness

```text
[ ] Backtest validated            [ ] Risk engine validated
[ ] Walk-forward validated        [ ] Kill switch validated
[ ] Out-of-sample validated       [ ] Recovery validated
[ ] Paper validated               [ ] Monitoring validated
[ ] Shadow validated              [ ] Audit trail validated
[ ] Alpaca adapter validated      [ ] Broker reconciliation validated
[ ] IBKR adapter validated
```

Solo después se prepararía live, con decisión humana explícita, `LIVE_TRADING_ENABLED=true`, y la retirada
consciente del bloqueo en código. Nunca por despliegue automático.

---

## Decisiones abiertas (requieren al usuario)

1. **Qué es JEV conceptualmente**: ¿hay una idea/teoría/señal propia detrás del nombre, o se define por
   investigación en la Fase 3? Esto condiciona features, horizonte y familia de modelos.
2. **Universo inicial**: p. ej. SPY, QQQ y 5–10 acciones muy líquidas.
3. **Claves de Alpaca Paper** en `.env` (Fase 4).
4. **Feed de datos**: IEX (gratuito, parcial) vs SIP (de pago, completo).
5. **Cortos en paper**: activados por defecto (sujetos a *shortability*); ¿mantener?
6. **Timeframe y horizonte objetivo**: 1 min / 5 min; 5–30 min.

## Riesgos del proyecto

- Que JEV no tenga ventaja tras costes (resultado posible y aceptable).
- Datos IEX incompletos → sesgo en features de volumen/spread.
- Sobreajuste en walk-forward → número de pruebas limitado y registrado, validación anidada, embargo.
- Paper ≠ real: los fills en paper son optimistas; el modo shadow y el análisis de slippage lo acotan.
