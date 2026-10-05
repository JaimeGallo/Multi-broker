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
| 3 | Datos históricos (Alpaca SIP), backtests y experimentos entre modelos | 🚧 en curso |
| 4 | Alpaca Paper en tiempo real (datos IEX, órdenes bracket, reconciliación) | 🚧 en curso: falta la primera sesión real |
| 5–10 | API, dashboard, shadow, IBKR, comparación, live readiness | pendiente |

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

`run` hace paper trading en tiempo real contra Alpaca (sección Fase 4).
Con Docker: `cp .env.example .env`, definir `POSTGRES_PASSWORD` y `docker compose up --build` (PostgreSQL + Redis +
motor ejecutando una simulación).

Opciones útiles de `simulate`: `--symbols MOCKA,MOCKB`, `--model baseline-random|baseline-flat`, `--run-id`,
`--max-events N` (simula una caída), `--json`. Configuración: `config/default.yaml`, perfiles con `--config`,
variables `JEV__SECCION__CLAVE` y `--db` / `DATABASE_URL`.

## Fase 3: datos reales y experimentos

Requiere las claves **paper** de Alpaca en `.env` (`APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`). El plan gratuito
basta para el histórico SIP.

```bash
# 1. Descargar un año de barras de 1 minuto del universo acordado (~250 peticiones; respeta 200/min)
python -m apps.trading_engine --config config/profiles/phase3.yaml data download --name sip-2024 --start 2024-01-02 --end 2024-12-31
python -m apps.trading_engine data info sip-2024 --verify

# 1b. Medir el spread típico de cada símbolo con cotizaciones SIP reales (~900 peticiones, ~5 min).
#     A partir de ahí los backtests de ese dataset usan spreads medidos en lugar del supuesto fijo.
python -m apps.trading_engine --config config/profiles/phase3.yaml data spreads sip-2024

# 2. Un backtest sobre datos reales (mismo motor, auditoría y verify que en simulación)
python -m apps.trading_engine --config config/profiles/phase3.yaml simulate --dataset sip-2024 --start 2024-01-02 --end 2024-01-31

# 3. Comparar modelos fold a fold (meses), en paralelo
python -m apps.trading_engine --config config/profiles/phase3.yaml experiment --dataset sip-2024 \
    --models jev-heuristic,baseline-ma,baseline-random,baseline-flat --reference baseline-flat --workers 4
```

Duración orientativa: ~2 s por símbolo y día con un modelo que opera mucho, dividido entre los procesos.
`simulate` y `experiment` muestran cada 30 s el avance (sesiones, trabajos, tiempo y estimación restante).

Cada fold mensual arranca con el mismo calentamiento que `run` en vivo: las barras de las 2 sesiones anteriores
llenan la ventana de features sin operar (`--warmup-sessions`, 0 para desactivarlo). Sin él, con decisiones de
5 minutos se perdían unas 1,3 sesiones al inicio de cada mes.

Auditoría de los experimentos (`--audit`):

- `lean` (por defecto): guarda señales, decisiones de riesgo, órdenes, trades y eventos, pero no cada barra,
  vector de features y predicción, y borra la base de cada trabajo al terminar (`--keep-dbs` la conserva).
  Un año de 10 símbolos ocupa poco disco y memoria.
- `full`: guarda todo y conserva las bases para `verify`. Cuesta unos 4 KB por barra y símbolo (un año de 10
  símbolos son ~4 GB por modelo); úsalo en rangos cortos (`--start/--end`).

Antes de empezar, `experiment` estima el disco necesario y se niega si no cabe con 1 GB de margen. Si la base de
datos falla de forma persistente (disco lleno), el trabajo se detiene y queda como FAILED en lugar de acumular
registros en memoria (`persistence.max_buffered_records`).

## Fase 4: paper trading en tiempo real (Alpaca)

Datos reales del mercado y la cuenta **paper** de Alpaca: las órdenes las simula Alpaca y no se mueve dinero.
El adaptador solo acepta `paper-api.alpaca.markets`; live sigue bloqueado en el código.

```bash
pip install -e ".[alpaca]"               # websockets para los streams en tiempo real
# .env: APCA_API_KEY_ID / APCA_API_SECRET_KEY (las mismas claves paper de las descargas)
python -m apps.trading_engine --config config/profiles/alpaca-paper.yaml run              # hasta el cierre
python -m apps.trading_engine --config config/profiles/alpaca-paper.yaml run --minutes 30 # prueba corta
```

Prueba del ciclo de órdenes real (con el mercado abierto, unos 2 minutos, centavos de dinero simulado):

```bash
python -m apps.trading_engine --config config/profiles/alpaca-paper.yaml paper-check   # 1 acción de SPY
```

Compra con bracket, comprueba la ejecución y las patas por `trade_updates`, cancela las patas, vende y verifica
que la cuenta queda plana. Se niega si el símbolo ya tiene posición u órdenes; si algo falla, cancela lo que abrió
y cierra lo que compró. Todo queda en la base de auditoría.

Qué hace `run`:

1. Conecta con la cuenta paper, lee el calendario oficial y mide el desfase del reloj (más de 2 s bloquea las
   entradas; en Windows: `w32tm /resync` como administrador).
2. Si el mercado aún no abrió, espera a la apertura (`--no-wait` para salir); si ya cerró, informa y sale.
3. Reconcilia órdenes y posiciones con Alpaca: cualquier orden o posición desconocida activa el kill switch.
4. Calienta las features con las barras de la sesión anterior y de hoy (REST); con ellas nunca opera.
5. Opera con el stream IEX: una decisión por barra, órdenes bracket (stop y objetivo en Alpaca), salidas por
   horizonte y cierre 5 min antes del final. Imprime un resumen cada 5 min y otro al terminar.
6. Ctrl+C detiene de forma segura: las posiciones abiertas conservan su stop y objetivo en Alpaca y el próximo
   `run` las reconcilia.

Feed IEX (gratis): precios reales de una sola bolsa; algunas barras faltan. Cuando una barra en vivo salta
minutos, el adaptador pide los que faltan por REST antes de entregarla (también en la unión entre el
calentamiento y el stream). Si un minuto sigue faltando es que no hubo operaciones en IEX: el perfil lo tolera
(`data_quality.ignore_gaps_up_to_bars: 1`); huecos mayores siguen bloqueando entradas. El resumen final muestra
`quality issues` (motivo de cada barra degradada) y `feed` (huecos revisados, barras recuperadas, reconexiones). Las comisiones reportadas son tasas regulatorias estimadas (Alpaca paper no cobra).
El kill switch de paper es persistente entre ejecuciones: si se activa, revisar y `kill-switch reset`.

## JEV con TypeSafe (opcional, desactivado)

El adaptador `typesafe-jev` delega la decisión de dirección en el modelo Jev de TypeSafe AI. No se usa salvo que
se elija su perfil; detalles, coste y limitaciones en [docs/JEV_TYPESAFE.md](docs/JEV_TYPESAFE.md).

```bash
pip install -e ".[typesafe]"            # SDK oficial
# en .env: TYPESAFE_API_KEY=...          (consola: console.typesafe.ai)
python -m apps.trading_engine --config config/profiles/typesafe-jev.yaml jev-check
python -m apps.trading_engine --config config/profiles/typesafe-jev.yaml simulate --start 2024-03-04
```

## Documentación

- [Arquitectura](docs/ARCHITECTURE.md)
- [Brokers y market data](docs/BROKER_ARCHITECTURE.md)
- [Riesgo y kill switch](docs/RISK.md)
- [Ejecución, idempotencia y recuperación](docs/EXECUTION.md)
- [Plan de implementación](docs/IMPLEMENTATION_PLAN.md)
- [JEV con TypeSafe Jev (adaptador opcional)](docs/JEV_TYPESAFE.md)

## Seguridad

- Las credenciales van solo en variables de entorno / `.env` (no versionado). Ver [.env.example](.env.example).
- Nunca API keys en Git, en el frontend, en logs ni en la base de datos.

## Aviso

Nada en este repositorio es asesoramiento financiero ni de inversión. Los parámetros de riesgo son valores
iniciales de software. Los resultados históricos o simulados no garantizan resultados futuros.
