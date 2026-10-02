# Arquitectura de brokers y market data

> Objetivo: que JEV, el Signal Engine, el Risk Engine y el Execution Engine **nunca** importen clases de Alpaca,
> Interactive Brokers o de cualquier otro proveedor. Todo lo específico vive detrás de dos contratos:
> `BrokerAdapter` y `MarketDataAdapter`.

Estado: `MockMarketDataAdapter` implementado; `MockBrokerAdapter` y los tests de contrato en implementación
(Fase 2). Alpaca (Fase 4) e IBKR (Fase 8) están **diseñados** aquí; aún no hay código que hable con ellos.

---

## 1. Capas

```text
TradingEngine ─► BrokerExecutionEngine ─► BrokerRouter ─► BrokerAdapter ─► API oficial del broker
                        ▲                                        │
                        └──────────── OrderEvent (normalizado) ◄─┘

Runner ◄─ MarketDataAdapter.stream() ◄─ WebSocket/REST oficial del proveedor
```

- El **adapter** traduce en ambos sentidos y no contiene lógica de trading.
- El **router** elige el adapter; nunca modifica órdenes ni señales.
- El **execution engine** es dueño de la idempotencia y de la máquina de estados.

---

## 2. Contrato `BrokerAdapter` (`packages/brokers/base/adapter.py`)

| Método | Semántica obligatoria |
|---|---|
| `capabilities` | `BrokerCapabilities`: `is_paper`, clases de activo, cortos, fraccionales, bracket, replace, longitud máx. de `client_order_id`. **`is_paper` debe ser veraz**: el motor se niega a operar si una cuenta no-paper aparece en un modo no-live. |
| `connect()` / `disconnect()` | Idempotentes. |
| `health()` | `BrokerHealth`: conexión REST, stream de órdenes, cuenta disponible, latencia, último evento, hora del servidor. |
| `get_account()` | `AccountSnapshot` con `equity`, `cash`, `buying_power`, `last_equity` (equity al cierre anterior → PnL diario). |
| `get_positions()` | Posiciones con cantidad **con signo** (+ largo, − corto). |
| `get_orders(status)` | Órdenes `open` / `closed` / `all`, con piernas (`legs`) anidadas cuando existan. |
| `submit_order(req)` | Devuelve la vista del broker (normalmente `ACKNOWLEDGED`). Errores: `OrderRejected` (rechazo síncrono), `DuplicateClientOrderId`, `AmbiguousSubmission` (resultado desconocido: timeout / red), `BrokerUnavailable` (no se llegó a enviar). **Nunca** devuelve `FILLED` sin confirmación del broker. |
| `cancel_order(id)` | Cancelar una orden terminal es un *no-op*; una orden desconocida → `OrderNotFound`. |
| `replace_order(id, changes)` | Cambia cantidad / precios / TIF de una orden abierta. |
| `get_order(id)` | `None` si el broker no la conoce (clave para reconciliar envíos ambiguos). |
| `stream_order_events()` | `AsyncIterator[OrderEvent]` normalizado, con la instantánea completa de la orden tras el evento. Entrega *at-least-once*: el consumidor deduplica por `event_id` y por cantidad acumulada. |
| `get_instrument(symbol)` | `InstrumentInfo`: operable, *shortable*, fraccionable, tick, cantidad mínima. |

**Identificadores.** El identificador canónico en todo el sistema es `client_order_id` (≤ 64 caracteres,
`[A-Za-z0-9._:-]`). Los parámetros `order_id` del spec se interpretan como este identificador; cada adapter
mantiene el mapeo con el id nativo del broker (`broker_order_id`).

## 3. Contrato `MarketDataAdapter` (`packages/market_data/base.py`)

| Método | Semántica |
|---|---|
| `subscribe_quotes / subscribe_trades / subscribe_bars` | Registran símbolos; los eventos llegan por `stream()`. |
| `stream()` | Eventos `MarketBar`/`MarketQuote`/`MarketTrade` **ya normalizados**, en orden temporal, con timestamps UTC. |
| `get_historical_bars(symbol, start, end, timeframe)` | Barras ordenadas en `[start, end)`; usado para *warm start* y relleno de huecos. |
| `health()` | Conexión, último mensaje, suscripciones. |

Reconexión, *buffering* y *backfill* tras una desconexión son responsabilidad del adapter; la detección de
huecos, duplicados y desorden la hace `MarketDataEngine` independientemente del proveedor.

## 4. Normalización

Entidades comunes: `MarketBar`, `MarketQuote`, `MarketTrade`, `MarketTick` (ver `ARCHITECTURE.md` §6).
Convención de barras: `start` = inicio del intervalo (UTC); la barra está disponible en `end`.

Mapeo previsto para Alpaca (a validar contra la documentación oficial al implementar la Fase 4):

| Alpaca (stream v2) | Común |
|---|---|
| barra `t, o, h, l, c, v, vw, n` | `MarketBar(start=t, open, high, low, close, volume, vwap, trade_count)` |
| quote `t, bp, bs, ap, as` | `MarketQuote(timestamp, bid, bid_size, ask, ask_size)` |
| trade `t, p, s, x, c` | `MarketTrade(timestamp, price, size, exchange, conditions)` |

Mapeo de estados de orden (propuesta; validar en Fase 4 / Fase 8):

| Común | Alpaca | IBKR (TWS) |
|---|---|---|
| `SUBMITTED` | `pending_new` | `PendingSubmit`, `ApiPending` |
| `ACKNOWLEDGED` | `new`, `accepted`, `held` (piernas bracket) | `PreSubmitted`, `Submitted` |
| `PARTIALLY_FILLED` | `partially_filled` | `Submitted` con `filled > 0` |
| `FILLED` | `filled` | `Filled` |
| `CANCEL_REQUESTED` | `pending_cancel` | `PendingCancel` |
| `CANCELLED` | `canceled` | `Cancelled`, `ApiCancelled` |
| `EXPIRED` | `expired` (y `done_for_day` hasta el siguiente evento) | TIF vencido |
| `REJECTED` | `rejected` | `Inactive` (según motivo) |
| `ERROR` | estado inesperado / incoherente | estado inesperado / incoherente |

Cualquier estado desconocido se normaliza a `ERROR` y dispara reconciliación: nunca se asume un fill.

---

## 5. `MockBrokerAdapter` — exchange simulado (implementado)

Es a la vez el broker de desarrollo, el motor de ejecución del backtest y (Fase 7) el ejecutor del modo shadow.

- **Cuenta:** efectivo inicial configurable, equity = efectivo + Σ cantidad × último precio; `last_equity` se fija
  al cambiar de sesión; poder de compra = equity × multiplicador − exposición − órdenes de entrada pendientes.
- **Órdenes:** market, limit, stop; TIF `day`/`gtc`; bracket (entrada + TP limit + SL stop, OCO); cancel; replace.
- **Fills sin look-ahead:** una orden solo puede ejecutarse en barras que empiezan **después** de su envío.
  - Market: apertura de la barra ± (medio spread + slippage), redondeo adverso al tick.
  - Limit: si la barra abre mejor que el límite → precio de apertura; si el rango **atraviesa** el límite → al límite.
  - Stop: si la barra abre más allá del stop (gap) → apertura + slippage; si el rango toca el stop → stop + slippage.
  - Bracket: piernas activas tras el fill de la entrada; en la barra de entrada solo puede saltar el SL
    (política `conservative`); si en una barra caben TP y SL, se asume el **SL primero**.
  - Fills parciales opcionales (órdenes simples) limitados por `participation_rate × volumen`.
- **Reglas del broker imitadas:** `client_order_id` duplicado → error; cantidad retenida por piernas abiertas no se
  puede volver a vender; órdenes que invertirían la posición en un solo paso → rechazo; símbolos no *shortables*
  → rechazo; poder de compra insuficiente → rechazo; órdenes `day` expiran al cambiar de sesión.
- **Costes:** el mismo `CostModel` que usa el Signal Engine para estimar el valor esperado (comisiones, tasas
  regulatorias en ventas, spread, slippage).
- **Inyección de fallos (tests):** rechazo, *timeout* antes/después de aceptar, broker no disponible, desconexión.

---

## 6. `AlpacaBrokerAdapter` / `AlpacaMarketDataAdapter` — diseño (Fase 4)

Solo APIs oficiales, solo **paper**. Nada de scraping ni clicks.

- **Implementación:** REST con `httpx` y streams con `websockets`, sin `alpaca-py` (mismas APIs oficiales;
  los tests sustituyen la red por un Alpaca en memoria). Handshakes, rutas y estados contrastados con el código
  de `alpaca-py`: `trade_updates` en `wss://paper-api.alpaca.markets/stream` con
  `{"action":"authenticate","data":{"key_id","secret_key"}}` y `listen`; datos en
  `wss://stream.data.alpaca.markets/v2/{iex|sip}` con `auth` y `subscribe` (`b` barras, `q` quotes, `u` barras
  corregidas, ignoradas).
- **Patas del bracket:** Alpaca les asigna ids propios; la plataforma las nombra `<entrada>-tp` / `<entrada>-sl`
  y guarda el mapeo, así reinicios y reconciliación no dependen de ids inventados por el broker.
- **Verificación de paper:** el adapter se niega a construirse si `paper` no es true o si el endpoint de trading
  o de `trade_updates` no es `paper-api.alpaca.markets`; por eso `is_paper=True` es cierto por construcción.
- **Órdenes:** `POST /v2/orders` de tipo market con `order_class=bracket`, `take_profit.limit_price`,
  `stop_loss.stop_price` (redondeados al centavo), `time_in_force=day` y `client_order_id` determinista.
- **Idempotencia:** Alpaca rechaza un `client_order_id` repetido → el adapter lo traduce a
  `DuplicateClientOrderId` y el execution engine recupera la orden existente por `client_order_id`.
- **Eventos:** `trade_updates` (`new, fill, partial_fill, canceled, expired, replaced, rejected, pending_*`, …)
  → `OrderEvent` con la instantánea de la orden incluida en el mensaje.
- **Market data:** feed `iex` por defecto (disponible en el plan gratuito según la documentación de Alpaca; `sip`
  requiere suscripción — verificar condiciones vigentes). Con IEX, el volumen y el spread reflejan solo parte del
  mercado: las features de volumen deben interpretarse con cuidado.
- **Reconexión:** *backoff* exponencial con *jitter*; tras reconectar, *backfill* de barras por REST y
  reconciliación de órdenes (`get_orders(status=all, after=último evento)`).
- **Calendario y reloj:** `get_clock()` / `get_calendar()` alimentan un `MarketCalendar` con festivos y cierres
  anticipados reales, y el check `clock_synchronized` compara la hora local con la del servidor.
- **Límites de uso:** respetar el *rate limit* de la API (verificar el valor vigente); las consultas por decisión
  se limitan a la cuenta y al instrumento, solo cuando una señal es elegible.
- **Restricciones que el Risk Engine debe conocer:** brackets no admiten fraccionales ni *extended hours*;
  los cortos requieren cuenta de margen y acciones *easy-to-borrow*; la *short sale rule* (SEC Rule 201, SSR)
  restringe cortos tras una caída del 10 % y aún no se modela. La regla de *pattern day trader* (USD 25.000)
  fue eliminada: la SEC aprobó el cambio a FINRA Rule 4210 el 14/04/2026, efectivo el 04/06/2026, con margen
  intradía proporcional a la exposición (verificado el 02/10/2026; confirmar la implementación de Alpaca).
- **Credenciales:** `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY` en `.env`. Nunca en YAML, DB, logs o frontend.

## 7. `IBKRBrokerAdapter` / `IBKRMarketDataAdapter` — diseño (Fase 8)

No es requisito del MVP. Se implementa después de validar Alpaca, con los mismos tests de contrato.

- **Conectividad:** TWS o IB Gateway en modo **paper** vía la TWS API, con la librería `ib_async`
  (sucesora mantenida de `ib_insync`). Host/puerto/clientId por configuración (los puertos por defecto de paper
  suelen ser 7497 en TWS y 4002 en Gateway — verificar en la instalación).
- **Identificadores:** `orderRef` = `client_order_id`; `permId` como id estable del broker. Antes de reenviar,
  buscar por `orderRef` en las órdenes abiertas y completadas.
- **Bracket:** orden padre + TP + SL enlazadas con `parentId`, `transmit=False` en todas salvo la última.
- **Eventos:** `orderStatus`, `execDetails`, `commissionReport` → `OrderEvent` (comisiones reales).
- **Operación:** límites de mensajes por segundo (pacing), suscripciones de market data, reinicio diario de
  TWS/Gateway → reconexión y reconciliación automáticas.

## 8. `BrokerRouter` (`packages/brokers/router.py`)

```yaml
broker:
  mode: paper
  active: mock          # inicial: un único broker
  routing: {}           # posterior: {US_EQUITY: alpaca, FUTURES: ibkr}
  mock:   {enabled: true}
  alpaca: {enabled: false}
  ibkr:   {enabled: false}
```

`select_broker(symbol, asset_class, strategy, execution_requirements)` devuelve un adapter habilitado que cumple
los requisitos (bracket, cortos, fraccionales). Si ninguno cumple → `BrokerUnavailable` → la señal se rechaza
con motivo `broker_unavailable`. El router nunca cambia tamaño, precio ni dirección.

## 9. Tests de contrato (`tests/contract/`)

`test_broker_contract.py` define un conjunto de pruebas genéricas parametrizadas por adapter (hoy `mock`).
Alpaca e IBKR se añadirán a la lista con *skip* automático si no hay credenciales / gateway, y con un marcador
para las pruebas que requieren mercado abierto. Mismo enfoque para `MarketDataAdapter`.

## 10. TradingView y automatización de navegador — política

- TradingView **no** forma parte de la ruta de ejecución. Usos permitidos: visualización, análisis, validación
  manual y, opcionalmente (fase posterior), alertas vía webhook como **fuente auxiliar de señales**.
- Un webhook nunca es una orden: entra como `Signal(source="tradingview")` y pasa por validación, Signal Engine
  (valor esperado), Risk Engine y Execution Engine como cualquier otra señal. La caída de TradingView no afecta al motor.
- La automatización de navegador (abrir web, login, pulsar BUY) **no** se usa para ejecutar operaciones. Solo
  para supervisión, investigación o comprobaciones manuales. Si existe API oficial, se usa la API.

## 11. Comparación de ejecución entre brokers (Fase 9)

La misma señal (mismo `signal_id`, misma decisión de riesgo) se registra con su **precio de referencia** (cierre
de la barra de decisión) y se compara con el fill de cada broker y con el fill simulado:

```text
Signal: LONG NVDA · referencia 182.40
Simulado 182.40 · Alpaca 182.43 · IBKR 182.42  → slippage por broker, latencia de ack y de fill, fill rate, comisiones
```

Las métricas del **modelo** (acierto direccional, PnL a precios de referencia) se calculan sin depender del broker;
las de **ejecución** (slippage, latencias, rechazos) se segmentan por broker.
