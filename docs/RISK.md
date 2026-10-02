# Risk Engine, límites y kill switch

> Los valores numéricos de este documento son **parámetros iniciales de software**, no recomendaciones
> financieras ni de inversión. Deben revisarse con evidencia de backtest/paper antes de cualquier uso real.

## 1. Autoridad e independencia

- El Risk Engine (`packages/risk/engine.py`) es la **autoridad final** sobre cada entrada. JEV propone; el riesgo
  puede rechazar cualquier señal, incluida una con probabilidad alta.
- No importa nada de `packages/jev`: recibe una `Signal` ya formada y un `RiskContext` construido a partir del
  broker (cuenta, instrumento), del portafolio local y del estado del sistema.
- Es determinista y sin E/S: `evaluate(signal, context) -> RiskDecision`. Cada decisión guarda **todos** los
  checks evaluados (aprobados y fallidos) con valor y límite, para auditoría.

## 2. Entradas (`RiskContext`)

| Campo | Origen |
|---|---|
| `account` (equity, cash, buying power, last_equity) | `broker.get_account()` en el momento de la decisión |
| `positions`, `gross_exposure`, `open_position_count` | `PortfolioTracker` (reconciliado con el broker) |
| `pending_entry_symbols` | `PositionManager` (entradas enviadas aún sin completar) |
| `instrument` (tradable, shortable, tick, mínimo) | `broker.get_instrument()` |
| `atr`, `reference_price` | `FeatureVector` de la decisión |
| `peak_equity` | máximo histórico de equity observado |
| `session` | `MarketCalendar` (apertura/cierre de la sesión) |
| `kill_switch_engaged`, `trading_paused`, `health_ok` | `KillSwitch`, `TradingControls`, `HealthMonitor` |
| `broker_capabilities` | adapter seleccionado por el router |

## 3. Checks (en orden)

| Check | Regla | Parámetro |
|---|---|---|
| `kill_switch` | kill switch no activado | — |
| `trading_not_paused` | trading no pausado manualmente | — |
| `system_health` | todos los health checks críticos OK | — |
| `signal_eligible` | la señal viene `ELIGIBLE` del Signal Engine | — |
| `signal_not_expired` | `now ≤ signal.expires_at` | `signals.signal_ttl_seconds` |
| `instrument_tradable` | el broker permite operar el símbolo | — |
| `short_allowed` / `instrument_shortable` | solo para SHORT | `risk.allow_short` |
| `market_open` / `entry_window` | sesión abierta, fuera de los primeros/últimos minutos | `no_entry_first_minutes`, `no_entry_last_minutes` |
| `daily_loss_limit` | `equity − last_equity > −max_daily_loss × last_equity` | `max_daily_loss` |
| `max_drawdown` | `1 − equity / peak_equity < max_drawdown` | `max_drawdown` |
| `max_open_positions` | posiciones abiertas + entradas pendientes < límite | `max_open_positions` |
| `no_position_in_symbol` | sin posición ni entrada pendiente en el símbolo (v1: sin piramidar) | — |
| `atr_available` | ATR finito y > 0 | — |
| `stop_distance_within_limits` | distancia del stop ≤ máximo | `min_stop_bps`, `max_stop_bps` |
| `stop_on_correct_side` | SL y TP del lado correcto del precio y > 0 | — |
| `risk_reward` | `TP_dist / SL_dist ≥ min_risk_reward` | `take_profit_rr`, `min_risk_reward` |
| `position_size` | cantidad final ≥ mínimo del instrumento | — |
| `total_exposure` | exposición bruta tras la entrada ≤ `max_total_exposure × equity` | `max_total_exposure` |
| `max_loss_per_trade` | `cantidad × SL_dist ≤ max_risk_per_trade × equity` | `max_risk_per_trade` |

Veredicto: `APPROVED` solo si **todos** pasan. Si fallan `daily_loss_limit` o `max_drawdown`, además se activa el
kill switch.

## 4. Stops, take profit y tamaño

```text
d_raw      = atr_stop_multiple × ATR
d          = max(d_raw, min_stop_bps × precio)          (rechazo si d > max_stop_bps × precio)
LONG :  SL = redondeo_abajo(precio − d)   TP = redondeo_arriba(precio + take_profit_rr × d)
SHORT:  SL = redondeo_arriba(precio + d)  TP = redondeo_abajo(precio − take_profit_rr × d)
SL_dist    = |precio − SL|   (tras redondear al tick)

qty_riesgo = floor(equity × max_risk_per_trade / SL_dist)          ← sizing inicial: risk_amount / stop_distance
qty        = min(qty_riesgo,
                 floor(equity × max_symbol_exposure / precio),
                 floor((equity × max_total_exposure − exposición_bruta) / precio),
                 floor(buying_power × buying_power_usage / precio))
max_loss   = qty × SL_dist
```

Ejemplo (equity 100 000, precio 100, ATR 1-min 0.15): `d = 0.30`, `qty_riesgo = 1 666`, tope por símbolo
`= 100` → `qty = 100`, `max_loss = 30` (0.03 % del equity). Con barras de 1 minuto el tope de exposición
por símbolo suele ser el límite activo; el detalle del check `position_size` indica cuál fue.

Métodos de sizing: `fixed_risk` implementado. `fixed_fraction`, `volatility_adjusted`, `risk_parity` y
`fractional_kelly` están previstos en la interfaz `PositionSizer`; **Kelly está desactivado por defecto** y el
sistema se niega a arrancar si se selecciona sin `kelly_enabled: true` explícito.

## 5. Límites por defecto (`config/default.yaml`)

```yaml
risk:
  max_risk_per_trade: 0.005
  max_daily_loss: 0.02
  max_total_exposure: 0.25
  max_open_positions: 5
  max_symbol_exposure: 0.10
  max_drawdown: 0.10
  allow_short: true
  atr_stop_multiple: 2.0
  take_profit_rr: 1.5
  min_risk_reward: 1.0
  min_stop_bps: 10
  max_stop_bps: 300
  no_entry_first_minutes: 5
  no_entry_last_minutes: 20
  buying_power_usage: 0.95
  sizing: {method: fixed_risk, kelly_enabled: false}
```

## 6. Kill switch (`packages/risk/kill_switch.py`)

| Disparador | Detección | Umbral por defecto |
|---|---|---|
| `DAILY_LOSS` | check `daily_loss_limit` | `max_daily_loss` |
| `MAX_DRAWDOWN` | check `max_drawdown` | `max_drawdown` |
| `STALE_DATA` | mercado abierto sin datos | `kill_switch.stale_data_seconds` (180 s) |
| `BROKER_DISCONNECTED` | health `broker_connected` fallando | `broker_disconnect_seconds` (60 s) |
| `DATABASE_UNAVAILABLE` | fallo de escritura/barrera de auditoría | inmediato |
| `ABNORMAL_SLIPPAGE` | slippage medio de entradas | `max_avg_slippage_bps` (25) sobre 20 fills (mín. 5) |
| `ABNORMAL_LATENCY` | latencia de decisión | `max_decision_latency_ms` (5 000) |
| `MODEL_UNAVAILABLE` | errores consecutivos del modelo | `max_consecutive_model_errors` (3) |
| `RISK_ENGINE_UNAVAILABLE` | excepción en el Risk Engine | inmediato |
| `UNEXPECTED_POSITION` | reconciliación: posición no explicada por órdenes propias | inmediato |
| `UNEXPECTED_ORDER` | reconciliación / eventos de órdenes desconocidas | inmediato |
| `MANUAL` | CLI (`kill-switch engage`) / API (Fase 5) | — |

Efectos:

1. Bloqueo **inmediato** de nuevas entradas (check `kill_switch`).
2. Cancelación de entradas pendientes (`cancel_entries_on_kill: true`). Las piernas de protección (SL/TP) se
   mantienen: el kill switch no deja posiciones sin stop.
3. Cierre de posiciones **solo** si `flatten_on_kill: true` (por defecto `false`: aplanar es una decisión humana).
4. Estado persistido en `system_state` + evento en `risk_events`; sobrevive a reinicios.
5. El reset es **solo manual** y exige identificar a la persona (`kill-switch reset --by <nombre> --reason <texto>`).

## 7. Pausa vs kill switch

| | Pausa | Kill switch |
|---|---|---|
| Origen | humano | sistema o humano |
| Efecto | no abrir entradas nuevas | no abrir entradas + cancelar entradas pendientes (+ aplanar si se configura) |
| Reanudar | `resume` | reset manual con identificación y motivo |

## 8. PnL diario y drawdown

- `daily_pnl = equity − last_equity`, donde `last_equity` es la equity del broker al cierre de la sesión anterior
  (misma semántica que Alpaca). Incluye PnL realizado y no realizado.
- `drawdown = 1 − equity / peak_equity`, con `peak_equity` el máximo observado desde el inicio de la ejecución
  (en fases posteriores se persistirá entre ejecuciones).

## 9. Restricciones del broker que el riesgo respeta

Poder de compra, *shortability* / *easy-to-borrow*, cantidad mínima e incremento, tick, soporte de bracket y de
fraccionales (`BrokerCapabilities` + `InstrumentInfo`). Reglas regulatorias: la de *pattern day trader* fue
eliminada (efectiva el 04/06/2026, sustituida por margen intradía según exposición); la SSR (SEC Rule 201) y el
coste de préstamo de cortos se incorporarán con el adapter de Alpaca (Fase 4). Verificar siempre lo vigente.

## 10. Lo que el Risk Engine nunca hace

- Modificar la dirección o las probabilidades de JEV.
- Aprobar sin stop loss.
- Aprobar con datos `STALE`/`INVALID`, con el kill switch activo o con la base de datos caída.
- Relajar límites automáticamente.
