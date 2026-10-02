# JEV con TypeSafe Jev (adaptador opcional)

> **Estado:** implementado y **desactivado por defecto**. El modelo por defecto sigue siendo `jev-heuristic`.
> No se atribuye ninguna ventaja a Jev para operar: es un candidato que debe validarse en la Fase 3.

## Qué es Jev

Jev es el primer modelo "System One" de TypeSafe AI (early access desde el 15/09/2026). No genera texto: recibe un
*estado* (texto o JSON) y preguntas con respuestas declaradas de antemano (`choice`, `score`, `noul`) y devuelve la
respuesta elegida con probabilidades calibradas y una confianza. TypeSafe documenta como puntos débiles la
aritmética, el conteo, las fechas y los estados grandes o ruidosos.

## Cómo lo usa la plataforma

`packages/jev/typesafe.py` (`TypeSafeJEVModel`) cumple el contrato `JEVModel`; el resto del pipeline no cambia.

| Paso | Detalle |
|---|---|
| Estado enviado | JSON compacto: símbolo, instante, timeframe, horizonte y 16 features redondeadas (NaN → `null`). Nunca datos de cuenta, posiciones ni broker. |
| Pregunta | Una `choice` `direction` con etiquetas `up` / `down` / `flat` sobre el horizonte. |
| Mapeo | `probability_up` y `probability_down` vienen de Jev; `confidence` también. `flat` dominante → `NO_TRADE`. |
| Lo que NO se le pide | Volatilidad y retorno esperados: salen de la volatilidad realizada, igual que en el sustituto heurístico. |
| Después | Signal Engine (valor esperado neto de costes), Risk Engine y ejecución, sin cambios. |
| Fallos | Error de la API, timeout, modelo distinto al fijado o etiquetas inesperadas → `ModelError`; 3 seguidos → kill switch `MODEL_UNAVAILABLE`. |

### Reproducibilidad

- **Modelo fijado** (`api_model`, por defecto `jev-1.13.0`). Si la API responde con otro modelo, la llamada falla
  en lugar de mezclar versiones.
- **Caché de respuestas** (`data/typesafe_jev_cache.jsonl`, JSONL append-only, clave = hash de modelo + estado +
  pregunta). Una respuesta grabada se reutiliza: los backtests se repiten idénticos y sin coste.
- **`verify` es offline**: reconstruye las decisiones desde la caché y nunca llama a la API.
- La identidad del modelo (`model_versions.params`) incluye `api_model`, `prompt_version` y umbrales; las opciones
  de ejecución (caché, timeout, reintentos, precio) no.
- Si cambias el estado o la pregunta, sube `prompt_version` en el código y la `version` del modelo en el perfil.

## Paso a paso

1. Crear una cuenta en `console.typesafe.ai` (Google o código por email) y generar una API key.
2. Guardarla **solo** en `.env`: `TYPESAFE_API_KEY=...` (nombre que lee el SDK oficial; los logs la ocultan).
3. Instalar el SDK oficial: `pip install -e ".[typesafe]"` (`typesafe-sdk` 0.7.x).
4. Probar la integración con una llamada real:
   `python -m apps.trading_engine --config config/profiles/typesafe-jev.yaml jev-check`
   Muestra los alias de modelo de tu cuenta (`jev-latest`, `jev-preview`; las versiones fijas como `jev-1.13.0` no
   se listan pero se aceptan), la decisión, la latencia y los tokens. Si la API respondiera con otro modelo, la
   llamada falla en lugar de mezclar versiones.
5. Simular: `python -m apps.trading_engine --config config/profiles/typesafe-jev.yaml simulate --start 2024-03-04`.
   El informe incluye llamadas a la API, aciertos de caché, tokens y coste estimado.
6. Verificar: `python -m apps.trading_engine --config config/profiles/typesafe-jev.yaml verify --run-id <run_id>`.

## Coste

Precio publicado al lanzamiento: USD 0,042 por millón de tokens de entrada; salida gratuita. Verifícalo antes de
fiarte de los informes (`usd_per_million_input_tokens` en el perfil). Medido en la primera llamada real
(02/10/2026): **743 tokens por decisión, ~USD 0,000031**, latencia ~330 ms.

| Escenario | Decisiones | Coste aproximado |
|---|---|---|
| 1 día, 3 símbolos, barras de 1 min | ~1.170 | ~USD 0,04 |
| 1 mes, 3 símbolos | ~25.000 | ~USD 0,80 |
| Backtest de 1 año, 3 símbolos | ~295.000 | ~USD 9 |

Con USD 5 de créditos caben unas 160.000 decisiones (~135 días de 3 símbolos en barras de 1 minuto).

Las re-ejecuciones sobre la misma caché no cuestan nada.

## Limitaciones conocidas

- **Llamada bloqueante**: el SDK síncrono se llama dentro del pipeline. Basta para backtests; el runner en tiempo
  real de la Fase 4 deberá ejecutarlo en un hilo (`asyncio.to_thread`). Timeout por defecto 3 s y sin
  reintentos, por debajo del límite de latencia del kill switch (5 s).
- **Sin datos reales todavía**: sobre el mercado sintético el resultado no dice nada; la evaluación seria es
  walk-forward con históricos y después de costes (Fase 3).
- **Probado en vivo** el 02/10/2026 con `jev-check` (Windows, Python 3.14): clave, modelo fijado, latencia y
  coste confirmados. Los tests automáticos no llaman a la API (transporte simulado contra el SDK oficial 0.7.2).
