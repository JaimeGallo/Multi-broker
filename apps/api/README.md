# apps/api (Fase 5)

FastAPI + WebSockets sobre la base de datos de auditoría y el bus de eventos (vía Redis).
Solo lectura y controles manuales (pausar, kill switch) protegidos por `JEV_API_TOKEN`.
Nunca expondrá credenciales de brokers. Aún no hay código.
