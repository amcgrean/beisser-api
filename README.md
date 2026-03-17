# agility-api

Standalone Raspberry Pi sync service for mirroring Agility SQL Server into normalized Postgres tables.

## Scope

- reads Agility SQL Server locally on the Pi
- writes normalized ERP mirror tables into Postgres/Supabase
- records worker heartbeat, per-table sync state, and batch metrics
- does not contain WH-Tracker or ToolBx app logic

## Initial layout

- `agility_api/config.py`
- `agility_api/database.py`
- `agility_api/models.py`
- `agility_api/mirror_framework.py`
- `agility_api/worker.py`
- `docs/CENTRAL_AGILITY_MIRROR_CUTOVER.md`

## Next run steps

1. Copy `.env.example` to `.env`
2. Fill in SQL Server and Postgres credentials
3. Install dependencies with `pip install -r requirements.txt`
4. Wire migrations/bootstrap schema creation
5. Run the worker on the Pi
