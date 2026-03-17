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
- `agility_api/sqlserver.py`
- `agility_api/discovery_queries.py`
- `agility_api/discovery_runner.py`
- `agility_api/first_slice_plan.py`
- `agility_api/models.py`
- `agility_api/mirror_framework.py`
- `agility_api/sync_definitions.py`
- `agility_api/runtime_sync.py`
- `agility_api/worker.py`
- `scripts/bootstrap_schema.py`
- `scripts/run_discovery.py`
- `scripts/run_target_gap_analysis.py`
- `scripts/verify_counts.py`
- `scripts/verify_samples.py`
- `docs/CENTRAL_AGILITY_MIRROR_CUTOVER.md`
- `docs/FIRST_SLICE_DISCOVERY_FINDINGS.md`

## Next run steps

1. Copy `.env.example` to `.env`
2. Fill in SQL Server and Postgres credentials
3. Install dependencies with `pip install -r requirements.txt`
4. Run discovery with `python scripts/run_discovery.py`
5. Review `docs/DISCOVERY_WORKFLOW.md`
6. Bootstrap Postgres tables with `python scripts/bootstrap_schema.py`
7. Run one sync cycle with `python -m agility_api.worker --bootstrap --once`
8. Validate with `python scripts/verify_counts.py`
9. Move the first-slice runtime to the Pi
