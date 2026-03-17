# Discovery Workflow

Use these scripts before building extractor SQL for the mirror.

## Goal

Answer these questions with evidence:

- do the target mirror tables exist exactly as expected?
- what are their real natural keys?
- which columns can drive incremental sync?
- are there related stored procedures with hidden business filters?

## Scripts

### Broad inventory

Run:

```powershell
python scripts/run_discovery.py
```

Outputs:

- full schemas, tables, views, PKs, FKs, columns, procedures
- targeted mirror table inventory
- targeted mirror column inventory
- change-tracking candidate columns
- mirror-related procedure list
- target gap report

### Quick mirror target check

Run:

```powershell
python scripts/run_target_gap_analysis.py
```

This is the fastest way to confirm whether the first mirror slice exists under the names we expect.

## What to review first

1. `*_mirror_target_gap_report.csv`
2. `*_mirror_target_columns.csv`
3. `*_mirror_change_tracking_candidates.csv`
4. `*_mirror_related_procedures.csv`

## How we use the outputs

- If the table names match: write direct extractor SQL against those tables.
- If names differ: map the real ERP names before building extractors.
- If update/change columns exist: use incremental polling.
- If not: design rolling windows or replace-style sync for that family.
- If procedures contain important filters: preserve those rules in Postgres views or extractor SQL.
