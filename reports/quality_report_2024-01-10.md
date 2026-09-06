# Data quality report - batch 2024-01-10
- Generated: `2026-09-04T06:26:04+00:00`
- Warehouse: `duckdb`
- Batch id: `backfill-2024-01-10`
- Pipeline runtime: `8.6s`
## Row counts by stage
| Stage | Rows |
| --- | ---: |
| 1_landed | 30,470 |
| 2_staged | 30,470 |
| 3_deduped | 30,470 |
| 4_fact | 30,470 |
| 5_agency_dim | 13 |
| 5_daily_mart | 189 |
## dbt tests
- Tests run: **44**
- Passed: **43** (97.7%)
- Failed: 0 | Warned: 1 | Errored: 0
- dbt elapsed: 0.312s
| Test | Status | Failing rows |
| --- | --- | ---: |
| nyc311 | warn | 1 |
## AI-assisted schema mapping
- Source systems mapped: **4**
- Column decisions: **83**
- Auto-match rate (excludes human overrides): **75.0%**
- Fallback rate (share of mapped columns resolved by fuzzy, not the LLM): **27.0%**
- LLM used: **False** - fallback path only
- By tier: `override`=3, `exact`=43, `llm`=0, `fuzzy`=17, `unmapped`=20
- LLM errors (fell back): `TypeError: "Could not resolve authentication method. Expected one of api_key, auth_token, or credentials to be set. Or for one of the `X-Api-Key` or `Authorizat`
- Human overrides applied: 3
### Mapping confidence distribution
| Bucket | Columns |
| --- | ---: |
| 1.00 (exact/override) | 46 |
| 0.90-0.99 | 15 |
| 0.82-0.89 | 2 |
| below threshold (unmapped) | 20 |
