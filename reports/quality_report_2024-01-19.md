# Data quality report - batch 2024-01-19
- Generated: `2026-09-06T04:07:42+00:00`
- Warehouse: `duckdb`
- Batch id: `final-verify`
- Pipeline runtime: `12.13s`
## Row counts by stage
| Stage | Rows |
| --- | ---: |
| 1_landed | 133,993 |
| 2_staged | 133,993 |
| 3_deduped | 133,993 |
| 4_fact | 133,993 |
| 5_agency_dim | 14 |
| 5_daily_mart | 854 |
## dbt tests
- Tests run: **44**
- Passed: **43** (97.7%)
- Failed: 0 | Warned: 1 | Errored: 0
- dbt elapsed: 0.338s
| Test | Status | Failing rows |
| --- | --- | ---: |
| assert_timestamp_anomaly_rate | warn | 1 |
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
| 1.00 (exact/override) | 92 |
| 0.90-0.99 | 30 |
| 0.82-0.89 | 4 |
| below threshold (unmapped) | 40 |
