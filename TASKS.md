# Task List

## Foundation (Completed)
- [x] Planning and Design
    - [x] Create implementation plan <!-- id: 0 -->
    - [x] Review plan with user <!-- id: 1 -->
- [x] Framework Core Setup
    - [x] Set up project structure (config, utils, clients) <!-- id: 2 -->
    - [x] Implement configuration loading (YAML + .env) <!-- id: 3 -->
- [x] GCP Integration
    - [x] Implement BigQuery client wrapper <!-- id: 4 -->
    - [x] Implement Job Triggers (Dataflow, Composer stub) <!-- id: 5 -->
- [x] Validation Logic
    - [x] Create reusable validation assertions (row count, schema, data checks) <!-- id: 6 -->
- [x] Test Implementation
    - [x] specific test cases implementation <!-- id: 7 -->
- [x] Verification
    - [x] Verify with sample data/mocks <!-- id: 8 -->
- [x] Infrastructure & Tooling
    - [x] Migrate to uv for dependency management <!-- id: 9 -->
    - [x] Add GitHub Actions CI workflow <!-- id: 10 -->
    - [x] Verify CI locally (simulated) or via review <!-- id: 11 -->
- [x] Refactoring & Cleanup
    - [x] Remove pydantic-settings and refactor config <!-- id: 12 -->
    - [x] Add Ruff and Black for linting/formatting <!-- id: 13 -->
    - [x] Update CI to include linting <!-- id: 14 -->
- [x] Environment Management
    - [x] Support loading .env.{env} files in settings.py <!-- id: 15 -->
    - [x] Create templates for dev, test, staging <!-- id: 16 -->
- [x] End-to-End Pipeline Test
    - [x] Implement multi-layer dataset configuration <!-- id: 17 -->
    - [x] Create test_e2e_pipeline.py reflecting user flow <!-- id: 18 -->
    - [x] Implement schema consistency assertion (Raw Vault vs Consumption) <!-- id: 19 -->
- [x] Data Seeding & CDC Support
    - [x] Create StorageClient for GCS operations (upload CSV) <!-- id: 50 -->
    - [x] Add support for "Initial" vs "CDC" data loading in tests <!-- id: 51 -->
- [x] Dispatch Job & Selective Testing
    - [x] Modify E2E test to support `TEST_STOP_AT_LAYER` env var <!-- id: 55 -->
    - [x] Support custom CSV filename via env var <!-- id: 56 -->
    - [x] Create GitHub Action `manual_trigger.yaml` with inputs <!-- id: 57 -->

## Phase 1: Advanced Dataflow Integration
- [ ] Implement Flex Template triggering support <!-- id: 20 -->
- [ ] Add robust job status polling with configurable timeouts <!-- id: 21 -->
- [ ] Implement job metric retrieval (e.g., counters for record counts) <!-- id: 22 -->
- [ ] Add support for job cancellation on test failure <!-- id: 23 -->

## Phase 2: Composer (Airflow) Integration
- [x] Implement `trigger_dag` using Airflow Stable REST API <!-- id: 30 -->
- [x] Add IAP (Identity-Aware Proxy) authentication support for private Composer <!-- id: 31 -->
- [ ] Implement DAG run status polling and log retrieval during failure <!-- id: 32 -->

## Phase 3: Advanced Data Quality (DQ) Assertions
- [ ] Implement `assert_column_unique(table, col)` <!-- id: 40 -->
- [ ] Implement `assert_column_not_null(table, col)` <!-- id: 41 -->
- [ ] Implement `assert_referential_integrity(table, col, parent_table, parent_col)` <!-- id: 42 -->
- [ ] Implement `assert_sql_returns_no_rows(query)` (for negative testing) <!-- id: 43 -->
- [ ] Implement logic to compare two tables (source vs target) <!-- id: 44 -->

## Phase 4: Test Data Management
- [x] Create `StorageClient` fixture to upload local CSV/JSON to bucket <!-- id: 50 -->
- [x] Implement automated cleanup/teardown of test tables (Unseed BQ) <!-- id: 51 -->
- [ ] Add support for "Randomized Dataset" per test run to ensure isolation <!-- id: 52 -->

## Phase 5: Reporting & Usability
- [x] Integrate granular BQ query costs into test report (Via Allure) <!-- id: 60 -->
- [x] Integrate Allure Reporting <!-- id: 63 -->
- [ ] Add CLI command to scaffold a new test file template <!-- id: 61 -->
- [ ] Expand documentation with API reference <!-- id: 62 -->
