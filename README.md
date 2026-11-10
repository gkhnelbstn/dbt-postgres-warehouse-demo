# dbt-postgres-warehouse-demo

Logistics data warehouse on PostgreSQL built with dbt: staging, intermediate and marts on synthetic data, with tests, docs and CI.

**Status: planned.** This repo is a work in progress; the checklist lives in the epic issue and on the [Portfolio build-out](https://github.com/users/gkhnelbstn/projects/6) board.

## Plan

- What: a small logistics warehouse on PostgreSQL built with dbt. Synthetic source data (trips, vehicles, customers, invoices, fuel), staging -> intermediate -> marts, dimensional model.
- Must have: at least 20 models, incremental model for trips, tests on every primary key and relationship, sources with freshness, dbt docs published to GitHub Pages, GitHub Actions running `dbt build` on a Postgres service container.
- Done when: a stranger can run `docker compose up` then `dbt build` and see all tests pass; README shows the lineage graph screenshot.
- Upwork angle: "I will build your dbt project like this one."
