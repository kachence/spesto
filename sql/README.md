# sql/

BigQuery SQL transformations, managed as version-controlled `.sql` files and run via
scheduled BigQuery queries (or Cloud Run jobs wrapping `bq query`).

Not yet populated. Intended layout once we start:

```
landed/      Schema definitions + load jobs for landed_raw.* (raw CSV → typed rows)
staged/      Cleaning, deduplication, normalization (→ staged.*)
prod/        Canonical products, price facts, dims (→ prod.*)
views/       Materialized daily aggregates, inflation indices, per-category stats
```

Deferred dbt adoption until model count justifies the DAG overhead (rule of thumb:
~50 models). For v1, `.sql` files run on a schedule are simpler and sufficient.
