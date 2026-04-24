# sql/staged/dims/

Dimension tables for the staged layer. All are version-controlled CSVs loaded into
BigQuery by `jobs/transform/` before the fact table rebuild each day.

Treating dims as code (rather than hand-editing in the BQ console) means every change
lands via PR, is diffable, and can be replayed. Small scale is what makes this
comfortable: ~200 rows (retailers), ~85 (categories), ~5500 (EKATTE settlements).

## Tables

### `retailer_dim.csv`

Groups the ~209 per-day EIK filings into retailer *brands* and tags each with a type.
A single brand (e.g. `Аптеки Марешки`) can span 30+ EIKs; `retailer_brand` is the
display key the consumer UI uses, `retailer_eik` is what joins to landed data.

| Column           | Type                                                                                                                 |
| ---------------- | -------------------------------------------------------------------------------------------------------------------- |
| `retailer_eik`   | STRING REQUIRED — legal entity EIK, matches `landed_raw.kolkostruva_daily.retailer_eik`                              |
| `retailer_brand` | STRING REQUIRED — consumer-facing brand name                                                                         |
| `retailer_type`  | STRING REQUIRED — enum: `grocery` · `pharmacy` · `drogerie` · `specialty` · `duty_free` · `online` · `alcohol_tobacco` · `other` |
| `is_primary`     | BOOL REQUIRED — `true` for top-tier consumer chains in the v1 product scope (see below)                              |
| `notes`          | STRING — free-text for uncertain classifications or grouping decisions                                               |

**`is_primary` — v1 scope filter.** Flags the top consumer-facing supermarket
chains that show up in the spesto.bg UI and drive product matching. All other
retailers are ingested and typed in the data warehouse (so they're available for
future B2B / analytics work) but don't reach the consumer experience. Current
primary list (7 brands, 8 EIKs — Fantastiko has two):

- Билла (130007884)
- Кауфланд (131129282)
- Лидл (131071587)
- Фантастико (206255903 main, 831556063 secondary)
- T Market (131324923)
- Минимарт (207064349)
- Метро (121644736)

To change v1 scope: flip the `is_primary` cell and re-run `jobs/transform/` with
`--upload-dims`. No data reprocessing needed because `staged.price_observations`
refreshes the full partition on every run.

**Type semantics:**

- `grocery` — supermarkets, hypers, convenience, cash-and-carry, specialty food (fish, meat, Italian). **This is v1 scope for the consumer UI.**
- `pharmacy` — licensed pharmacies (Аптеки …).
- `drogerie` — health & beauty with some food (ДМ, Lilly, Вис Виталис).
- `specialty` — single-category non-food (Douglas perfume, Jumbo toys, Mr.Bricolage hardware, DS Home).
- `duty_free` — airport / border duty-free.
- `online` — e-commerce retailers.
- `alcohol_tobacco` — alcohol and tobacco specialty.
- `other` — could not classify from the retailer name alone; review before use.

### `category_dim.csv`

Maps the shared `category_code` to human-readable names (Bulgarian + English) plus
an `is_grocery` flag. The `is_grocery` flag is the v1 filter for which rows reach
`canonical_products` matching.

Empirical range: codes 1–101 are real categories (1–77 food + alcohol, 78 cigarettes,
79–85 household / personal-care, 86–101 pharmacy). A handful of garbage codes (`-1`,
`1972047017`, `Категория`, empty) are kept in the dim with `is_grocery=false` so
joins on landed data remain complete.

| Column              | Type                                                     |
| ------------------- | -------------------------------------------------------- |
| `category_code`     | STRING REQUIRED — raw value from the feed (leave as STRING to handle the garbage codes) |
| `category_name_bg`  | STRING REQUIRED — consumer-facing name for the UI         |
| `category_name_en`  | STRING REQUIRED — internal English label                  |
| `is_grocery`        | BOOL REQUIRED — v1 scope filter                           |
| `notes`             | STRING — flagging, subcategory hints                      |

Grocery coverage at current retailer mix: **~61% of row volume** (946k of 1.54M
daily rows). Categories 79–85 (personal-care and household) are defaulted to
`is_grocery=false` but flipping them to `true` widens scope to supermarket staples
like toothpaste and toilet paper — that's a product call, not a data call.

### `ekatte_dim.csv`

Bulgarian NSI settlement classifier — 5,256 rows covering every populated place in
Bulgaria (4,997 villages + 257 cities + 2 monasteries across 28 oblasts). Sourced
from the live NSI NRNM JSON endpoint:

```
https://nrnm.nsi.bg/ekatte/territorial-units/json
```

Re-seeding: infrequent (NSI updates a few codes per year). When you re-fetch, the
transform logic lives in the one-shot script at the top of this file's git history
— feed the JSON through the same mapping (`kind` 1/3/7 → град/село/манастир,
strip "обл. " prefix from oblast names).

| Column                | Type                                                   |
| --------------------- | ------------------------------------------------------ |
| `ekatte_code`         | STRING REQUIRED — 5-digit, zero-padded                 |
| `settlement_name_bg`  | STRING REQUIRED — Bulgarian name (e.g. София, Пловдив) |
| `settlement_name_en`  | STRING REQUIRED — Latin transliteration (Sofia, Plovdiv) |
| `settlement_type`     | STRING REQUIRED — `град` \| `село` \| `манастир`        |
| `oblast_code`         | STRING REQUIRED — 3-letter NSI code (SOF, PDV, VAR, …) |
| `oblast_name_bg`      | STRING REQUIRED — clean Bulgarian (no "обл. " prefix)  |
| `obshtina_name_bg`    | STRING REQUIRED — municipality name                    |
| `nuts3_code`          | STRING REQUIRED — EU NUTS3 region (BG411, BG412, …)    |

**Two Sofia oblasts exist and are different things** — `SOF` = **София (столица)**,
the city of Sofia itself (obshtina Столична, 38 settlements including the capital
and its adjacent villages). `SFO` = **София**, the surrounding Sofia Region (284
settlements). Easy to confuse if you're building city-level filters.

**Normalization required at the staged layer** — the raw feed contains malformed
`settlement_code` values that need fixing before joining to this dim:

- Short codes (`7079`, `151`) — pad with leading zeros to 5 chars.
- Extra zero (`068134`) — strip back to 5 chars.
- Sofia district suffixes (`68134-01`, `68134-09`) — split on `-`, keep the base
  `68134`; the suffix is a retailer extension to EKATTE, not in the official dim.
- Garbage (`Населено място`) — the CSV-header-as-data leak, drop at staged.

Store-level lat/lng does **not** live here — it's a property of each retail store,
resolved by `jobs/resolve_stores/` into `staged.store_dim`.

### `settlement_overrides.csv`

Small lookup table for per-retailer settlement-code corrections. Some retailers
emit codes that aren't valid NSI EKATTE (e.g. Alex Fish, EIK `831314284`, sends
`06813` for stores we've confirmed are in Sofia = `68134`). The staged transform
joins on this table before `ekatte_dim` so the bad codes get rewritten to the
canonical ones.

Keyed on `(retailer_eik, raw_settlement_code)` — where `raw_settlement_code` is
the value **after** our standard normalization (LPAD to 5, suffix strip, leading-
zero trim). So one override entry handles both `6813` and `06813` forms.

| Column                   | Type                                   |
| ------------------------ | -------------------------------------- |
| `retailer_eik`           | STRING REQUIRED                        |
| `raw_settlement_code`    | STRING REQUIRED — post-normalization   |
| `corrected_ekatte_code`  | STRING REQUIRED — must exist in `ekatte_dim` |
| `reason`                 | STRING — document why, with source     |

Add a new row whenever the staged dim-miss health-check query surfaces a
retailer-specific code, and you've confirmed the real settlement.

## Opening the CSVs

Open in VS Code or any plain-text editor. **Do not open in Excel** — it strips
leading zeros from `retailer_eik` values like `000004283` (КООП) and `040492787`
(Пламко), which breaks joins to landed data.

## Review protocol

Each CSV commit should come with a brief PR description noting what changed and why.
For `retailer_dim.csv` specifically, spot-check new rows against the real retailer
name / URL before merging — the CSV is the source of truth for how retailers appear
in the consumer UI.
