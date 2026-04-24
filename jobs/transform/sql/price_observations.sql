-- Build staged.price_observations for a given ingestion_date.
--
-- Parameter:
--   @ingestion_date  DATE  — the landed partition to transform
--
-- Substitutions (applied by main.py before submission):
--   {project}  — GCP project id
--   {staged}   — staged dataset name (default: staged)
--   {landed}   — landed dataset name (default: landed_raw)
--
-- Idempotency: the caller sets destination=price_observations${{YYYYMMDD}} with
-- WRITE_TRUNCATE, so re-runs overwrite only the target partition.

WITH landed AS (
  SELECT
    ingestion_date,
    loaded_at,
    source_file,
    retailer_eik,
    settlement_code   AS settlement_code_raw,
    store_name,
    product_name,
    product_code,
    category_code,
    retail_price_raw,
    promo_price_raw,
    raw_row_hash
  FROM `{project}.{landed}.kolkostruva_daily`
  WHERE ingestion_date = @ingestion_date
    -- Drop CSV-header-as-data garbage rows.
    AND (settlement_code IS NULL OR settlement_code != 'Населено място')
    AND (category_code IS NULL OR category_code != 'Категория')
),

normalized AS (
  SELECT
    *,
    -- settlement_code normalization:
    --   1. Strip the district suffix: "68134-01" -> "68134"
    --   2. LPAD to 5 chars, then take the last 5 — handles both short codes
    --      (e.g. "7079" -> "07079") and extra leading zero ("068134" -> "68134").
    --   3. NULL if the raw value is empty.
    CASE
      WHEN TRIM(COALESCE(settlement_code_raw, '')) = '' THEN NULL
      ELSE RIGHT(LPAD(SPLIT(TRIM(settlement_code_raw), '-')[OFFSET(0)], 5, '0'), 5)
    END AS settlement_code_normalized,

    -- price normalization: comma-decimal -> dot-decimal, then SAFE_CAST to NUMERIC.
    -- NULLIF on promo_price_raw so empty string -> NULL (no-promo) rather than 0.
    SAFE_CAST(REPLACE(retail_price_raw, ',', '.') AS NUMERIC)        AS retail_price,
    SAFE_CAST(REPLACE(NULLIF(promo_price_raw, ''), ',', '.') AS NUMERIC) AS promo_price
  FROM landed
),

-- Apply per-retailer settlement-code overrides (e.g. Alex Fish's "06813" -> "68134").
-- Overrides are keyed by (retailer_eik, post-normalization settlement_code) so one
-- entry handles both "6813" and "06813" forms.
overridden AS (
  SELECT
    n.*,
    COALESCE(o.corrected_ekatte_code, n.settlement_code_normalized) AS settlement_code
  FROM normalized n
  LEFT JOIN `{project}.{staged}.settlement_overrides` o
    ON n.retailer_eik = o.retailer_eik
   AND n.settlement_code_normalized = o.raw_settlement_code
),

enriched AS (
  SELECT
    n.ingestion_date,
    n.loaded_at,
    n.retailer_eik,
    r.retailer_brand,
    r.retailer_type,
    r.is_primary,

    -- Stable IDs. Use TO_HEX(SHA256(...)) with field separator \x1f (unit separator)
    -- so concatenation can't collide via overlapping content.
    TO_HEX(SHA256(CONCAT(
      n.retailer_eik, '\x1f',
      COALESCE(n.product_code, '')
    ))) AS retailer_sku_id,
    TO_HEX(SHA256(CONCAT(
      n.retailer_eik, '\x1f',
      COALESCE(n.settlement_code, ''), '\x1f',
      COALESCE(n.store_name, '')
    ))) AS store_id,

    n.store_name,
    n.settlement_code,
    e.settlement_name_bg,
    e.oblast_code,
    e.oblast_name_bg,

    n.category_code,
    c.category_name_bg,
    c.is_grocery,

    n.product_name,
    n.product_code,
    n.retail_price,
    n.promo_price,
    CASE
      WHEN n.promo_price IS NOT NULL
       AND n.promo_price > 0
       AND n.retail_price IS NOT NULL
       AND n.promo_price < n.retail_price
      THEN TRUE ELSE FALSE
    END AS has_promo,
    CASE
      WHEN n.promo_price IS NOT NULL
       AND n.promo_price > 0
       AND n.retail_price IS NOT NULL
       AND n.retail_price > 0
       AND n.promo_price < n.retail_price
      THEN ROUND((n.retail_price - n.promo_price) / n.retail_price, 4)
      ELSE NULL
    END AS discount_pct,

    n.source_file,
    n.raw_row_hash
  FROM overridden n
  LEFT JOIN `{project}.{staged}.retailer_dim` r
    ON n.retailer_eik = r.retailer_eik
  LEFT JOIN `{project}.{staged}.category_dim` c
    ON n.category_code = c.category_code
  LEFT JOIN `{project}.{staged}.ekatte_dim` e
    ON n.settlement_code = e.ekatte_code
)

SELECT
  ingestion_date AS observation_date,
  loaded_at,
  retailer_eik,
  retailer_brand,
  retailer_type,
  is_primary,
  retailer_sku_id,
  store_id,
  store_name,
  settlement_code,
  settlement_name_bg,
  oblast_code,
  oblast_name_bg,
  category_code,
  category_name_bg,
  is_grocery,
  product_name,
  product_code,
  retail_price,
  promo_price,
  has_promo,
  discount_pct,
  source_file,
  raw_row_hash
FROM enriched
-- Dedup: the same raw_row_hash shouldn't appear twice in a partition, but guard against it.
QUALIFY ROW_NUMBER() OVER (PARTITION BY raw_row_hash ORDER BY loaded_at DESC) = 1
