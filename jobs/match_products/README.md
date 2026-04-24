# match_products

The product-matching pipeline — extract structured attributes from retailer
product names, form canonical match groups across retailers, and feed a
`prod.canonical_products` catalog. This is the moat of spesto.

## Status: **phase 1 (spike)**

Before committing anything to schema, we're validating the approach on one
category (category 6 — fresh milk, ~30 SKUs) with full human eyeballing.

Only one script exists: [`spike_category_6.py`](spike_category_6.py). It does
not write to BigQuery — it emits a summary and a JSON file for review.

## Approach

The matching algorithm reduces to "extract well":

1. **Extract** structured attributes from each retailer's `product_name` via
   Claude Opus 4.7: `{brand, product_type, size_value, size_unit, fat_pct, variant}`.
2. **Normalize** the attributes (lowercase brand, unit conversion to g/ml, round
   fat to 0.1%).
3. **Match key** = `sha256(brand|product_type|size_g_or_ml|fat|variant)`.
4. Two SKUs with the same match key are proposed as the same canonical product.

This trades model-call cost for simplicity: matching is deterministic once
extraction is done, and extraction is per-unique-SKU (cached across days,
typically ~100 new/changed SKUs per day at scale).

## Why Claude Opus 4.7 (not Sonnet, not Gemini)

- **Bulgarian-language quality** — consistent brand extraction across Cyrillic
  variants ("ВЕРЕЯ" / "Верея") and size/fat normalization in mixed-decimal
  writing ("3,5%" vs "3.5%") is where the model earns its cost.
- **Consistency across retailers** — the load-bearing property is that two
  retailers' different wording for the same product extract to *identical*
  attributes. A cheaper model that's 95% consistent isn't good enough; one
  extraction mistake per product fragments the canonical catalog.
- **Cost is negligible at v1 scale** — ~15k SKUs × ~500 tokens × Opus 4.7 with
  prompt caching ≈ $50 one-time to seed the catalog; ~$0.30/day incremental
  afterwards. Matching quality is worth the premium.

## Why `client.messages.parse()` with Pydantic

`messages.parse()` (vs `messages.create()` with forced tool use) gives us:
- Pydantic schema as source of truth — field descriptions become part of the
  prompt automatically.
- Validated output — `response.parsed_output` is either a validated
  `ProductAttributes` instance or `None` (in which case we log + skip the SKU
  rather than ingesting a malformed extraction).
- No manual JSON parsing of tool-use blocks.

## What we're validating in the spike

1. **Groups form correctly.** SKUs that are obviously the same product (e.g.
   "Верея 3.5% 1л" from 5 different retailers) should all land in one group.
2. **Singletons are actually unique.** A SKU that doesn't match any other
   should be because it's genuinely unique (one retailer has this specific
   product), not because extraction produced a different brand or size on one
   row.
3. **Low-confidence extractions are actually hard cases.** Claude's
   self-reported confidence should correlate with SKUs where the name is
   genuinely ambiguous.
4. **Cost and latency are what we expect.** ~30 SKUs × Opus 4.7 should finish
   in 1-2 minutes and cost under $0.50.

If all four hold, phase 2 is scaling to the remaining 76 grocery categories
(writing extracted attributes to `staged.retailer_skus`). If any fail, we
iterate on the system prompt and few-shot examples before scaling.

## Running

You need an Anthropic API key and the usual GCP ADC setup (for BigQuery):

```bash
export ANTHROPIC_API_KEY=sk-ant-...
# (ADC already set up — same as the transform job uses)

cd jobs/match_products
uv sync
uv run python spike_category_6.py --date 2026-04-22
```

Results go to stdout and `/tmp/spike_category_6.json`.

### Useful flags

- `--date YYYY-MM-DD` — defaults to yesterday BG time
- `--output PATH` — where to write the JSON
- `--limit N` — process only the first N SKUs (debug)

## Reading the output

stdout has three sections in order:

1. **SUMMARY** — total SKUs, how many extracted, low-confidence count, groups
   formed, singleton count.
2. **GROUPS** (largest first) — each group shows the match_key and every SKU
   that rolled into it. These are the proposed canonical products.
3. **SINGLETONS** — SKUs that didn't match anyone else. Scan for ones that
   *should* have matched (extraction bug) vs ones that genuinely are unique.
4. **LOW-CONFIDENCE EXTRACTIONS** — raw attrs plus Claude's confidence score
   for anything under 0.8. Eyeball these for correctness.

The JSON file has the same data in programmatic form plus the full `attrs`
dict per SKU.

## If the spike looks good

Phase 2 plan:

1. Add `staged.retailer_skus` table with extracted attributes + embedding
   (for future nearest-neighbor candidate retrieval on new SKUs).
2. Per-category batch extraction job — same prompt, run once per new/changed
   `retailer_sku_id` across all 77 grocery categories.
3. Match-key materialization in `staged.retailer_sku_matches`.
4. Match-review UI as an admin route in `apps/web/` for confidence-middle-band
   cases.
5. `prod.canonical_products` + `prod.price_facts_daily` built on top.

## If the spike looks bad

Likely failure modes and fixes:

- **Inconsistent brand extraction across retailers** — add explicit brand
  examples to the system prompt. Seen variants: "ВЕРЕЯ" / "Верея" / "Vereya".
- **UHT vs fresh mistakenly treated as separate variants** — the prompt
  explicitly rules this out; if it still happens, add a few-shot counter-example.
- **Decimal separator confusion** — Bulgarian uses "," as decimal (`3,5%`);
  the prompt notes this, but verify Claude handles it.
- **Generic / descriptive names with no brand** — these should return
  `brand: "unknown"` with lower confidence. If Claude is inventing brands from
  descriptors, tighten the prompt.
