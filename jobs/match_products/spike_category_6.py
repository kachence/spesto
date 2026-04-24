"""Spike: extract product attributes via Claude and form match groups for category 6 (fresh milk).

Approach
--------
For each unique retailer_sku_id in category 6 on the target date:
  1. Call Claude Opus 4.7 to extract `{brand, product_type, size, fat_pct, variant}`
     from the raw product_name using adaptive thinking + forced structured output.
  2. Compute a deterministic match_key from the normalized attributes.
  3. SKUs that produce the same match_key are proposed as one canonical product.

Outputs
-------
  * stdout  — summary + grouped listing for eyeballing
  * JSON    — /tmp/spike_category_6.json with the full extraction detail

No data is written to BigQuery. This is a pure-research step to validate prompt
quality and the "deterministic match key from Claude-extracted attributes"
approach before committing to schema in `staged.retailer_skus`.

Cost envelope
-------------
~30 SKUs in category 6 × Opus 4.7 with prompt caching ≈ $0.30-0.50 per run.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import anthropic
from google.cloud import bigquery
from pydantic import BaseModel, Field

SOFIA = ZoneInfo("Europe/Sofia")
log = logging.getLogger("spike")

CATEGORY_CODE = "6"
CATEGORY_NAME_BG = "Прясно мляко"
CATEGORY_SPEC = "Official: Прясно мляко от 2% до 3,6% 1 л"
MODEL = "claude-opus-4-7"


class ProductAttributes(BaseModel):
    """Structured extraction schema for a grocery product name."""

    brand: str = Field(
        description=(
            "Manufacturer brand (not retailer). Use proper Bulgarian casing as it "
            "appears ('Верея', 'Маджаров', 'Meggle'). For retailer private labels "
            "use the exact private-label brand ('BILLA', 'CLEVER', 'NN'). "
            "If no brand is identifiable, use 'unknown'."
        )
    )
    product_type: str = Field(
        description=(
            "Generic Bulgarian product type — for this category always 'прясно мляко' "
            "regardless of UHT/pasteurized/processing method. Don't include brand, "
            "size, or fat percentage."
        )
    )
    size_value: float = Field(description="Numeric size as stated in the name.")
    size_unit: Literal["g", "kg", "ml", "l"] = Field(
        description="Size unit as written in the name."
    )
    fat_pct: float | None = Field(
        default=None,
        description="Fat percentage (0-100) if stated in the name, null otherwise.",
    )
    variant: str | None = Field(
        default=None,
        description=(
            "Distinguishing variant that would separate this product from an "
            "otherwise-identical one: 'без лактоза' (lactose-free), 'био' (organic), "
            "'обогатено' (fortified). UHT vs fresh vs pasteurized is NOT a variant — "
            "both are 'прясно мляко'. Null if no variant applies."
        ),
    )
    confidence: float = Field(
        description="Extraction confidence, 0.0 to 1.0.",
        ge=0.0,
        le=1.0,
    )


SYSTEM_PROMPT = """\
You extract structured attributes from Bulgarian grocery product names for \
spesto.bg, a consumer price-comparison product. The extracted attributes group \
equivalent products across retailers so users can compare prices on the same item.

Consistency is the thing that matters most. If two retailers list the same \
physical product with different wording — "Верея 3.5% 1л" vs "ВЕРЕЯ 3.5% 1 Л \
БУТИЛКА" vs "Vereya Прясно мляко 3,5%" — your extraction must produce identical \
brand / product_type / size_value / size_unit / fat_pct / variant for all three, \
so they group together.

## Brand normalization (CRITICAL — read carefully)

Brand names appear in both Cyrillic and Latin in the Bulgarian retail feed, \
often for the same physical product. You MUST normalize to a single canonical \
form regardless of the source script.

**Rule: always output brand in LOWERCASE LATIN script.** If the source is \
Cyrillic, transliterate to Latin using standard Bulgarian BGN/PCGN romanization. \
If the source is already Latin, lowercase it.

**Bulgarian letter ъ transliterates to 'a'.** Always. Not 'ŭ', not 'u', not 'o'. \
Examples: Наборъ → nabor, Жълтуша → zhaltusha, Пършевица → parshevitsa, \
Добруджа → dobrudzha. This is a consistent convention for our product, even \
where BGN/PCGN would use a different character — we prefer ASCII-friendly \
match keys.

Examples you will see in this category — always use the right-hand form:

| If source says...              | Output brand as... |
|--------------------------------|--------------------|
| "Верея" or "Vereya" or "ВЕРЕЯ" | vereya             |
| "Маджаров" / "Madzharov" / "Димитър Маджаров" | madzharov |
| "Саяна" / "Sayana" / "САЯНА"   | sayana             |
| "Боженци" / "Bozhentsi" / "БОЖЕНЦИ" | bozhentsi     |
| "Булгарче" / "Bulgarche"       | bulgarche          |
| "Меггле" / "Meggle" / "MEGGLE" | meggle             |
| "Млековита" / "Mlekovita"      | mlekovita          |
| "Олимпус" / "Olympus"          | olympus            |
| "My Day" / "Май Дей" / "May Day" | my day           |
| "Milki Dream" / "Милки Дрийм"  | milki dream        |
| "Laciate" / "Лачиате" / "ЛАЧИАТЕ" | laciate         |
| "Polmlek"                      | polmlek            |
| "Balkan" / "Балкан"            | balkan             |
| "K-Free"                       | k-free             |
| "Пършевица" / "Pŭrshevitsa"    | parshevitsa        |
| "БОЖАНА" / "Bozhana"           | bozhana            |
| "Белииса" / "Beliisa"          | beliisa            |
| "Наборъ" / "Naborŭ"            | nabor              |
| "Жълтуша" / "Zhŭltusha"        | zhaltusha          |
| "Еdnо" / "Едно"                | edno               |
| "Грош" / "Grosh"               | grosh              |
| "Berti"                        | berti              |
| "Domlian" / "Домлян"           | domlian            |
| "ЕЛ БИ" / "ЕЛ БИ Булгарикум"   | el bi              |

Retailer private labels (treat like any other brand):

| If source says...      | Output brand as... |
|------------------------|--------------------|
| "BILLA"                | billa              |
| "CLEVER" (Billa PL)    | clever             |
| "NN" / "KLC" (Kaufland PL) | nn             |
| "Ф Вкус" / "F Vkus" (Fantastico PL) | f vkus |

When the raw name includes *both* a Cyrillic and a Latin form of the same brand \
(e.g., "Meggle Краве мляко UHT 3,2% 1л"), extract the brand once in canonical form.

If you see a brand not listed above, apply the same rule: transliterate Cyrillic \
to Latin, lowercase. Do not invent brands — if the string looks like "ЛАЧИАТЕ" \
and you're confident it's Cyrillic for Laciate, output "laciate". If you're NOT \
confident what Cyrillic spelling X transliterates to, drop confidence to 0.5 or \
below rather than guessing.

## product_type rules

product_type is always 'прясно мляко' for this category regardless of how it's \
written. UHT, pasteurized, "fresh", "UHT Краве мляко", "Краве мляко UHT", \
"пастьоризирано" — all 'прясно мляко'. The processing method is NOT a variant.

## Variant rules

Variants that DO separate products: \
'без лактоза' (lactose-free), 'био' (organic), 'обогатено' (fortified with \
vitamins), 'козе' (goat milk). Otherwise null.

"Свежо" (fresh), "краве" (cow), "пастьоризирано" (pasteurized), "UHT", \
"бутилка" (bottle), packaging descriptors — NOT variants.

## Size and fat rules

Size is almost always 1 L but can be 500 мл, 1.5 л, or 2 л. Fat percentages \
typically: 1.5, 1.7, 2, 2.5, 3, 3.2, 3.5, 3.6, 3.7. Decimal separator may be \
"." or ","; parse either as numeric.

## When to drop confidence

- No brand identifiable (name is purely descriptive like "Прясно мляко 3%") \
→ brand='unknown', confidence ≤ 0.5.
- Brand appears to be a compound or unusual (e.g. "Верея Чудно" is a sub-line \
of Верея — use 'vereya' as brand, reduce confidence to ~0.85).
- Cyrillic name you can't confidently transliterate → confidence ≤ 0.5.
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    default_date = (dt.datetime.now(SOFIA).date() - dt.timedelta(days=1)).isoformat()
    p.add_argument("--date", default=default_date, help="YYYY-MM-DD (defaults to yesterday BG time).")
    p.add_argument("--output", default="/tmp/spike_category_6.json")
    p.add_argument("--limit", type=int, help="Process only the first N SKUs (debug).")
    return p.parse_args()


def get_unique_skus(
    bq: bigquery.Client, project: str, date: dt.date
) -> list[dict]:
    sql = f"""
    SELECT
      retailer_sku_id,
      retailer_eik,
      retailer_brand,
      ANY_VALUE(product_name) AS product_name,
      COUNT(*) AS row_count
    FROM `{project}.staged.price_observations`
    WHERE observation_date = @obs_date
      AND category_code = @cat
      AND is_grocery
      AND is_primary
    GROUP BY retailer_sku_id, retailer_eik, retailer_brand
    ORDER BY row_count DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("obs_date", "DATE", date.isoformat()),
            bigquery.ScalarQueryParameter("cat", "STRING", CATEGORY_CODE),
        ]
    )
    return [dict(r) for r in bq.query(sql, job_config=job_config, location="EU").result()]


def extract_attributes(
    client: anthropic.Anthropic,
    product_name: str,
    retailer_brand: str,
) -> ProductAttributes:
    user_content = (
        f"Category: {CATEGORY_NAME_BG}\n"
        f"Category spec: {CATEGORY_SPEC}\n"
        f"Retailer: {retailer_brand}\n"
        f"Product name: {product_name!r}"
    )
    # `messages.parse()` doesn't accept top-level `cache_control` — put it on
    # the system block as a list item. Same effect: caches system + schema so
    # subsequent calls read at ~10% cost.
    response = client.messages.parse(
        model=MODEL,
        max_tokens=1024,
        thinking={"type": "adaptive"},
        output_config={"effort": "high"},
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_content}],
        output_format=ProductAttributes,
    )
    if response.parsed_output is None:
        raise ValueError(
            f"parse returned no parsed_output for {product_name!r}; "
            f"stop_reason={response.stop_reason}"
        )
    return response.parsed_output


def normalize_size(size_value: float, size_unit: str) -> tuple[int, str]:
    """Convert kg→g, l→ml; round to integer for stable hashing."""
    if size_unit == "kg":
        return round(size_value * 1000), "g"
    if size_unit == "l":
        return round(size_value * 1000), "ml"
    return round(size_value), size_unit


def compute_match_key(
    attrs: ProductAttributes, retailer_sku_id: str
) -> tuple[str, str]:
    """Return (human-readable key, sha256 hash).

    If size is 0 or None (Claude couldn't find a size in the product name),
    the SKU gets a unique key based on retailer_sku_id so it becomes a
    singleton rather than falsely clustering with other unknown-size SKUs.
    These land in the human review queue.
    """
    if not attrs.size_value:
        readable = f"_needs_size_review|sku:{retailer_sku_id[:16]}"
        return readable, hashlib.sha256(readable.encode("utf-8")).hexdigest()[:16]

    brand = attrs.brand.lower().strip()
    ptype = attrs.product_type.lower().strip()
    size_v, size_u = normalize_size(attrs.size_value, attrs.size_unit)
    fat = f"{attrs.fat_pct:.1f}" if attrs.fat_pct is not None else "null"
    variant = (attrs.variant or "").lower().strip() or "null"
    readable = f"{brand}|{ptype}|{size_v}{size_u}|fat:{fat}|var:{variant}"
    return readable, hashlib.sha256(readable.encode("utf-8")).hexdigest()[:16]


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        stream=sys.stdout,
    )
    args = parse_args()
    date = dt.date.fromisoformat(args.date)

    if not os.environ.get("ANTHROPIC_API_KEY"):
        sys.exit("ANTHROPIC_API_KEY must be set in the environment")

    bq = bigquery.Client()
    anthropic_client = anthropic.Anthropic()
    project = bq.project

    log.info("fetching category-%s SKUs for %s from staged", CATEGORY_CODE, date)
    skus = get_unique_skus(bq, project, date)
    log.info("  %d unique SKUs found", len(skus))

    if args.limit:
        skus = skus[: args.limit]
        log.info("  (--limit) processing only first %d", len(skus))

    results: list[dict] = []
    for i, sku in enumerate(skus, start=1):
        log.info(
            "[%d/%d] %d rows  %s  //  %s",
            i,
            len(skus),
            sku["row_count"],
            sku["retailer_brand"],
            sku["product_name"],
        )
        try:
            attrs = extract_attributes(
                anthropic_client, sku["product_name"], sku["retailer_brand"]
            )
        except Exception as e:
            log.error("  extraction failed: %s", e)
            continue

        key_readable, key_hash = compute_match_key(attrs, sku["retailer_sku_id"])
        log.info("  -> %s  (conf %.2f)", key_readable, attrs.confidence)

        results.append(
            {
                "retailer_sku_id": sku["retailer_sku_id"],
                "retailer_eik": sku["retailer_eik"],
                "retailer_brand": sku["retailer_brand"],
                "product_name": sku["product_name"],
                "row_count": sku["row_count"],
                "attrs": attrs.model_dump(),
                "match_key_readable": key_readable,
                "match_key": key_hash,
            }
        )

    # Group by match_key (largest groups first)
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        groups[r["match_key"]].append(r)

    out_path = Path(args.output)
    out_path.write_text(
        json.dumps(
            {
                "date": date.isoformat(),
                "category_code": CATEGORY_CODE,
                "category_name_bg": CATEGORY_NAME_BG,
                "model": MODEL,
                "total_skus": len(skus),
                "extracted": len(results),
                "groups": {
                    k: v
                    for k, v in sorted(
                        groups.items(), key=lambda kv: -len(kv[1])
                    )
                },
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        )
    )

    low_conf = [r for r in results if r["attrs"]["confidence"] < 0.8]
    singleton_keys = [k for k, v in groups.items() if len(v) == 1]

    log.info("")
    log.info("=" * 64)
    log.info("SUMMARY")
    log.info("=" * 64)
    log.info("  Total unique SKUs:           %d", len(skus))
    log.info("  Successfully extracted:       %d", len(results))
    log.info("  Failed:                        %d", len(skus) - len(results))
    log.info("  Low-confidence (<0.8):         %d", len(low_conf))
    log.info("  Groups formed:                 %d", len(groups))
    log.info("  Singleton groups (size 1):     %d", len(singleton_keys))
    log.info("  Output written:                %s", out_path)

    log.info("")
    log.info("=" * 64)
    log.info("GROUPS (excluding singletons, largest first)")
    log.info("=" * 64)
    for i, (key, members) in enumerate(
        sorted(groups.items(), key=lambda kv: -len(kv[1])), start=1
    ):
        if len(members) == 1:
            continue
        first = members[0]
        log.info("")
        log.info(
            "Group %d — %d SKUs — %s",
            i,
            len(members),
            first["match_key_readable"],
        )
        for m in members:
            log.info(
                "  %s (%s) — %d rows — %s",
                m["retailer_brand"],
                m["retailer_eik"],
                m["row_count"],
                m["product_name"],
            )

    if singleton_keys:
        log.info("")
        log.info("=" * 64)
        log.info("SINGLETONS (%d — did not match any other SKU)", len(singleton_keys))
        log.info("=" * 64)
        for k in singleton_keys:
            s = groups[k][0]
            log.info(
                "  %s (%s) — %d rows — %s",
                s["retailer_brand"],
                s["retailer_eik"],
                s["row_count"],
                s["product_name"],
            )
            log.info(
                "      -> %s  (conf %.2f)",
                s["match_key_readable"],
                s["attrs"]["confidence"],
            )

    if low_conf:
        log.info("")
        log.info("=" * 64)
        log.info("LOW-CONFIDENCE EXTRACTIONS (%d)", len(low_conf))
        log.info("=" * 64)
        for r in low_conf:
            log.info(
                "  conf=%.2f  %s",
                r["attrs"]["confidence"],
                r["product_name"],
            )
            log.info("    attrs: %s", r["attrs"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
