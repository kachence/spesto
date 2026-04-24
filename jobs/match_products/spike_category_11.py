"""Spike: extract attributes + form match groups for category 11 (кашкавал — packaged).

Purpose
-------
Validate prompt portability — does the structure of the category-6 prompt
(brand normalization to canonical Latin, rules for product_type/variant/size,
confidence thresholds) generalize to a different category with different
vocabulary? Expected findings:

  * Structure generalizes — same schema, same match-key approach.
  * Content per category is different — brand list, variant rules, size units.
  * One small structural addition: kashkaval can have multiple variants at
    once (овче + БДС, био + овче, etc.). compute_match_key normalizes
    multi-variant strings to a deterministic canonical form.

If this validates, the phase-2 production job reads the prompt from
`prompts/category_{N}.md` based on a `--category` arg — same code, per-category
content files.
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

CATEGORY_CODE = "11"
CATEGORY_NAME_BG = "Кашкавал (пакетиран)"
CATEGORY_SPEC = "Official: Кашкавал от краве мляко, пакетиран от 200 г до 1 кг"
MODEL = "claude-opus-4-7"


class ProductAttributes(BaseModel):
    """Structured extraction schema for a grocery product name."""

    brand: str = Field(
        description=(
            "Manufacturer brand (not retailer). Canonical lowercase Latin. "
            "See system prompt for the brand table and transliteration rules. "
            "If no brand is identifiable, use 'unknown'."
        )
    )
    product_type: str = Field(
        description=(
            "Generic Bulgarian product type — for this category always 'кашкавал'. "
            "Don't include brand, size, fat, milk source, or BDS."
        )
    )
    size_value: float = Field(description="Numeric size as stated in the name.")
    size_unit: Literal["g", "kg", "ml", "l"] = Field(
        description="Size unit as written (almost always 'g' for this category)."
    )
    fat_pct: float | None = Field(
        default=None,
        description=(
            "Fat percentage (0-100) if stated in the name, null otherwise. "
            "Kashkaval is typically NOT labeled with fat% (default ~45% on dry "
            "matter per BDS)."
        ),
    )
    variant: str | None = Field(
        default=None,
        description=(
            "Distinguishing variant(s). Multiple can apply — separate with ', '. "
            "See system prompt for variant rules and list. Null if no variant."
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
physical product with different wording — "Домлян кашкавал вакуум 200г" vs \
"Кашкавал от краве мляко ДОМЛЯН 200г" — your extraction must produce identical \
brand / product_type / size_value / size_unit / variant, so they group together.

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

| If source says...                          | Output brand as...        |
|--------------------------------------------|---------------------------|
| "Домлян" / "Domlian"                       | domlian                   |
| "Маджаров" / "Димитър Маджаров"           | madzharov                 |
| "Верея" / "Vereya"                         | vereya                    |
| "Саяна" / "Sayana"                         | sayana                    |
| "Здравец"                                  | zdravets                  |
| "Булгарче" / "Bulgarche"                   | bulgarche                 |
| "Белииса" / "Beliisa"                      | beliisa                   |
| "Беленско"                                 | belensko                  |
| "Боженци" / "Bozhentsi"                    | bozhentsi                 |
| "Тертер"                                   | terter                    |
| "Рикас"                                    | rikas                     |
| "Ведраре"                                  | vedrare                   |
| "Мурата"                                   | murata                    |
| "Мизия"                                    | miziya                    |
| "Трапезица"                                | trapezitsa                |
| "Рафтис" / "Raftis"                        | raftis                    |
| "Камал"                                    | kamal                     |
| "Дилбер"                                   | dilber                    |
| "Родопска наслада"                         | rodopska naslada          |
| "Имало едно време"                         | imalo edno vreme          |
| "Чановете"                                 | chanovete                 |
| "Планинско"                                | planinsko                 |
| "Орехите"                                  | orehite                   |
| "Гюбек" / "Gyubek"                         | gyubek                    |
| "LB Bulgaricum" / "Ел Би Булгарикум" / "ЕЛ БИ" | lb bulgaricum         |
| "Железница"                                | zheleznitsa               |
| "Polinna"                                  | polinna                   |
| "Старопланински"                           | staroplaninski            |
| "Milki Dream" / "Милки Дрийм" / "MILKY DREAM" / "Milky Dream" | milki dream |
| "Мургаш"                                   | murgash                   |
| "Мероне" / "Merone"                        | merone                    |
| "Пастир"                                   | pastir                    |
| "Лесидрен" / "Lesidren"                    | lesidren                  |
| "Хаджийски"                                | hadzhiyski                |
| "Каймакани"                                | kaymakani                 |
| "Равдин" / "Ravdin"                        | ravdin                    |
| "Народен дар"                              | naroden dar               |
| "Наше село"                                | nashe selo                |
| "Тракийка"                                 | trakiyka                  |
| "Филипополис" / "Filipopolis"              | filipopolis               |
| "Невенна"                                  | nevenna                   |
| "Алфатар"                                  | alfatar                   |
| "Lacrima" / "Лакрима"                      | lacrima                   |
| "Дестан" / "Destan"                        | destan                    |
| "Соколово" / "Sokolovo"                    | sokolovo                  |
| "Бор Чвор" / "Бор-Чвор" / "Bor Chvor"      | bor chvor                 |
| "Наборъ"                                   | nabor                     |
| "Жълтуша"                                  | zhaltusha                 |
| "Пършевица"                                | parshevitsa               |
| "Бр. Пееви" / "Братя Пееви"                | bratya peevi              |
| "President"                                | president                 |

Retailer private labels (treat like any other brand):

| If source says...      | Output brand as... |
|------------------------|--------------------|
| "BILLA"                | billa              |
| "CLEVER"               | clever             |
| "NN" / "KLC"           | nn                 |
| "Ф Вкус" / "F Vkus"    | f vkus             |

If you see a brand not listed above, apply the same rule: transliterate Cyrillic \
to Latin using BGN/PCGN, lowercase. Do NOT invent brands. If you're not \
confident what the Cyrillic spelling romanizes to, drop confidence to 0.5 or \
below rather than guessing.

## product_type rules

product_type is always 'кашкавал' for this category. The category is specifically \
cow-milk kashkaval per the regulatory spec. Do NOT include "от краве мляко" or \
"краве кашкавал" in product_type — those are category-level defaults. Just \
'кашкавал'.

## Variant rules (MULTIPLE can apply)

Variants that DO separate products (consumers compare these at different prices):

- 'овче' — sheep milk. IMPORTANT: typically ~3× the price of cow kashkaval; \
category-11 is officially cow-only, so овче entries here are either \
miscategorized by the retailer or a sheep-cow blend. Extract the variant \
either way.
- 'козе' — goat milk.
- 'биволско' — buffalo milk.
- 'БДС' — Bulgarian State Standard (premium quality marker, mandated \
minimum specs; carries a price premium).
- 'био' — organic / certified bio.
- 'ръчен' — handmade / artisanal.
- 'без лактоза' — lactose-free.
- 'пушен' — smoked. Distinct product.
- 'нискомаслен' — low-fat. Distinct product.
- 'с манатарки' — with porcini mushrooms (a flavored inclusion). Distinct.
- 'с трюфели' — with truffles. Distinct.
- 'с билки' — with herbs. Distinct.
- 'с поръска сор' — with seasoning sprinkle. Distinct.

For any flavor/inclusion ("с X" / "with X"), extract as variant using the \
shortest phrase that identifies the inclusion. If multiple apply, separate \
with ', '.

If multiple variants apply (very common — e.g. "овче БДС" or "био ръчен"), \
output as a COMMA-SEPARATED string in the source order. The downstream \
match-key normalizer will sort and canonicalize. Examples:

- "Домлян Овчи кашкавал БДС 200г" → variant: "овче, БДС"
- "Био овчи кашкавал ръчен 250г" → variant: "био, овче, ръчен"
- "Кашкавал пушен 200г" → variant: "пушен"
- "Домлян кашкавал 200г" → variant: null (no variant)

NOT variants — these are packaging / format / cut, not distinguishing attributes:

- 'слайс' / 'на филии' — pre-sliced
- 'вакуум' — vacuum packed
- 'пита' — whole wheel
- 'разфасован' — portioned
- 'на хапки' — bite-size pieces
- 'с капачка' — with lid
- 'опаковка' — packaging generic

Processing methods (UHT-equivalent for cheese) — NOT variants:

- 'пастьоризиран' — pasteurized
- 'обогатен' — fortified (unless clearly the product differentiator)

## Size rules

Sizes for this category are in grams. Range per spec is 200 г — 1 кг. Some \
retailers give an approximate weight ("около 250г") — use the numeric value. \
Some give a range ("от 200 г до 250 г") — use the midpoint or the first value.

## Fat rules

Kashkaval is typically NOT labeled with a fat percentage (the BDS standard fixes \
fat-on-dry-matter at ~45%). Return fat_pct=null unless the name explicitly \
states a percentage. Do NOT invent a default fat%.

## When to drop confidence

- No brand identifiable ("Кашкавал 250г" with no brand at all) → \
brand='unknown', confidence ≤ 0.5.
- Brand looks like a compound / sub-line (e.g. "Маджаров Боженски") — use \
parent brand ('madzharov') with confidence ~0.8, note uncertainty.
- Cyrillic brand you can't confidently transliterate → confidence ≤ 0.5.
- Ambiguous variant — e.g. "светъл" could be a variant or just an adjective → \
confidence ≤ 0.7.
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    default_date = (dt.datetime.now(SOFIA).date() - dt.timedelta(days=1)).isoformat()
    p.add_argument("--date", default=default_date, help="YYYY-MM-DD (defaults to yesterday BG time).")
    p.add_argument("--output", default="/tmp/spike_category_11.json")
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


def normalize_variant(variant: str | None) -> str:
    """Canonicalize multi-variant strings.

    Split on commas, trim, lowercase, sort alphabetically, rejoin. Ensures
    "овче, БДС" and "БДС, овче" produce the same match key.
    """
    if variant is None:
        return "null"
    parts = [p.strip().lower() for p in variant.split(",")]
    parts = [p for p in parts if p]
    if not parts:
        return "null"
    return ", ".join(sorted(parts))


def compute_match_key(
    attrs: ProductAttributes, retailer_sku_id: str
) -> tuple[str, str]:
    """Return (human-readable key, sha256 hash).

    If size is 0 or None (Claude couldn't find a size in the product name —
    common with Lidl kashkaval SKUs that omit weight from the name), the SKU
    gets a unique key based on retailer_sku_id so it becomes a singleton
    rather than falsely clustering with other unknown-size SKUs. These land
    in the human review queue.
    """
    if not attrs.size_value:
        readable = f"_needs_size_review|sku:{retailer_sku_id[:16]}"
        return readable, hashlib.sha256(readable.encode("utf-8")).hexdigest()[:16]

    brand = attrs.brand.lower().strip()
    ptype = attrs.product_type.lower().strip()
    size_v, size_u = normalize_size(attrs.size_value, attrs.size_unit)
    fat = f"{attrs.fat_pct:.1f}" if attrs.fat_pct is not None else "null"
    variant = normalize_variant(attrs.variant)
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
