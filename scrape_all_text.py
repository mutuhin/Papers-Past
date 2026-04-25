"""
scrape_all_text.py — Download full OCR article text for ALL Papers Past newspapers

Uses the DigitalNZ API (no API key required from a home/office IP).
Run this on your own computer, NOT on a cloud server.

Prerequisites:
    pip install curl-cffi

Input:
    output/newspapers_all.json   (already generated — copy from the Replit output/ folder)

Output:
    output/<slug>_text.json      per-newspaper article text (checkpoint files)
    output/all_text.json         single combined master file

Each record:
    {
      "newspaper":    "Akaroa Mail and Banks Peninsula Advertiser",
      "slug":         "akaroa-mail-and-banks-peninsula-advertiser",
      "date":         "1878-03-15",
      "title":        "SHIPPING INTELLIGENCE",
      "fulltext":     "ARRIVED. March 14...",
      "source_url":   "https://paperspast.natlib.govt.nz/...",
      "digitalnz_id": 12345678
    }

Resume-safe: already-fetched years and completed newspapers are skipped.
"""

import json
import time
import random
import logging
from pathlib import Path
from curl_cffi import requests as cffi_requests

# ─── CONFIG ──────────────────────────────────────────────────────────────────

OUTPUT_DIR     = Path("output")
DIGITALNZ_BASE = "https://api.digitalnz.org/records.json"
BASE_URL       = "https://paperspast.natlib.govt.nz"
PER_PAGE       = 100
DELAY_MIN      = 1.0
DELAY_MAX      = 2.5
MAX_RETRIES    = 4
SKIP_SLUGS     = {"he-aha-nga-tanga-reo-maori"}

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── DIGITALNZ HELPERS ───────────────────────────────────────────────────────

def _sleep():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def api_get(url: str, retries: int = MAX_RETRIES):
    for attempt in range(1, retries + 1):
        _sleep()
        try:
            resp = cffi_requests.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 90 * attempt
                log.warning(f"  Rate-limited — sleeping {wait}s")
                time.sleep(wait)
            else:
                log.warning(f"  HTTP {resp.status_code} attempt {attempt}")
                time.sleep(8 * attempt)
        except Exception as e:
            log.error(f"  Error attempt {attempt}: {e}")
            time.sleep(12 * attempt)
    return None


def _enc(name: str) -> str:
    return name.replace(" ", "+").replace("&", "%26").replace(",", "%2C")


def get_years(collection_name: str) -> tuple:
    url = (
        f"{DIGITALNZ_BASE}?text=*"
        f"&and[collection][]={_enc(collection_name)}"
        f"&per_page=0&page=1&facets=year&facets_per_page=200"
    )
    data = api_get(url)
    if not data:
        return 0, []
    search = data.get("search", {})
    total  = search.get("result_count", 0)
    years  = sorted(search.get("facets", {}).get("year", {}).keys())
    return total, years


MAX_PAGE = 100  # DigitalNZ hard limit — returns 400 beyond this


def _fetch_pages(base_url: str, label: str) -> list:
    """Fetch all pages of a query, stopping at DigitalNZ's page-100 cap."""
    first = api_get(f"{base_url}&page=1")
    if not first:
        return []
    search = first.get("search", {})
    total  = search.get("result_count", 0)
    pages  = min((total + PER_PAGE - 1) // PER_PAGE, MAX_PAGE)
    log.info(f"    {label}: {total} articles, fetching {pages} page(s)")

    articles = list(search.get("results", []))
    for page in range(2, pages + 1):
        data = api_get(f"{base_url}&page={page}")
        if data:
            articles.extend(data.get("search", {}).get("results", []))
        else:
            log.warning(f"    {label} page {page}/{pages} failed — skipping")
    return articles


def fetch_year(collection_name: str, year: str) -> list:
    """Fetch all articles for a year. If > 10,000 results, splits into months."""
    enc  = _enc(collection_name)
    base = (
        f"{DIGITALNZ_BASE}?text=*"
        f"&and[collection][]={enc}"
        f"&and[year][]={year}"
        f"&per_page={PER_PAGE}"
        f"&fields=id,title,date,fulltext,source_url"
    )

    # Quick check: how many total results?
    probe = api_get(f"{base}&per_page=0&page=1")
    if not probe:
        return []
    total = probe.get("search", {}).get("result_count", 0)
    log.info(f"    Year {year}: {total} total articles")

    if total <= MAX_PAGE * PER_PAGE:
        # Fits within 100 pages — fetch directly
        return _fetch_pages(base, f"Year {year}")

    # Too many results — split into monthly sub-queries
    log.info(f"    Year {year} has {total} articles (>{MAX_PAGE*PER_PAGE}) — splitting by month")
    import calendar
    all_articles = []
    seen_ids = set()

    for month in range(1, 13):
        _, last_day = calendar.monthrange(int(year), month)
        date_from = f"{year}-{month:02d}-01"
        date_to   = f"{year}-{month:02d}-{last_day:02d}"
        month_base = (
            f"{DIGITALNZ_BASE}?text=*"
            f"&and[collection][]={enc}"
            f"&and[date][]={date_from}+TO+{date_to}"
            f"&per_page={PER_PAGE}"
            f"&fields=id,title,date,fulltext,source_url"
        )
        label = f"Year {year} month {month:02d}"

        # Check monthly count
        mprobe = api_get(f"{month_base}&per_page=0&page=1")
        if not mprobe:
            log.warning(f"    {label}: probe failed — skipping")
            continue
        mcount = mprobe.get("search", {}).get("result_count", 0)
        if mcount == 0:
            continue

        if mcount <= MAX_PAGE * PER_PAGE:
            results = _fetch_pages(month_base, label)
        else:
            # Extremely dense month — split into weekly chunks
            log.info(f"    {label} still has {mcount} results — splitting by week")
            results = []
            week_starts = list(range(1, last_day + 1, 7))
            for ws in week_starts:
                we = min(ws + 6, last_day)
                wfrom = f"{year}-{month:02d}-{ws:02d}"
                wto   = f"{year}-{month:02d}-{we:02d}"
                week_base = (
                    f"{DIGITALNZ_BASE}?text=*"
                    f"&and[collection][]={enc}"
                    f"&and[date][]={wfrom}+TO+{wto}"
                    f"&per_page={PER_PAGE}"
                    f"&fields=id,title,date,fulltext,source_url"
                )
                results.extend(_fetch_pages(week_base, f"{label} wk{ws}"))

        # Deduplicate by id
        for r in results:
            rid = r.get("id")
            if rid not in seen_ids:
                seen_ids.add(rid)
                all_articles.append(r)

    log.info(f"    Year {year}: {len(all_articles)} unique articles after monthly split")
    return all_articles


def _parse_date(d) -> str:
    if isinstance(d, list):
        d = d[0] if d else ""
    return str(d)[:10]


# ─── PROCESS ONE NEWSPAPER ───────────────────────────────────────────────────

def process_newspaper(newspaper: dict) -> list:
    slug = newspaper["slug"]
    name = newspaper["name"]
    out       = OUTPUT_DIR / f"{slug}_text.json"
    done_file = OUTPUT_DIR / f"{slug}_done.json"  # tracks fully-completed years

    # Load the set of years that were fully fetched (never infer from article data)
    done_years: set = set()
    if done_file.exists():
        try:
            done_years = set(json.loads(done_file.read_text()))
        except Exception:
            pass

    # Load only articles from completed years (ignore any partial-year data)
    all_articles: list = []
    if out.exists() and done_years:
        try:
            saved = json.loads(out.read_text())
            all_articles = [a for a in saved if a.get("date", "")[:4] in done_years]
            log.info(f"  Checkpoint: {len(all_articles)} articles, "
                     f"completed years: {sorted(done_years)}")
        except Exception:
            pass

    # Get year list from DigitalNZ
    log.info(f"  Fetching year list from DigitalNZ…")
    total, years = get_years(name)
    if total == 0:
        log.warning(f"  '{name}' not in DigitalNZ — skipping")
        return []
    log.info(f"  {total} total articles across {len(years)} year(s): {years}")

    # Fetch every year that isn't fully done yet
    pending = [y for y in years if y not in done_years]
    log.info(f"  Years to fetch: {len(pending)} / {len(years)}")

    for year in pending:
        raw = fetch_year(name, year)

        if not raw:
            log.warning(f"  No articles for {year} — marking done anyway")
            done_years.add(year)
            done_file.write_text(json.dumps(sorted(done_years), ensure_ascii=False))
            continue

        year_articles = []
        for a in raw:
            year_articles.append({
                "newspaper":    name,
                "slug":         slug,
                "date":         _parse_date(a.get("date")),
                "title":        a.get("title") or "",
                "fulltext":     a.get("fulltext") or "",
                "source_url":   a.get("source_url") or "",
                "digitalnz_id": a.get("id"),
            })

        year_articles.sort(key=lambda x: (x["date"], x["title"]))
        all_articles.extend(year_articles)
        # Mark year fully done BEFORE writing so a crash mid-write is recoverable
        done_years.add(year)
        done_file.write_text(json.dumps(sorted(done_years), ensure_ascii=False))
        out.write_text(json.dumps(all_articles, indent=2, ensure_ascii=False))
        with_text = sum(1 for a in year_articles if a.get("fulltext"))
        log.info(f"  Year {year}: +{len(year_articles)} articles "
                 f"({with_text} with text) → {len(all_articles)} total")

    out.write_text(json.dumps(all_articles, indent=2, ensure_ascii=False))
    with_text = sum(1 for a in all_articles if a.get("fulltext"))
    log.info(f"  Complete: {len(all_articles)} articles, {with_text} with OCR text → {out.name}")
    return all_articles


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # ── Migration: create _done.json for any legacy _text.json that lacks one ──
    # Run BEFORE loading newspapers so already-complete files are recognised.
    # If you have a PARTIAL _text.json (e.g. ashburton-guardian from a previous
    # interrupted run), DELETE that file first so it gets re-fetched cleanly.
    migrated = 0
    for tf in OUTPUT_DIR.glob("*_text.json"):
        slug = tf.stem.replace("_text", "")
        df   = OUTPUT_DIR / f"{slug}_done.json"
        if not df.exists():
            try:
                articles = json.loads(tf.read_text())
                if articles:
                    years = sorted({a["date"][:4] for a in articles if a.get("date")})
                    df.write_text(json.dumps(years, ensure_ascii=False))
                    migrated += 1
            except Exception:
                pass
    if migrated:
        log.info(f"Migration: created _done.json for {migrated} existing newspaper(s)")

    # Accept either newspapers_all.json (196 newspaper list)
    # OR all_calendar_links.json (1M+ issue records — auto-deduplicate)
    candidates = [
        OUTPUT_DIR / "newspapers_all.json",
        OUTPUT_DIR / "all_calendar_links.json",
        Path("newspapers_all.json"),
        Path("all_calendar_links.json"),
    ]
    newspapers_file = next((p for p in candidates if p.exists()), None)
    if not newspapers_file:
        log.error(
            "Cannot find input file. Place one of these in the output/ folder:\n"
            "  • newspapers_all.json  (196-entry newspaper list)\n"
            "  • all_calendar_links.json  (full calendar links file)"
        )
        return

    log.info(f"Loading {newspapers_file} …")
    raw = json.loads(newspapers_file.read_text())

    # Build a clean {slug, name} list regardless of input format
    seen = {}
    for entry in raw:
        slug = entry.get("slug", "")
        name = entry.get("newspaper") or entry.get("name") or slug.replace("-", " ").title()
        if slug and slug not in seen:
            seen[slug] = name
    newspapers = [{"slug": s, "name": n} for s, n in seen.items()
                  if s not in SKIP_SLUGS]
    newspapers.sort(key=lambda x: x["slug"])
    log.info(f"Total newspapers to process: {len(newspapers)}")
    log.info("=" * 65)

    all_combined : list = []
    summary      : list = []

    for ni, newspaper in enumerate(newspapers, 1):
        slug = newspaper["slug"]
        name = newspaper["name"]
        out       = OUTPUT_DIR / f"{slug}_text.json"
        done_file = OUTPUT_DIR / f"{slug}_done.json"

        log.info(f"\n[{ni}/{len(newspapers)}] {name}")

        # A newspaper is fully done only when its _done.json exists
        # (written after every year completes). Text-file-only means partial.
        if done_file.exists() and out.exists():
            try:
                existing = json.loads(out.read_text())
                done_yrs = json.loads(done_file.read_text())
                if existing and done_yrs:
                    log.info(f"  Already complete: {len(existing)} articles "
                             f"across {len(done_yrs)} year(s) — skipping")
                    all_combined.extend(existing)
                    summary.append((name, len(existing)))
                    continue
            except Exception:
                pass

        articles = process_newspaper(newspaper)
        all_combined.extend(articles)
        summary.append((name, len(articles)))

    # Save master file
    combined_path = OUTPUT_DIR / "all_text.json"
    combined_sorted = sorted(all_combined, key=lambda x: (x["slug"], x["date"], x["title"]))
    combined_path.write_text(json.dumps(combined_sorted, indent=2, ensure_ascii=False))

    # Summary
    log.info(f"\n{'='*65}")
    log.info("COMPLETE — Summary:")
    for name, n in summary:
        log.info(f"  {name}: {n} articles")
    log.info(f"{'='*65}")
    log.info(f"Grand total : {len(combined_sorted):,} articles")
    log.info(f"Master file : {combined_path}")


if __name__ == "__main__":
    main()
