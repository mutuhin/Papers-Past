"""
scrape_all_text.py — Download full OCR article text for ALL Papers Past newspapers

Uses the DigitalNZ API (no API key required from a home/office IP).
Run this on your own computer, NOT on a cloud server.

Prerequisites:
    pip install curl-cffi google-cloud-storage

Input:
    output/newspapers_all.json   (already generated)

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

import asyncio
import json
import os
import random
import logging
import calendar
from pathlib import Path
from curl_cffi.requests import AsyncSession

# ─── CONFIG ──────────────────────────────────────────────────────────────────

OUTPUT_DIR     = Path("output")
DIGITALNZ_BASE = "https://api.digitalnz.org/records.json"
PER_PAGE       = 100
MAX_RETRIES    = 4
SKIP_SLUGS     = {"he-aha-nga-tanga-reo-maori"}
MAX_PAGE       = 100   # DigitalNZ hard limit — returns 400 beyond this
CONCURRENCY    = 8     # max simultaneous HTTP requests (global semaphore)
NEWSPAPER_CONCURRENCY = 3   # newspapers processed in parallel

# ─── GCS CONFIG ──────────────────────────────────────────────────────────────
# Set GCS_BUCKET to enable automatic upload after each checkpoint.
# Leave empty (or unset the env var) to disable GCS and work locally only.
#
#   export GCS_BUCKET=my-papers-past-bucket
#   export GCS_PREFIX=papers-past          # optional folder prefix (default: papers-past)
#
# Authentication uses Application Default Credentials automatically.
# Run once to authenticate:  gcloud auth application-default login

GCS_BUCKET = os.getenv("GCS_BUCKET", "")
GCS_PREFIX = os.getenv("GCS_PREFIX", "papers-past")

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── GLOBALS (initialised in main) ───────────────────────────────────────────

_sem: asyncio.Semaphore
_gcs_client = None   # lazy-initialised on first upload


def _get_gcs_client():
    global _gcs_client
    if _gcs_client is None:
        from google.cloud import storage
        _gcs_client = storage.Client()
    return _gcs_client


async def upload_to_gcs(local_path: Path) -> None:
    """Upload a local file to GCS. No-op when GCS_BUCKET is not set."""
    if not GCS_BUCKET:
        return
    blob_name = f"{GCS_PREFIX}/{local_path.name}" if GCS_PREFIX else local_path.name

    def _upload():
        bucket = _get_gcs_client().bucket(GCS_BUCKET)
        blob   = bucket.blob(blob_name)
        blob.upload_from_filename(str(local_path))

    try:
        await asyncio.to_thread(_upload)
        log.info(f"  GCS ↑ gs://{GCS_BUCKET}/{blob_name}")
    except Exception as e:
        log.warning(f"  GCS upload failed for {local_path.name}: {e}")

# ─── DIGITALNZ HELPERS ───────────────────────────────────────────────────────

def _enc(name: str) -> str:
    return name.replace(" ", "+").replace("&", "%26").replace(",", "%2C")


async def api_get(session: AsyncSession, url: str, retries: int = MAX_RETRIES):
    for attempt in range(1, retries + 1):
        await asyncio.sleep(random.uniform(0.1, 0.3))  # jitter before each attempt
        try:
            async with _sem:   # hold slot only for the actual HTTP call
                resp = await session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"Rate-limited — sleeping {wait}s")
                await asyncio.sleep(wait)
            else:
                log.warning(f"HTTP {resp.status_code} attempt {attempt}")
                await asyncio.sleep(5 * attempt)
        except Exception as e:
            log.error(f"Error attempt {attempt}: {e}")
            await asyncio.sleep(8 * attempt)
    return None


async def get_years(session: AsyncSession, collection_name: str) -> tuple:
    url = (
        f"{DIGITALNZ_BASE}?text=*"
        f"&and[collection][]={_enc(collection_name)}"
        f"&per_page=0&page=1&facets=year&facets_per_page=200"
    )
    data = await api_get(session, url)
    if not data:
        return 0, []
    search = data.get("search", {})
    total  = search.get("result_count", 0)
    years  = sorted(search.get("facets", {}).get("year", {}).keys())
    return total, years


async def _fetch_pages(session: AsyncSession, base_url: str, label: str) -> list:
    """Fetch page 1 to get the total, then gather all remaining pages concurrently."""
    first = await api_get(session, f"{base_url}&page=1")
    if not first:
        return []
    search = first.get("search", {})
    total  = search.get("result_count", 0)
    pages  = min((total + PER_PAGE - 1) // PER_PAGE, MAX_PAGE)
    log.info(f"    {label}: {total} articles, {pages} page(s)")

    articles = list(search.get("results", []))

    if pages > 1:
        async def _get_page(p: int) -> list:
            data = await api_get(session, f"{base_url}&page={p}")
            if data:
                return data.get("search", {}).get("results", [])
            log.warning(f"    {label} page {p}/{pages} failed — skipping")
            return []

        page_batches = await asyncio.gather(*[_get_page(p) for p in range(2, pages + 1)])
        for batch in page_batches:
            articles.extend(batch)

    return articles


async def fetch_year(session: AsyncSession, collection_name: str, year: str) -> list:
    """Fetch all articles for a year. Splits into months/weeks for very large years."""
    enc  = _enc(collection_name)
    base = (
        f"{DIGITALNZ_BASE}?text=*"
        f"&and[collection][]={enc}"
        f"&and[year][]={year}"
        f"&per_page={PER_PAGE}"
        f"&fields=id,title,date,fulltext,source_url"
    )

    probe = await api_get(session, f"{base}&per_page=0&page=1")
    if not probe:
        return []
    total = probe.get("search", {}).get("result_count", 0)
    log.info(f"    Year {year}: {total} total articles")

    if total <= MAX_PAGE * PER_PAGE:
        return await _fetch_pages(session, base, f"Year {year}")

    # Too many results — split into months (concurrently)
    log.info(f"    Year {year} has {total} articles (>{MAX_PAGE * PER_PAGE}) — splitting by month")

    async def fetch_month(month: int) -> list:
        _, last_day = calendar.monthrange(int(year), month)
        date_from   = f"{year}-{month:02d}-01"
        date_to     = f"{year}-{month:02d}-{last_day:02d}"
        month_base  = (
            f"{DIGITALNZ_BASE}?text=*"
            f"&and[collection][]={enc}"
            f"&and[date][]={date_from}+TO+{date_to}"
            f"&per_page={PER_PAGE}"
            f"&fields=id,title,date,fulltext,source_url"
        )
        label  = f"Year {year} month {month:02d}"
        mprobe = await api_get(session, f"{month_base}&per_page=0&page=1")
        if not mprobe:
            return []
        mcount = mprobe.get("search", {}).get("result_count", 0)
        if mcount == 0:
            return []
        if mcount <= MAX_PAGE * PER_PAGE:
            return await _fetch_pages(session, month_base, label)

        # Extremely dense month — split into weekly chunks (concurrently)
        log.info(f"    {label} still has {mcount} results — splitting by week")
        week_starts = list(range(1, last_day + 1, 7))

        async def fetch_week(ws: int) -> list:
            we        = min(ws + 6, last_day)
            week_base = (
                f"{DIGITALNZ_BASE}?text=*"
                f"&and[collection][]={enc}"
                f"&and[date][]={year}-{month:02d}-{ws:02d}+TO+{year}-{month:02d}-{we:02d}"
                f"&per_page={PER_PAGE}"
                f"&fields=id,title,date,fulltext,source_url"
            )
            return await _fetch_pages(session, week_base, f"{label} wk{ws}")

        week_results = await asyncio.gather(*[fetch_week(ws) for ws in week_starts])
        return [r for batch in week_results for r in batch]

    month_results = await asyncio.gather(*[fetch_month(m) for m in range(1, 13)])

    # Deduplicate by id
    seen_ids: set  = set()
    all_articles: list = []
    for batch in month_results:
        for r in batch:
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

async def process_newspaper(session: AsyncSession, newspaper: dict) -> list:
    slug      = newspaper["slug"]
    name      = newspaper["name"]
    out       = OUTPUT_DIR / f"{slug}_text.json"
    done_file = OUTPUT_DIR / f"{slug}_done.json"

    # Load the set of years that were fully fetched
    done_years: set = set()
    if done_file.exists():
        try:
            done_years = set(json.loads(done_file.read_text()))
        except Exception:
            pass

    # Load only articles from completed years
    all_articles: list = []
    if out.exists() and done_years:
        try:
            saved        = json.loads(out.read_text())
            all_articles = [a for a in saved if a.get("date", "")[:4] in done_years]
            log.info(f"  [{slug}] Checkpoint: {len(all_articles)} articles, "
                     f"done years: {sorted(done_years)}")
        except Exception:
            pass

    log.info(f"  [{slug}] Fetching year list…")
    total, years = await get_years(session, name)
    if total == 0:
        log.warning(f"  [{slug}] '{name}' not in DigitalNZ — skipping")
        return []
    log.info(f"  [{slug}] {total} articles across {len(years)} year(s)")

    pending = [y for y in years if y not in done_years]
    log.info(f"  [{slug}] Pending years: {len(pending)} / {len(years)}")

    for year in pending:
        raw = await fetch_year(session, name, year)

        if not raw:
            log.warning(f"  [{slug}] No articles for {year} — marking done")
            done_years.add(year)
            done_file.write_text(json.dumps(sorted(done_years), ensure_ascii=False))
            continue

        year_articles = [
            {
                "newspaper":    name,
                "slug":         slug,
                "date":         _parse_date(a.get("date")),
                "title":        a.get("title") or "",
                "fulltext":     a.get("fulltext") or "",
                "source_url":   a.get("source_url") or "",
                "digitalnz_id": a.get("id"),
            }
            for a in raw
        ]
        year_articles.sort(key=lambda x: (x["date"], x["title"]))
        all_articles.extend(year_articles)

        # Checkpoint: mark year done before writing so a crash mid-write is recoverable
        done_years.add(year)
        done_file.write_text(json.dumps(sorted(done_years), ensure_ascii=False))
        out.write_text(json.dumps(all_articles, indent=2, ensure_ascii=False))
        await asyncio.gather(upload_to_gcs(done_file), upload_to_gcs(out))

        with_text = sum(1 for a in year_articles if a.get("fulltext"))
        log.info(f"  [{slug}] Year {year}: +{len(year_articles)} articles "
                 f"({with_text} with text) → {len(all_articles)} total")

    out.write_text(json.dumps(all_articles, indent=2, ensure_ascii=False))
    await upload_to_gcs(out)
    with_text = sum(1 for a in all_articles if a.get("fulltext"))
    log.info(f"  [{slug}] Complete: {len(all_articles)} articles, {with_text} with OCR text")
    return all_articles


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    global _sem
    _sem = asyncio.Semaphore(CONCURRENCY)
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Migration: create _done.json for any legacy _text.json that lacks one
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

    # Accept either newspapers_all.json or all_calendar_links.json
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

    log.info(f"Loading {newspapers_file}…")
    raw = json.loads(newspapers_file.read_text())

    seen = {}
    for entry in raw:
        slug = entry.get("slug", "")
        name = entry.get("newspaper") or entry.get("name") or slug.replace("-", " ").title()
        if slug and slug not in seen:
            seen[slug] = name
    newspapers = [{"slug": s, "name": n} for s, n in seen.items()
                  if s not in SKIP_SLUGS]
    newspapers.sort(key=lambda x: x["slug"])
    log.info(f"Total newspapers: {len(newspapers)}")
    log.info("=" * 65)

    all_combined: list = []
    summary: list = []

    # Pre-filter already-complete newspapers
    pending_newspapers = []
    for newspaper in newspapers:
        slug      = newspaper["slug"]
        name      = newspaper["name"]
        out       = OUTPUT_DIR / f"{slug}_text.json"
        done_file = OUTPUT_DIR / f"{slug}_done.json"
        if done_file.exists() and out.exists():
            try:
                existing = json.loads(out.read_text())
                done_yrs = json.loads(done_file.read_text())
                if existing and done_yrs:
                    log.info(f"  Already complete: {name} ({len(existing)} articles) — skipping")
                    all_combined.extend(existing)
                    summary.append((name, len(existing)))
                    continue
            except Exception:
                pass
        pending_newspapers.append(newspaper)

    log.info(f"Newspapers to fetch: {len(pending_newspapers)}")

    news_sem = asyncio.Semaphore(NEWSPAPER_CONCURRENCY)

    async def run_newspaper(session: AsyncSession, newspaper: dict):
        async with news_sem:
            return newspaper["name"], await process_newspaper(session, newspaper)

    async with AsyncSession() as session:
        tasks   = [run_newspaper(session, np) for np in pending_newspapers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            log.error(f"Newspaper task failed: {result}")
            continue
        name, articles = result
        all_combined.extend(articles)
        summary.append((name, len(articles)))

    # Save master file
    combined_path   = OUTPUT_DIR / "all_text.json"
    combined_sorted = sorted(all_combined, key=lambda x: (x["slug"], x["date"], x["title"]))
    combined_path.write_text(json.dumps(combined_sorted, indent=2, ensure_ascii=False))
    await upload_to_gcs(combined_path)

    log.info(f"\n{'='*65}")
    log.info("COMPLETE — Summary:")
    for name, n in summary:
        log.info(f"  {name}: {n} articles")
    log.info(f"{'='*65}")
    log.info(f"Grand total : {len(combined_sorted):,} articles")
    log.info(f"Master file : {combined_path}")


if __name__ == "__main__":
    asyncio.run(main())
