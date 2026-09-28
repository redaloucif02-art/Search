#!/usr/bin/env python3
"""Crawl les URLs du CSV, nettoie le HTML, écrit du JSONL avec reprise automatique.

- Shardé par domaine (hash) -> politesse par domaine respectée dans un même shard
- Checkpoint = le JSONL lui-même (append-only, flush à chaque ligne). Relance = skip des id déjà présents.
- Rotation des fichiers part-0001.jsonl, part-0002.jsonl ... (~40 Mo max)
- Arrêt propre à --max-minutes (les requêtes en cours se terminent)
"""
import argparse, asyncio, csv, hashlib, json, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup, Comment

SKIP_HOSTS = ("facebook.com", "instagram.com", "linkedin.com", "twitter.com",
              "x.com", "youtube.com", "tiktok.com", "pinterest.com")
UA = "Mozilla/5.0 (compatible; TheGreatestDevBot/1.0; +https://thegreatestdev.com)"
MAX_BYTES = 3_000_000
PART_LIMIT = 40 * 1024 * 1024
JUNK = ["script", "style", "noscript", "svg", "iframe", "template", "canvas",
        "video", "audio", "picture", "source", "link", "object", "embed", "form input[type=hidden]"]


def host(url):
    return (urlparse(url).netloc or "").lower().removeprefix("www.")


def shard_of(url, n):
    return int(hashlib.md5(host(url).encode()).hexdigest(), 16) % n


def clean_html(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    for c in soup.find_all(string=lambda s: isinstance(s, Comment)):
        c.extract()
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    meta = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    description = (meta.get("content") or "").strip() if meta else ""
    canonical = ""
    lc = soup.find("link", rel="canonical")
    if lc and lc.get("href"):
        canonical = urljoin(base_url, lc["href"])
    emails, phones = set(), set()
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if h.lower().startswith("mailto:"):
            emails.add(h[7:].split("?")[0].strip())
        elif h.lower().startswith("tel:"):
            phones.add(h[4:].strip())
    for t in soup(JUNK):
        t.decompose()
    for t in soup.find_all(True):
        # supprime tous les attributs sauf href (garde le sens, vire style/class/data-*)
        t.attrs = {k: v for k, v in t.attrs.items() if k == "href"}
    body = soup.body or soup
    text = body.get_text("\n", strip=True)
    text = re.sub(r"[ \t\u00a0]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return {"title": title, "description": description, "canonical": canonical,
            "emails": sorted(emails), "phones": sorted(phones), "text": text}


class Writer:
    def __init__(self, outdir):
        self.dir = Path(outdir)
        self.dir.mkdir(parents=True, exist_ok=True)
        parts = sorted(self.dir.glob("part-*.jsonl"))
        self.idx = int(parts[-1].stem.split("-")[1]) if parts else 1
        self.f = None
        self._open()

    def _path(self):
        return self.dir / f"part-{self.idx:04d}.jsonl"

    def _open(self):
        self.f = open(self._path(), "a", encoding="utf-8")

    def write(self, rec):
        if self.f.tell() > PART_LIMIT:
            self.f.close()
            self.idx += 1
            self._open()
        self.f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.f.flush()

    def close(self):
        self.f.close()


def load_done(outdir, retry_errors):
    done = set()
    for p in Path(outdir).glob("part-*.jsonl"):
        with open(p, encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue  # ligne coupée par un crash -> sera refaite
                if retry_errors and r.get("status") == "error":
                    continue
                done.add(r["id"])
    return done


async def fetch_one(client, row, sems, delay, writer, stats):
    url = row["url"]
    h = host(url)
    rec = {"id": row["llm_link_id"], "siren": row["siren"], "nom": row["nom"],
           "category": row["category"], "url": url}
    async with sems[h]:
        for attempt in range(3):
            try:
                r = await client.get(url)
                ct = r.headers.get("content-type", "")
                rec.update(final_url=str(r.url), http_status=r.status_code, content_type=ct)
                if "html" not in ct.lower() and "xml" not in ct.lower() and ct:
                    rec.update(status="skipped_non_html")
                elif r.status_code >= 400 and r.status_code not in (403, 404, 410) and attempt < 2:
                    await asyncio.sleep(2 * (attempt + 1)); continue
                else:
                    rec.update(clean_html(r.text[:MAX_BYTES], str(r.url)))
                    rec["status"] = "ok" if r.status_code < 400 else f"http_{r.status_code}"
                break
            except Exception as e:
                rec.update(status="error", error=f"{type(e).__name__}: {str(e)[:200]}")
                if attempt < 2:
                    await asyncio.sleep(2 * (attempt + 1))
        await asyncio.sleep(delay)
    rec["fetched_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    writer.write(rec)
    stats["n"] += 1
    stats[rec["status"]] = stats.get(rec["status"], 0) + 1


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="input/links.csv")
    ap.add_argument("--out", required=True)
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--max-minutes", type=float, default=330)
    ap.add_argument("--concurrency", type=int, default=40)
    ap.add_argument("--per-domain", type=int, default=2)
    ap.add_argument("--delay", type=float, default=0.7)
    ap.add_argument("--retry-errors", action="store_true")
    a = ap.parse_args()

    rows = [r for r in csv.DictReader(open(a.csv, encoding="utf-8-sig"))
            if r.get("url") and shard_of(r["url"], a.shards) == a.shard]
    writer = Writer(a.out)
    done = load_done(a.out, a.retry_errors)
    todo = []
    for r in rows:
        if r["llm_link_id"] in done:
            continue
        if any(host(r["url"]).endswith(s) for s in SKIP_HOSTS):
            writer.write({"id": r["llm_link_id"], "siren": r["siren"], "nom": r["nom"],
                          "category": r["category"], "url": r["url"], "status": "skipped_social"})
            continue
        todo.append(r)
    # ordre stable : un domaine à la fois évite de bloquer sur un seul hôte
    print(f"shard {a.shard}/{a.shards}: {len(rows)} urls, {len(done)} déjà faites, {len(todo)} restantes", flush=True)

    deadline = time.time() + a.max_minutes * 60
    sems = {}
    for r in todo:
        sems.setdefault(host(r["url"]), asyncio.Semaphore(a.per_domain))
    stats = {"n": 0}
    queue = asyncio.Queue()
    for r in todo:
        queue.put_nowait(r)
    limits = httpx.Limits(max_connections=a.concurrency)
    async with httpx.AsyncClient(headers={"User-Agent": UA, "Accept-Language": "fr-FR,fr;q=0.9"},
                                 timeout=25, follow_redirects=True, limits=limits, verify=False) as client:
        async def worker():
            while time.time() < deadline:
                try:
                    row = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                await fetch_one(client, row, sems, a.delay, writer, stats)
                if stats["n"] % 200 == 0:
                    print(f"  {stats}", flush=True)
        await asyncio.gather(*[worker() for _ in range(a.concurrency)])
    writer.close()
    remaining = queue.qsize()
    Path(a.out, "remaining.txt").write_text(str(remaining))
    print(f"fini: {stats} | restantes: {remaining}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
