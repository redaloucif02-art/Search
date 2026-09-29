import csv, json, os, re, sys, time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

SHARD = int(os.getenv("SHARD", 0))
N_SHARDS = int(os.getenv("N_SHARDS", 1))
WORKERS = 10
TIMEOUT = 15
MAX_CHARS = 6000  # texte gardé par page

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Chrome/120.0)"}
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9\-]+(?:\.[a-zA-Z0-9\-]+)*\.[a-zA-Z]{2,}")
BAD_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js")
BAD_PARTS = ("sentry", "wixpress", "example.", "domain.", "email.com", "votreemail", "@2x", "u003e")


def clean_url(u):
    # le CSV contient parfois "https://site/contact,110" -> on enlève ",110"
    return re.sub(r",\d+$", "", (u or "").strip())


def decode_cf(hexstr):
    try:
        k = int(hexstr[:2], 16)
        return "".join(chr(int(hexstr[i:i + 2], 16) ^ k) for i in range(2, len(hexstr), 2))
    except Exception:
        return None


def valid_email(e):
    e = e.lower().strip(".")
    return not e.endswith(BAD_EXT) and not any(b in e for b in BAD_PARTS)


def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
            return r.text
    except Exception:
        pass
    return None


def parse(html):
    soup = BeautifulSoup(html, "html.parser")
    emails = set()
    for a in soup.select('a[href^="mailto:"]'):
        emails.add(a["href"][7:].split("?")[0].strip())
    for el in soup.select("[data-cfemail]"):
        d = decode_cf(el["data-cfemail"])
        if d:
            emails.add(d)
    for t in soup(["script", "style", "svg", "noscript", "iframe", "img"]):
        t.decompose()
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    emails.update(EMAIL_RE.findall(text))
    return {e.lower() for e in emails if valid_email(e)}, text[:MAX_CHARS]


def process(row):
    out = {"siren": row["siren"], "nom": row["nom"], "emails": [], "pages": {}}
    urls = [clean_url(row.get(k)) for k in ("url_contact", "url_mentions_legales", "url_equipe")]
    urls = [u for u in urls if u]
    if not urls and row.get("site_web"):
        urls = [row["site_web"].strip()]
    found = set()
    for u in dict.fromkeys(urls):
        html = fetch(u)
        if not html:
            continue
        emails, text = parse(html)
        found |= emails
        out["pages"][u] = text
        time.sleep(0.3)
    out["emails"] = sorted(found)
    return out


def main():
    with open("urls_a_scraper.csv", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    rows = [r for i, r in enumerate(rows) if i % N_SHARDS == SHARD]
    print(f"Shard {SHARD}/{N_SHARDS}: {len(rows)} agences", flush=True)
    os.makedirs("out", exist_ok=True)
    with open(f"out/shard_{SHARD}.jsonl", "w", encoding="utf-8") as fo, \
         ThreadPoolExecutor(WORKERS) as ex:
        for i, res in enumerate(ex.map(process, rows), 1):
            fo.write(json.dumps(res, ensure_ascii=False) + "\n")
            fo.flush()
            if i % 50 == 0:
                print(f"{i}/{len(rows)}", flush=True)


if __name__ == "__main__":
    main()
