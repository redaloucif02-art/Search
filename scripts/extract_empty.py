#!/usr/bin/env python3
"""Extrait tous les enregistrements sans texte (vide, erreur, skip) dans un fichier à part."""
import argparse, json
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--data", default="data")
ap.add_argument("--out", default="data/empty/empty.jsonl")
ap.add_argument("--min-chars", type=int, default=1, help="texte < N caractères = vide")
a = ap.parse_args()

seen, n, counts = set(), 0, {}
Path(a.out).parent.mkdir(parents=True, exist_ok=True)
with open(a.out, "w", encoding="utf-8") as out:
    for p in sorted(Path(a.data).glob("shard-*/part-*.jsonl")):
        for line in open(p, encoding="utf-8"):
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if len((r.get("text") or "").strip()) >= a.min_chars or r["id"] in seen:
                continue
            seen.add(r["id"])
            out.write(json.dumps({k: r.get(k) for k in
                ("id", "siren", "nom", "category", "url", "final_url", "http_status", "status", "error", "title")
                if r.get(k) is not None}, ensure_ascii=False) + "\n")
            n += 1
            counts[r.get("status")] = counts.get(r.get("status"), 0) + 1
print(f"{n} vides -> {a.out} | {counts}")
