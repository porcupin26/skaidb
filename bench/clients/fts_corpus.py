#!/usr/bin/env python3
"""Prepare the FTS benchmark corpus and query set from a MediaWiki
pages-articles dump (e.g. simplewiki-latest-pages-articles.xml.bz2 —
Wikimedia discontinued the abstract dumps, so we take the lead prose of
real articles instead: ns-0, redirects skipped, wikitext coarsely
stripped, first ~2000 chars).

Emits:
  corpus.jsonl   {"id": n, "title": ..., "body": ...} per line
  queries.json   {"term": [...], "and": [[a,b],...], "or": [[a,b],...],
                  "phrase": [...]} — matched query sets both engines run
                 verbatim.

Usage: fts_corpus.py <pages-articles.xml.bz2> <out_dir> [max_docs]
"""

import bz2
import json
import random
import re
import sys
from collections import Counter
from xml.etree.ElementTree import iterparse

STOPWORDS = set(
    "the of and in to a is was for on as by with from at it an be are this "
    "that or his her which had also its were has have not but he she they".split()
)
WORD = re.compile(r"[a-z]{3,}")
NS = "{http://www.mediawiki.org/xml/export-0.11/}"

RE_REF = re.compile(r"<ref[^>]*?/>|<ref.*?</ref>", re.S)
RE_TAG = re.compile(r"<[^>]+>")
RE_LINK_PIPED = re.compile(r"\[\[[^\[\]|]*\|([^\[\]]*)\]\]")
RE_LINK = re.compile(r"\[\[([^\[\]]*)\]\]")
RE_EXTLINK = re.compile(r"\[https?://\S*\s?([^\]]*)\]")
RE_URL = re.compile(r"https?://\S+")
RE_TTE = re.compile(r"\{\{[^{}]*\}\}|\{\|[^{}]*\|\}", re.S)
RE_HEAD = re.compile(r"^=+.*?=+\s*$", re.M)
RE_WS = re.compile(r"\s+")


def strip_wikitext(text: str) -> str:
    text = RE_REF.sub(" ", text)
    for _ in range(4):  # nested templates/tables, coarse inside-out passes
        new = RE_TTE.sub(" ", text)
        if new == text:
            break
        text = new
    text = RE_LINK_PIPED.sub(r"\1", text)
    text = RE_LINK.sub(r"\1", text)
    text = RE_EXTLINK.sub(r"\1", text)
    text = RE_URL.sub(" ", text)
    text = RE_TAG.sub(" ", text)
    text = RE_HEAD.sub(" ", text)
    text = text.replace("'''", "").replace("''", "")
    return RE_WS.sub(" ", text).strip()


def main() -> None:
    src, out_dir = sys.argv[1], sys.argv[2]
    max_docs = int(sys.argv[3]) if len(sys.argv) > 3 else 500_000

    docs = 0
    tf = Counter()
    phrases = []
    rng = random.Random(0x5EED)
    with bz2.open(src, "rb") as fh, open(f"{out_dir}/corpus.jsonl", "w") as out:
        for _, elem in iterparse(fh):
            if elem.tag != f"{NS}page":
                continue
            ns = elem.findtext(f"{NS}ns")
            redirect = elem.find(f"{NS}redirect")
            title = (elem.findtext(f"{NS}title") or "").strip()
            raw = elem.findtext(f"{NS}revision/{NS}text") or ""
            elem.clear()
            if ns != "0" or redirect is not None or len(title) < 3:
                continue
            body = strip_wikitext(raw)[:2000]
            if len(body) < 80:
                continue
            docs += 1
            out.write(json.dumps({"id": docs, "title": title, "body": body}) + "\n")
            words = WORD.findall(body.lower())
            tf.update(w for w in words if w not in STOPWORDS)
            # Sample real adjacent word pairs as phrase queries (they hit).
            if len(words) >= 2 and rng.random() < 0.01 and len(phrases) < 2000:
                i = rng.randrange(len(words) - 1)
                phrases.append(f"{words[i]} {words[i + 1]}")
            if docs >= max_docs:
                break

    # Query terms: mid-to-high document frequency, sampled deterministically
    # (the very top terms are near-stopwords; rank 20..2000 is the realistic
    # searchy band).
    common = [w for w, _ in tf.most_common(2000)][20:]
    rng.shuffle(common)
    terms = common[:100]
    ands = [[common[100 + i], common[200 + i]] for i in range(100)]
    ors = [[common[300 + i], common[400 + i]] for i in range(100)]
    rng.shuffle(phrases)
    with open(f"{out_dir}/queries.json", "w") as out:
        json.dump(
            {"term": terms, "and": ands, "or": ors, "phrase": phrases[:100]},
            out,
            indent=1,
        )
    print(f"{docs} docs, {len(tf)} distinct terms, queries written")


if __name__ == "__main__":
    main()
