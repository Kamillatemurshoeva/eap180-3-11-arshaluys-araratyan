#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import math
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://eap.bl.uk"
COLLECTION_URL = "https://eap.bl.uk/collection/EAP180-3-11"
SEARCH_URL = f"{COLLECTION_URL}/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; OpenDataArmenia/1.0)"
}

DELAY = 1.0
TIMEOUT = 60

OUT_CSV = "eap180_3_11_clean.csv"
OUT_JSONL = "eap180_3_11_clean.jsonl"


def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def get_total_results_and_page_size(soup: BeautifulSoup):
    text = soup.get_text(" ", strip=True)
    m = re.search(r"Showing\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)\s+results", text, re.I)
    if not m:
        raise RuntimeError("Could not detect pagination text.")
    start_i = int(m.group(1))
    end_i = int(m.group(2))
    total = int(m.group(3))
    page_size = end_i - start_i + 1
    return total, page_size


def parse_year_from_title(title: str) -> str:
    t = clean_text(title).strip(' "\'“”')
    m = re.search(r"(\d{4})\s*$", t)
    return m.group(1) if m else ""


def extract_reference_from_text(text: str) -> str:
    m = re.search(r"File Ref:\s*([A-Z0-9/.\-]+)", text, re.I)
    return clean_text(m.group(1)) if m else ""


def extract_issue_from_title(title: str) -> str:
    m = re.search(r"Issue\s+(\d+)", title, re.I)
    return m.group(1) if m else ""


def extract_english_description_from_text(text: str) -> str:
    m = re.search(r"File Ref:\s*[A-Z0-9/.\-]+\s*(.*?)\s*Original material:", text, re.I)
    return clean_text(m.group(1)) if m else ""


def extract_results_from_page(soup: BeautifulSoup):
    rows = []
    seen = set()

    for a in soup.select('a[href^="/archive-file/"]'):
        href = a.get("href", "")
        item_url = urljoin(BASE, href)

        if item_url in seen:
            continue
        seen.add(item_url)

        title = clean_text(a.get_text(" ", strip=True)).strip('“”')
        if not title:
            continue

        container = a
        for _ in range(8):
            if container is None:
                break
            txt = clean_text(container.get_text(" ", strip=True))
            if "File Ref:" in txt:
                break
            container = container.parent

        context_text = clean_text(container.get_text(" ", strip=True)) if container else ""
        reference = extract_reference_from_text(context_text)
        english_description = extract_english_description_from_text(context_text)
        year = parse_year_from_title(title)
        issue = extract_issue_from_title(title)

        rows.append({
            "item_title_search": title,
            "item_url": item_url,
            "reference_search": reference,
            "year": year,
            "issue": issue,
            "english_description": english_description,
        })

    return rows


def extract_detail_fields(item_soup: BeautifulSoup):
    text_lines = [
        clean_text(line)
        for line in item_soup.get_text("\n").splitlines()
        if clean_text(line)
    ]

    labels = {
        "Related people:": "related_people",
        "Reference:": "reference",
        "Creation date:": "creation_date",
    }

    stop_labels = {
        "Creation date:",
        "Languages:",
        "Scripts:",
        "Content type:",
        "Originals information:",
        "Related people:",
        "Reference:",
        "This file is part of",
        "Related files",
        "Supported by",
        "Digitisation details",
        "File details",
    }

    data = {v: "" for v in labels.values()}

    for i, line in enumerate(text_lines):
        if line in labels:
            key = labels[line]
            values = []
            j = i + 1
            while j < len(text_lines):
                nxt = text_lines[j]
                if nxt in stop_labels:
                    break
                values.append(nxt)
                j += 1
            data[key] = clean_text(" ".join(values))

    return data


def parse_item_page(url: str):
    soup = get_soup(url)

    h1 = soup.find("h1")
    item_title_detail = clean_text(h1.get_text(" ", strip=True)) if h1 else ""

    details = extract_detail_fields(soup)

    return {
        "item_title_detail": item_title_detail,
        **details,
    }


def drop_empty_columns(rows):
    if not rows:
        return rows

    keep = []
    for key in rows[0].keys():
        if any(clean_text(str(row.get(key, ""))) not in {"", "None", "nan"} for row in rows):
            keep.append(key)

    return [{k: row.get(k, "") for k in keep} for row in rows]


def save_csv(rows, path):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def save_jsonl(rows, path):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main():
    first = get_soup(SEARCH_URL)
    total, page_size = get_total_results_and_page_size(first)
    total_pages = math.ceil(total / page_size)

    print(f"Total results: {total}")
    print(f"Page size: {page_size}")
    print(f"Total pages: {total_pages}")

    all_items = []

    for page in range(total_pages):
        url = SEARCH_URL if page == 0 else f"{SEARCH_URL}?page={page}"
        print(f"[SEARCH PAGE {page + 1}/{total_pages}] {url}")
        soup = get_soup(url)
        page_rows = extract_results_from_page(soup)
        all_items.extend(page_rows)
        time.sleep(DELAY)

    dedup = {}
    for row in all_items:
        dedup[row["item_url"]] = row
    all_items = list(dedup.values())

    print(f"Unique items found: {len(all_items)}")

    final_rows = []

    for i, item in enumerate(all_items, start=1):
        print(f"[ITEM {i}/{len(all_items)}] {item['item_url']}")
        try:
            detail = parse_item_page(item["item_url"])
        except Exception as e:
            print(f"ERROR on {item['item_url']}: {e}")
            detail = {
                "item_title_detail": "",
                "related_people": "",
                "reference": "",
                "creation_date": "",
            }

        final_rows.append({
            "item_title": detail.get("item_title_detail") or item.get("item_title_search", ""),
            "item_url": item.get("item_url", ""),
            "reference": detail.get("reference") or item.get("reference_search", ""),
            "year": detail.get("creation_date") or item.get("year", ""),
            "issue": item.get("issue", ""),
            "english_description": item.get("english_description", ""),
            "related_people": detail.get("related_people", ""),
        })

        time.sleep(DELAY)

    final_rows = drop_empty_columns(final_rows)

    save_csv(final_rows, OUT_CSV)
    save_jsonl(final_rows, OUT_JSONL)

    print(f"Saved {len(final_rows)} rows to {OUT_CSV}")
    print(f"Saved {len(final_rows)} rows to {OUT_JSONL}")


if __name__ == "__main__":
    main()