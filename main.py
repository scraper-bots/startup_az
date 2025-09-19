"""
scrape_startup_az.py

Scrapes listing page(s) from startup.az and each startup's detail page.
Saves results to CSV and XLSX.

Usage:
    python scrape_startup_az.py
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse
import time
import random
from tqdm import tqdm
import logging
from typing import Dict, List, Optional

# ---------- Configuration ----------
BASE_URL = "https://www.startup.az"
LISTING_URL = "https://www.startup.az/startup.html?page=2&per-page=12"
USER_AGENT = "Mozilla/5.0 (compatible; StartupAZScraper/1.0; +https://example.com/bot)"
REQUESTS_TIMEOUT = 15  # seconds
SLEEP_MIN = 0.8  # between requests
SLEEP_MAX = 1.6
OUTPUT_CSV = "startup_az_page2.csv"
OUTPUT_XLSX = "startup_az_page2.xlsx"
# -----------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_soup(url: str, session: requests.Session, retries: int = 3) -> Optional[BeautifulSoup]:
    """Fetch a URL and return BeautifulSoup object or None on failure."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=REQUESTS_TIMEOUT)
            resp.raise_for_status()
            # detect and use correct encoding if provided
            if resp.encoding is None:
                resp.encoding = resp.apparent_encoding
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            logging.warning("Failed to fetch %s (attempt %d/%d): %s", url, attempt, retries, e)
            time.sleep(1 + attempt * 0.5)
    logging.error("Giving up on %s after %d attempts", url, retries)
    return None


def parse_listing_cards(soup: BeautifulSoup) -> List[Dict]:
    """
    Parse listing page soup and return list of items with:
    - title, short_desc, image_url (absolute), detail_url (absolute)
    """
    items = []
    # The HTML snippet shows each card is <a href="/startup-content/xxx" class="card mb-4"> inside .startup-block
    card_anchors = soup.select("div.startup-block a.card")
    if not card_anchors:
        # try a more general selector fallback
        card_anchors = soup.select("a.card.mb-4, a.card")
    for a in card_anchors:
        href = a.get("href", "").strip()
        detail_url = urljoin(BASE_URL, href) if href else None

        # image tag
        img_tag = a.find("img", class_="card-img-top")
        img_src = None
        if img_tag:
            img_src = img_tag.get("src") or img_tag.get("data-src") or ""
            if img_src:
                img_src = urljoin(BASE_URL, img_src)

        # title
        title_tag = a.select_one(".card-body .card-title")
        title = title_tag.get_text(strip=True) if title_tag else None

        # short desc
        desc_tag = a.select_one(".card-body .card-text")
        short_desc = desc_tag.get_text(separator=" ", strip=True) if desc_tag else None

        items.append({
            "title": title,
            "short_desc": short_desc,
            "image_url": img_src,
            "detail_url": detail_url,
        })

    logging.info("Found %d cards on listing page", len(items))
    return items


def extract_detail_fields(soup: BeautifulSoup) -> Dict[str, str]:
    """
    For a detail page soup, extract the fields shown in the example.
    The detail page uses structure like:
        <h4 class="...">Başlıq</h4>
        <p class="mb-0">value</p>
    We'll search for the label <h4> text and take the following <p> or text sibling.
    """
    result: Dict[str, str] = {}
    # First: main image (if present)
    img = soup.select_one("article.post .post-image img, .post-image img")
    if img:
        img_src = img.get("src") or ""
        result["image_detail"] = urljoin(BASE_URL, img_src)
    else:
        result["image_detail"] = ""

    # Find all "process-step-content" blocks if present
    blocks = soup.select(".process-step-content")
    if blocks:
        for block in blocks:
            # label
            label_tag = block.find(["h4", "h3", "strong"])
            val_tag = None
            if label_tag:
                label = label_tag.get_text(strip=True)
                # the value(s) are usually in <p class="mb-0"> following the label inside the block
                val_tag = label_tag.find_next_sibling("p")
                if val_tag:
                    # sometimes there are nested <p> tags; join them
                    val_text = " ".join([p.get_text(separator=" ", strip=True) for p in block.find_all("p") if p.get_text(strip=True)])
                else:
                    # fallback: take any text nodes in the block excluding the label text
                    texts = []
                    for node in block.find_all(text=True):
                        txt = node.strip()
                        if not txt:
                            continue
                        if label in txt:
                            # avoid the label text itself
                            continue
                        texts.append(txt)
                    val_text = " ".join(texts).strip()
                result[label] = val_text
    else:
        # fallback: parse rows of <div class="row"> where label & value are in columns
        rows = soup.select("div.card-body .row > div")
        # We'll try to detect repeated pattern label->value pairs.
        for i in range(0, len(rows), 2):
            try:
                label_elem = rows[i].select_one("h4")
                val_elem = rows[i+1]
                if label_elem:
                    label = label_elem.get_text(strip=True)
                    val_text = val_elem.get_text(separator=" ", strip=True)
                    result[label] = val_text
            except Exception:
                continue

    # Additionally capture main textual description if present (long description)
    desc_container = soup.select_one("div.process-step-content p")
    if desc_container:
        # extract the first significant long paragraph that likely is the description (but avoid duplicates)
        long_desc = desc_container.get_text(separator="\n", strip=True)
        # only set if more than short strings
        if long_desc and len(long_desc) > 10:
            result.setdefault("LongDescription", long_desc)

    # As a last resort collect all <p> under article and join them as fallback_text
    if not result.get("LongDescription"):
        article_ps = soup.select("article.post p")
        joined = " | ".join([p.get_text(separator=" ", strip=True) for p in article_ps if p.get_text(strip=True)])
        if joined:
            result.setdefault("FallbackAllP", joined)

    # Normalize keys: strip whitespace
    normalized = {}
    for k, v in result.items():
        nk = k.strip()
        normalized[nk] = v.strip() if isinstance(v, str) else v
    return normalized


def scrape_listing_and_details(listing_url: str) -> List[Dict]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    listing_soup = get_soup(listing_url, session)
    if listing_soup is None:
        raise RuntimeError("Failed to fetch listing page")

    cards = parse_listing_cards(listing_soup)
    results = []
    for card in tqdm(cards, desc="Startups", unit="item"):
        row = {
            "listing_title": card.get("title"),
            "short_description": card.get("short_desc"),
            "listing_image": card.get("image_url"),
            "detail_url": card.get("detail_url"),
        }

        detail_url = card.get("detail_url")
        if detail_url:
            # be polite
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            detail_soup = get_soup(detail_url, session)
            if detail_soup:
                fields = extract_detail_fields(detail_soup)
                # flatten fields into row (with safe keys)
                for k, v in fields.items():
                    # Make column name safe by removing newlines and excessive whitespace
                    col = k.replace("\n", " ").strip()
                    # Avoid overwriting primary keys
                    if col in row:
                        col = "detail_" + col
                    row[col] = v
            else:
                logging.warning("Could not fetch detail page for %s", detail_url)
        else:
            logging.warning("No detail URL for card: %s", card)

        results.append(row)
    return results


def save_outputs(rows: List[Dict], csv_path: str, xlsx_path: str) -> None:
    if not rows:
        logging.warning("No rows to save")
        return
    df = pd.DataFrame(rows)
    # sort columns consistently (title, short_description, detail_url first)
    cols = list(df.columns)
    preferred = ["listing_title", "short_description", "listing_image", "detail_url"]
    ordered = [c for c in preferred if c in cols] + [c for c in cols if c not in preferred]
    df = df[ordered]
    # Save CSV with utf-8 (BOM included for Excel compatibility)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    # Save Excel
    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    logging.info("Saved %d rows to %s and %s", len(df), csv_path, xlsx_path)


def main():
    logging.info("Starting scraper for listing: %s", LISTING_URL)
    rows = scrape_listing_and_details(LISTING_URL)
    save_outputs(rows, OUTPUT_CSV, OUTPUT_XLSX)
    logging.info("Done. Total startups scraped: %d", len(rows))


if __name__ == "__main__":
    main()
