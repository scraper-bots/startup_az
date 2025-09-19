"""
scrape_startup_az_linkwise.py

Crawl startup.az using link-wise pagination:
- Detect max pages via pagination links when available
- Otherwise follow the "next" link until no more
- Extract listing cards and visit detail pages
- Normalize fields and save to CSV and XLSX

Dependencies:
    pip install requests beautifulsoup4 pandas openpyxl lxml tqdm
"""

import re
import time
import random
import logging
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

# ------------- Configuration -------------
BASE_URL = "https://www.startup.az"
LISTING_PATH = "/startup.html"
PER_PAGE = 12
USER_AGENT = "Mozilla/5.0 (compatible; StartupAZScraper/1.0; +https://example.com/bot)"
TIMEOUT = 15
SLEEP_MIN = 0.6
SLEEP_MAX = 1.2
MAX_RETRIES = 3
OUTPUT_CSV = "startup_az_linkwise.csv"
OUTPUT_XLSX = "startup_az_linkwise.xlsx"
MAX_SAFE_PAGES = 300  # absolute safety limit to prevent runaway loops
# -----------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Azerbaijani label -> English field mapping (extend as needed)
FIELD_MAPPING = {
    "Başlıq": "Title",
    "Seqment": "Segment",
    "Təsvir": "Description",
    "Təsisçilər": "Founders",
    "Komanda": "Team",
    "Komanda axtar": "LookingForTeam",
    "Yaranma tarixi": "FoundedDate",
    "Status": "Status",
    "İnvestisiyalar": "Investments",
    "Proqramlarda iştirak": "Programs",
    "Əlaqə": "ContactPhone",
    "Email": "Email",
    "Veb": "Website",
    "Startap Şəhadətnaməsi": "Certification",
    "Şəkillər": "Images",
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
PHONE_RE = re.compile(r"(?:\+?\d{1,3}[\s-]?)?(?:\(?\d{2,3}\)?[\s-]?)?\d{2,3}[\s-]?\d{2,3}[\s-]?\d{2,4}", re.I)


def session_get(session: requests.Session, url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """GET with retries and error handling."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            if resp.encoding is None:
                resp.encoding = resp.apparent_encoding
            return resp
        except Exception as e:
            logging.warning("GET %s failed (attempt %d/%d): %s", url, attempt, retries, e)
            time.sleep(1 + attempt * 0.5)
    logging.error("Giving up on %s after %d attempts", url, retries)
    return None


def get_soup(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    resp = session_get(session, url)
    if not resp:
        return None
    return BeautifulSoup(resp.text, "lxml")


def parse_listing_cards(soup: BeautifulSoup) -> List[Dict]:
    """Extract cards on a listing page."""
    cards = []
    anchors = soup.select("div.startup-block a.card")
    if not anchors:
        anchors = soup.select("a.card.mb-4, a.card")
    for a in anchors:
        href = a.get("href", "").strip()
        detail_url = urljoin(BASE_URL, href) if href else None
        img_tag = a.find("img", class_="card-img-top")
        img_src = ""
        if img_tag:
            img_src = img_tag.get("src") or img_tag.get("data-src") or ""
            img_src = urljoin(BASE_URL, img_src) if img_src else ""
        title_tag = a.select_one(".card-body .card-title")
        title = title_tag.get_text(strip=True) if title_tag else None
        desc_tag = a.select_one(".card-body .card-text")
        short_desc = desc_tag.get_text(separator=" ", strip=True) if desc_tag else None
        cards.append({
            "listing_title": title,
            "short_description": short_desc,
            "listing_image": img_src,
            "detail_url": detail_url
        })
    return cards


def map_label(label: str) -> str:
    label = label.strip()
    if label in FIELD_MAPPING:
        return FIELD_MAPPING[label]
    # fallback: sanitize label (remove punctuation) and use as-is
    safe = re.sub(r"[^\w\s]", "", label)
    return safe or label


def extract_detail_fields(soup: BeautifulSoup, detail_url: str) -> Dict[str, str]:
    """Extract structured fields from detail page; return normalized dict."""
    data: Dict[str, str] = {}

    # Collect images found in article/post
    img_tags = soup.select("article.post img, .post-image img, .blog-single-post img")
    images = []
    for img in img_tags:
        src = img.get("src") or img.get("data-src") or ""
        if src:
            images.append(urljoin(BASE_URL, src))
    if images:
        # deduplicate preserving order
        seen = set()
        uniq = []
        for u in images:
            if u not in seen:
                seen.add(u)
                uniq.append(u)
        data["Images"] = ";".join(uniq)

    # Parse labeled blocks .process-step-content
    blocks = soup.select(".process-step-content")
    if blocks:
        for block in blocks:
            label_tag = block.find(["h4", "h3", "strong"])
            if not label_tag:
                continue
            label = label_tag.get_text(strip=True)
            p_texts = [p.get_text(separator=" ", strip=True) for p in block.find_all("p") if p.get_text(strip=True)]
            value = " | ".join(p_texts).strip() if p_texts else ""
            key = map_label(label)
            if value:
                data[key] = value

    # fallback: rows of cards
    if not blocks:
        candidate_cols = soup.select("div.card-body .row > div")
        for i, col in enumerate(candidate_cols):
            h4 = col.find("h4")
            if not h4:
                continue
            label = h4.get_text(strip=True)
            # try get p in same col
            p = col.find("p")
            value = p.get_text(separator=" ", strip=True) if p else ""
            if not value:
                # maybe next column holds value
                try:
                    nxt = candidate_cols[i+1]
                    value = nxt.get_text(separator=" ", strip=True)
                except Exception:
                    value = ""
            if value:
                data[map_label(label)] = value

    # description fallback: first long paragraph in article
    long_ps = [p.get_text(separator=" ", strip=True) for p in soup.select("article.post p, .post p") if len(p.get_text(strip=True)) > 30]
    if long_ps and "Description" not in data:
        data["Description"] = long_ps[0]

    # Extract emails, phones, websites from page text
    text_blob = " ".join(s.strip() for s in soup.stripped_strings)
    emails = set(EMAIL_RE.findall(text_blob))
    if emails and "Email" not in data:
        data["Email"] = ";".join(sorted(emails))
    phones = set()
    for m in PHONE_RE.findall(text_blob):
        ph = re.sub(r"\s+", " ", m).strip()
        digits = re.sub(r"\D", "", ph)
        if len(digits) >= 6:
            phones.add(ph)
    if phones and "ContactPhone" not in data:
        data["ContactPhone"] = ";".join(sorted(phones))
    # websites from anchors
    anchors = soup.select("a")
    websites = set()
    for a in anchors:
        href = a.get("href", "").strip()
        if href.startswith("http://") or href.startswith("https://"):
            websites.add(href)
    if websites and "Website" not in data:
        filtered = [w for w in websites if not re.search(r"(facebook|fb\.com|instagram|twitter|linkedin|youtube|tiktok)", w, re.I)]
        pick = filtered[0] if filtered else next(iter(websites))
        data["Website"] = pick

    # Title fallback (H1/H2 or slug)
    if "Title" not in data:
        h1 = soup.find(["h1", "h2"])
        if h1 and h1.get_text(strip=True):
            data["Title"] = h1.get_text(strip=True)
        else:
            path = urlparse(detail_url).path
            slug = path.rstrip("/").split("/")[-1]
            data["Title"] = slug

    # trim
    for k, v in list(data.items()):
        if isinstance(v, str):
            data[k] = v.strip()
    return data


def detect_max_page_from_pagination(soup: BeautifulSoup) -> Optional[int]:
    """
    Try to detect the max page from pagination anchors. Consider:
    - href ?page=N
    - data-page (zero-indexed)
    - visible numeric link text
    """
    pages = set()
    for a in soup.select("ul.pagination a, ul.pagination li a, li.page-item a"):
        # data-page
        dp = a.get("data-page")
        if dp and dp.isdigit():
            # site often uses zero-indexed data-page -> convert
            pages.add(int(dp) + 1)
        # visible text
        txt = a.get_text(strip=True)
        if txt.isdigit():
            pages.add(int(txt))
        # href param
        href = a.get("href", "")
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            pages.add(int(m.group(1)))
    if pages:
        return max(pages)
    return None


def find_next_link(soup: BeautifulSoup) -> Optional[str]:
    """
    Return the href (possibly relative) for the 'next' pagination link.
    Looks for:
     - a[rel="next"]
     - anchors with aria-label/title mentioning next
     - anchors containing 'angle-right' icon markup or » / › characters
    """
    # rel="next"
    a = soup.select_one('ul.pagination a[rel="next"], a[rel="next"]')
    if a and a.get("href"):
        return a["href"]
    # aria-label or title containing Next
    for sel in ['ul.pagination a[aria-label*="Next"]', 'ul.pagination a[aria-label*="next"]',
                'a[aria-label*="Next"]', 'a[aria-label*="next"]', 'a[title*="Next"]', 'a[title*="next"]']:
        a = soup.select_one(sel)
        if a and a.get("href"):
            return a["href"]
    # anchors that include angle-right icon or entities
    anchors = soup.select("ul.pagination a, li.page-item a")
    for a in anchors:
        inner = (a.decode_contents() or "").lower()
        if "angle-right" in inner or "&raquo;" in inner or "›" in inner or "→" in inner:
            href = a.get("href")
            if href:
                return href
    return None



def crawl_linkwise_follow_next():
    """
    Robust link-wise crawler that always follows the 'next' pagination link
    starting from page=1, rather than trusting the visible numeric page links.

    It will:
      - Start at page=1
      - Parse cards on the page
      - Find the 'next' link (rel=next, aria-label, or anchor showing angle-right)
      - Follow that link, repeating until no 'next' link or MAX_SAFE_PAGES reached
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})

    start_url = f"{BASE_URL}{LISTING_PATH}?page=1&per-page={PER_PAGE}"
    logging.info("Starting link-wise crawl (follow-next) at %s", start_url)

    current_url = start_url
    pages_crawled = 0
    all_rows = []
    seen_detail = set()

    while current_url and pages_crawled < MAX_SAFE_PAGES:
        logging.info("Fetching listing page: %s", current_url)
        listing_soup = get_soup(session, current_url)
        if not listing_soup:
            logging.warning("Failed to fetch %s — stopping crawl.", current_url)
            break

        cards = parse_listing_cards(listing_soup)
        logging.info("Parsed %d listing cards", len(cards))
        if not cards:
            logging.info("No cards found — stopping crawl.")
            break

        # Process each card on the current page
        for card in tqdm(cards, desc=f"Page {pages_crawled+1}", unit="card"):
            row = {
                "listing_title": card.get("listing_title"),
                "short_description": card.get("short_description"),
                "listing_image": card.get("listing_image"),
                "detail_url": card.get("detail_url"),
            }
            detail_url = card.get("detail_url")
            if detail_url and detail_url not in seen_detail:
                seen_detail.add(detail_url)
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                detail_soup = get_soup(session, detail_url)
                if detail_soup:
                    fields = extract_detail_fields(detail_soup, detail_url)
                    # prefer Title from detail
                    if fields.get("Title"):
                        row["Title"] = fields.pop("Title")
                    for k, v in fields.items():
                        if k not in row or not row.get(k):
                            row[k] = v
                        else:
                            if str(row.get(k)) != str(v):
                                row[f"detail_{k}"] = v
            all_rows.append(row)

        pages_crawled += 1

        # Find next link on current listing page
        nxt = find_next_link(listing_soup)
        if not nxt:
            logging.info("No next link found on page %d — crawl finished.", pages_crawled)
            break

        # Ensure per-page param stays present
        parsed = urlparse(nxt)
        if "per-page" not in parsed.query:
            sep = "&" if "?" in nxt else "?"
            nxt = f"{nxt}{sep}per-page={PER_PAGE}"

        # Normalize next to absolute URL
        current_url = urljoin(BASE_URL, nxt)

        # Polite sleep before fetching the next listing page
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    logging.info("Link-wise follow-next crawl finished: crawled %d pages, %d items", pages_crawled, len(all_rows))
    return all_rows


def save_results(rows: List[Dict], csv_path: str, xlsx_path: str):
    if not rows:
        logging.warning("No rows to save")
        return
    df = pd.DataFrame(rows)
    preferred = ["Title", "listing_title", "short_description", "Description", "listing_image", "Images", "detail_url", "Website", "Email", "ContactPhone"]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    logging.info("Saved %d rows to %s and %s", len(df), csv_path, xlsx_path)


def main():
    logging.info("Starting link-wise crawl for startup.az")
    rows = crawl_linkwise_follow_next()
    save_results(rows, OUTPUT_CSV, OUTPUT_XLSX)
    logging.info("Crawl complete. Total startups scraped: %d", len(rows))


if __name__ == "__main__":
    main()
