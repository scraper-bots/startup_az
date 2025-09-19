"""
scrape_startup_az_all_pages.py

Crawls startup.az listing pages, follows each startup detail page,
extracts structured fields and images, normalizes Azerbaijani labels
to English column names, and saves to CSV and XLSX.

Designed to crawl multiple pages (auto-detect total pages).
Be polite: respect robots.txt / terms of service and avoid heavy load.

Requirements:
    pip install requests beautifulsoup4 pandas openpyxl lxml tqdm
"""

from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
from tqdm import tqdm
import logging

# ---------- Configuration ----------
BASE_URL = "https://www.startup.az"
LISTING_PATH = "/startup.html"
# default per-page param used by the site; we will set per-page=12 as example
PER_PAGE = 12
USER_AGENT = "Mozilla/5.0 (compatible; StartupAZScraper/1.0; +https://example.com/bot)"
TIMEOUT = 15
SLEEP_MIN = 0.6
SLEEP_MAX = 1.2
MAX_RETRIES = 3
OUTPUT_CSV = "startup_az_all_pages.csv"
OUTPUT_XLSX = "startup_az_all_pages.xlsx"
# If pagination detection fails, fallback to at most this many pages to avoid runaway scraping
FALLBACK_MAX_PAGES = 20
# -----------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Map Azerbaijani field labels (approximate) to normalized English keys
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
    # possible variants (normalized):
    "Şəkillər": "Images",
    "Təsvirə": "Description",
}

# regex helpers
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
PHONE_RE = re.compile(r"(?:\+?\d{1,3}[\s-]?)?(?:\(?\d{2,3}\)?[\s-]?)?\d{2,3}[\s-]?\d{2,3}[\s-]?\d{2,4}", re.I)
URL_SCHEME_RE = re.compile(r"^https?://", re.I)


def safe_request(session: requests.Session, url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """GET with retries and basic error handling."""
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            # prefer apparent_encoding if server doesn't specify
            if r.encoding is None:
                r.encoding = r.apparent_encoding
            return r
        except Exception as e:
            logging.warning("Request failed (%s) attempt %d/%d: %s", url, attempt, retries, e)
            time.sleep(1 + attempt * 0.5)
    logging.error("Giving up on %s after %d attempts", url, retries)
    return None


def get_soup(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    resp = safe_request(session, url)
    if not resp:
        return None
    return BeautifulSoup(resp.text, "lxml")


def detect_total_pages(soup: BeautifulSoup) -> int:
    """
    Inspect pagination and return total pages.
    Looks for pagination links like /startup.html?page=16&per-page=12
    """
    try:
        pag_links = soup.select("ul.pagination a.page-link")
        pages = set()
        for a in pag_links:
            href = a.get("href", "")
            if "page=" in href:
                m = re.search(r"page=(\d+)", href)
                if m:
                    pages.add(int(m.group(1)))
            else:
                # sometimes the link text is page number
                txt = a.get_text(strip=True)
                if txt.isdigit():
                    pages.add(int(txt))
        if pages:
            max_page = max(pages)
            logging.info("Detected %d total pages from pagination", max_page)
            return max_page
    except Exception:
        logging.debug("Pagination detection failed", exc_info=True)
    logging.warning("Failed to detect total pages; using fallback of %d", FALLBACK_MAX_PAGES)
    return FALLBACK_MAX_PAGES


def parse_listing_cards(soup: BeautifulSoup) -> List[Dict]:
    """
    Parse listing cards on a listing page. Returns list of dicts with:
    - listing_title, short_description, listing_image, detail_url
    """
    cards = []
    # cards are anchors with class card inside .startup-block (from provided HTML)
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
            if img_src:
                img_src = urljoin(BASE_URL, img_src)

        title_tag = a.select_one(".card-body .card-title")
        title = title_tag.get_text(strip=True) if title_tag else None

        desc_tag = a.select_one(".card-body .card-text")
        short_desc = desc_tag.get_text(separator=" ", strip=True) if desc_tag else None

        cards.append({
            "listing_title": title,
            "short_description": short_desc,
            "listing_image": img_src,
            "detail_url": detail_url,
        })
    logging.info("Parsed %d listing cards", len(cards))
    return cards


def map_label_to_key(label: str) -> str:
    """Map Azerbaijani label to English key; fallback to clean label."""
    lab = label.strip()
    if lab in FIELD_MAPPING:
        return FIELD_MAPPING[lab]
    # try simpler normalization (remove punctuation, lowercase)
    lab_norm = re.sub(r"[^A-Za-z0-9əəöçğşİıƏÖÇĞŞÜü\s]", "", lab)
    # crude matching: check startswith of known keys
    for az_label, en_key in FIELD_MAPPING.items():
        if lab.startswith(az_label[:4]) or az_label.startswith(lab[:4]):
            return en_key
    return lab_norm or label


def extract_detail_fields(soup: BeautifulSoup, detail_url: str) -> Dict[str, str]:
    """
    Extract structured fields from a detail page Soup.
    Returns a dictionary with normalized keys.
    """
    data: Dict[str, str] = {}

    # collect all images inside article.post or the entire page as a fallback
    img_tags = soup.select("article.post img, .post-image img, .blog-single-post img")
    images = []
    for img in img_tags:
        src = img.get("src") or img.get("data-src") or ""
        if src:
            images.append(urljoin(BASE_URL, src))
    if images:
        # unique preserve order
        seen = set()
        uniq_images = []
        for u in images:
            if u not in seen:
                seen.add(u)
                uniq_images.append(u)
        data["Images"] = ";".join(uniq_images)

    # parse labeled blocks like <div class="process-step-content"> <h4>Label</h4> <p>Value</p>
    blocks = soup.select(".process-step-content")
    if blocks:
        for block in blocks:
            label_tag = block.find(["h4", "h3", "strong"])
            if not label_tag:
                # sometimes label is in strong or bold elements
                continue
            label = label_tag.get_text(strip=True)
            # the value is usually subsequent <p class="mb-0"> elements. Join all <p> inside block.
            p_texts = [p.get_text(separator=" ", strip=True) for p in block.find_all("p") if p.get_text(strip=True)]
            if p_texts:
                value = " | ".join(p_texts).strip()
            else:
                # fallback: any sibling text nodes
                value = label_tag.find_next_sibling(text=True)
                value = value.strip() if value else ""
            key = map_label_to_key(label)
            # avoid empty assignments
            if value:
                data[key] = value

    # If no structured blocks found, try to find "rows" of label/value pairs (common fallback)
    if not blocks:
        candidate_rows = soup.select("div.card-body .row > div")
        # heuristics: find pairs where first has an h4 and second has p
        for i in range(0, len(candidate_rows), 1):
            col = candidate_rows[i]
            h4 = col.find("h4")
            if h4:
                label = h4.get_text(strip=True)
                # value might be next sibling column or the p in same column
                next_p = col.find("p")
                value = ""
                if next_p:
                    value = next_p.get_text(separator=" ", strip=True)
                else:
                    # try next sibling column if exists
                    try:
                        nxt = candidate_rows[i+1]
                        value = nxt.get_text(separator=" ", strip=True)
                    except Exception:
                        value = ""
                if value:
                    data[map_label_to_key(label)] = value

    # Extract long description -- prefer explicit Təsvir / Description
    desc_candidates = []
    # Look for main article description paragraphs (first large paragraph)
    article_ps = soup.select("article.post p, .post .card-body p")
    for p in article_ps:
        txt = p.get_text(separator=" ", strip=True)
        if txt and len(txt) > 20:
            desc_candidates.append(txt)
    # If Description key missing but we have candidates, set the first one
    if "Description" not in data and desc_candidates:
        data["Description"] = desc_candidates[0]

    # Extract contact info from anywhere on the detail page as fallback
    text_blob = " ".join([t.strip() for t in soup.stripped_strings])

    # Emails
    emails = set(EMAIL_RE.findall(text_blob))
    if emails:
        # preserve any existing Email if set; otherwise use first
        if "Email" not in data:
            data["Email"] = ";".join(sorted(emails))

    # Phones — look for phone-like strings; then filter short nonsense
    phones = set()
    for m in PHONE_RE.findall(text_blob):
        # normalize whitespace and dashes
        ph = re.sub(r"\s+", " ", m).strip()
        # avoid capturing short numbers (like '2021' or '50' etc)
        digits = re.sub(r"\D", "", ph)
        if len(digits) >= 6:
            phones.add(ph)
    if phones and "ContactPhone" not in data:
        data["ContactPhone"] = ";".join(sorted(phones))

    # Websites: look for anchors with href or textual urls
    anchors = soup.select("a")
    websites = set()
    for a in anchors:
        href = a.get("href", "").strip()
        if href and (href.startswith("http://") or href.startswith("https://")):
            websites.add(href)
    # If Website field missing and we found at least one (exclude social media if possible)
    if websites and "Website" not in data:
        # prefer ones that are not facebook/twitter/youtube etc
        filtered = [w for w in websites if not re.search(r"(facebook|fb\.com|instagram|linkedin|twitter|youtube|tiktok)", w, re.I)]
        pick = filtered[0] if filtered else next(iter(websites))
        data["Website"] = pick

    # Ensure Title exists: sometimes label exists; otherwise fallback to page H1 or listing title
    if "Title" not in data:
        # try H1 or page title
        h1 = soup.find(["h1", "h2"])
        if h1 and h1.get_text(strip=True):
            data["Title"] = h1.get_text(strip=True)
        else:
            # last fallback: from URL slug
            path = urlparse(detail_url).path
            slug = path.rstrip("/").split("/")[-1]
            data["Title"] = slug

    # Trim values
    for k, v in list(data.items()):
        if isinstance(v, str):
            data[k] = v.strip()
    return data


def crawl_all_pages():
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    # Start from page 1 to detect pagination
    first_listing_url = f"{BASE_URL}{LISTING_PATH}?page=1&per-page={PER_PAGE}"
    logging.info("Fetching first listing page to detect pagination: %s", first_listing_url)
    soup = get_soup(session, first_listing_url)
    if not soup:
        raise RuntimeError("Could not fetch first listing page")

    total_pages = detect_total_pages(soup)
    # limit to sensible maximum
    total_pages = min(total_pages, FALLBACK_MAX_PAGES)
    logging.info("Will crawl %d pages", total_pages)

    all_rows: List[Dict] = []
    seen_detail_urls = set()

    for page in range(1, total_pages + 1):
        listing_url = f"{BASE_URL}{LISTING_PATH}?page={page}&per-page={PER_PAGE}"
        logging.info("Processing listing page %d/%d: %s", page, total_pages, listing_url)
        page_soup = get_soup(session, listing_url)
        if page_soup is None:
            logging.warning("Skipping page %d due to fetch failure", page)
            continue

        cards = parse_listing_cards(page_soup)
        # iterate cards
        for card in tqdm(cards, desc=f"Page {page}", unit="card"):
            row = {
                "listing_title": card.get("listing_title"),
                "short_description": card.get("short_description"),
                "listing_image": card.get("listing_image"),
                "detail_url": card.get("detail_url"),
            }
            detail_url = card.get("detail_url")
            if not detail_url:
                logging.warning("Card has no detail URL; skipping: %s", row.get("listing_title"))
                all_rows.append(row)
                continue

            # deduplicate detail pages
            if detail_url in seen_detail_urls:
                logging.debug("Already scraped detail URL: %s", detail_url)
                all_rows.append(row)
                continue
            seen_detail_urls.add(detail_url)

            # polite sleep
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            detail_soup = get_soup(session, detail_url)
            if not detail_soup:
                logging.warning("Failed to fetch detail page: %s", detail_url)
                all_rows.append(row)
                continue

            fields = extract_detail_fields(detail_soup, detail_url)
            # prefer detail Title over listing_title if present
            if fields.get("Title"):
                row["Title"] = fields.pop("Title")
            # merge remaining fields (do not overwrite listing keys)
            for k, v in fields.items():
                if k in row and not row.get(k):  # if column exists but empty, fill it
                    row[k] = v
                elif k not in row:
                    row[k] = v
                else:
                    # if both exist and differ, keep both: detail_{k}
                    if row.get(k) != v:
                        row[f"detail_{k}"] = v
            all_rows.append(row)
    return all_rows


def save_results(rows: List[Dict], csv_path: str, xlsx_path: str):
    if not rows:
        logging.warning("No rows to save")
        return
    df = pd.DataFrame(rows)
    # reorder columns: prefer listing fields first then normalized keys sorted
    preferred = ["Title", "listing_title", "short_description", "Description", "listing_image", "Images", "detail_url", "Website", "Email", "ContactPhone", "Founders", "Team", "LookingForTeam", "FoundedDate", "Status", "Investments", "Programs", "Certification"]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    # Save CSV with BOM so Excel can open UTF-8
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    logging.info("Saved %d rows to %s and %s", len(df), csv_path, xlsx_path)


def main():
    logging.info("Starting full crawl of startup.az")
    rows = crawl_all_pages()
    save_results(rows, OUTPUT_CSV, OUTPUT_XLSX)
    logging.info("Finished. Total startups scraped: %d", len(rows))


if __name__ == "__main__":
    main()
