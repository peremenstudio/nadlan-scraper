import os
import time
import datetime
import traceback

import pandas as pd
import streamlit as st
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ── Config ────────────────────────────────────────────────────────────────────
MAX_PAGES      = 99
WAIT_SECONDS   = 0.8
COORDS_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "coordinates.csv")

HEADERS = [
    "Serial Number", "Address", "Area (sqm)", "Transaction Date",
    "Transaction Price", "Parcel", "Property Type", "Rooms", "Floor", "Change Trend",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Loading coordinates…")
def load_coords():
    if not os.path.exists(COORDS_FILE):
        return None
    df = pd.read_csv(COORDS_FILE)
    df.columns = df.columns.str.strip().str.lower()
    return df.rename(columns={"gush_helka": "Gush_Helka", "x": "X", "y": "Y"})[
        ["Gush_Helka", "X", "Y"]
    ]


def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")

    # Streamlit Cloud (Ubuntu) paths
    for binary in ["/usr/bin/chromium", "/usr/bin/chromium-browser"]:
        if os.path.exists(binary):
            opts.binary_location = binary
            break

    for driver_path in ["/usr/bin/chromedriver", "/usr/lib/chromium/chromedriver",
                        "/usr/lib/chromium-browser/chromedriver"]:
        if os.path.exists(driver_path):
            return webdriver.Chrome(service=Service(driver_path), options=opts)

    # Local fallback — Selenium 4.6+ manager or webdriver-manager
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    except Exception:
        return webdriver.Chrome(options=opts)


def search_location(driver, location: str, log):
    """Open nadlan.gov.il and search for the given location."""
    driver.get("https://www.nadlan.gov.il/")

    # Wait for the react-autosuggest input (id="myInput2")
    log("Waiting for search box…")
    search_input = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "myInput2"))
    )

    search_input.clear()
    search_input.send_keys(location)
    log(f"Typed: {location}")

    # Wait for react-autosuggest suggestions to appear
    suggestion = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "li.react-autosuggest__suggestion"))
    )
    suggestion.click()
    log("Suggestion selected.")

    # Wait for results table
    log("Waiting for results table…")
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "dealsTable"))
    )


def scrape_all_pages(driver, max_pages: int, log):
    rows_data = []
    page = 1

    while page <= max_pages:
        table = driver.find_element(By.ID, "dealsTable")
        tbody = table.find_element(By.TAG_NAME, "tbody")
        rows  = tbody.find_elements(By.TAG_NAME, "tr")

        page_rows = 0
        for row in rows:
            cells  = row.find_elements(By.TAG_NAME, "td")
            values = [c.text.strip() for c in cells[: len(HEADERS)]]
            if any(values):
                rows_data.append(values)
                page_rows += 1

        log(f"Page {page} — {page_rows} rows  (total: {len(rows_data)})")

        # Check for next button
        try:
            next_btn = driver.find_element(By.ID, "next")
        except Exception:
            break

        classes  = next_btn.get_attribute("class") or ""
        disabled = next_btn.get_attribute("disabled")
        style    = next_btn.get_attribute("style") or ""

        if (
            not next_btn.is_enabled()
            or "disabled" in classes.lower()
            or disabled is not None
            or "display:none" in style.replace(" ", "")
        ):
            break

        try:
            prev_first = rows[0].find_elements(By.TAG_NAME, "td")[0].text.strip()
        except Exception:
            prev_first = ""

        driver.execute_script("arguments[0].scrollIntoView();", next_btn)
        driver.execute_script("arguments[0].click();", next_btn)
        time.sleep(WAIT_SECONDS)

        WebDriverWait(driver, 15).until(
            lambda d: (
                d.find_element(By.ID, "dealsTable")
                 .find_element(By.TAG_NAME, "tbody")
                 .find_elements(By.TAG_NAME, "tr")[0]
                 .find_elements(By.TAG_NAME, "td")[0]
                 .text.strip()
            ) != prev_first
        )

        page += 1

    return rows_data, page


def process(rows_data: list) -> pd.DataFrame:
    df = pd.DataFrame(rows_data, columns=HEADERS)

    df = df.drop(columns=["Serial Number"], errors="ignore")

    if "Transaction Price" in df.columns:
        df["Transaction Price"] = (
            df["Transaction Price"].astype(str)
            .str.replace("\u20aa", "", regex=False)
            .str.replace(",",      "", regex=False)
            .str.strip()
        )
        df["Transaction Price"] = pd.to_numeric(df["Transaction Price"], errors="coerce")

    if "Transaction Date" in df.columns:
        df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], errors="coerce")

    if "Parcel" in df.columns:
        parts = df["Parcel"].str.split("-", expand=True)
        df["_p1"] = pd.to_numeric(parts[0], errors="coerce")
        df["_p2"] = pd.to_numeric(parts[1], errors="coerce")
        df["Gush_Helka"] = (
            df["_p1"].astype("Int64").astype(str) + "/"
            + df["_p2"].astype("Int64").astype(str)
        )

    if "Change Trend" in df.columns:
        ct = (
            df["Change Trend"].astype(str)
            .str.replace("green arrow up",   "", regex=False)
            .str.replace("tooltip 16 copy",  "", regex=False)
        )
        df["Change Trend"]        = ct
        df["Percentage"]          = pd.to_numeric(
            ct.str[:6].str.replace("%", "").str.strip(), errors="coerce"
        )
        df["Number_of_years"]     = pd.to_numeric(
            ct.str[-9:].str.replace(r"[\u05e9\u05e0\u05d9\u05dd\u05d1]", "", regex=True).str.strip(),
            errors="coerce",
        )
        df["Percentage_Per_Year"] = df["Percentage"] / df["Number_of_years"]

    if "Area (sqm)" in df.columns:
        df["Area (sqm)"]    = pd.to_numeric(df["Area (sqm)"], errors="coerce")
        df["Price_per_sqm"] = (
            df["Transaction Price"] / df["Area (sqm)"]
        ).round(0).astype("Int64")

    return df.drop(columns=[c for c in ["_p1", "_p2"] if c in df.columns])


def merge_coords(df: pd.DataFrame, coords_df) -> pd.DataFrame:
    if coords_df is None or "Gush_Helka" not in df.columns:
        return df
    return pd.merge(df, coords_df, on="Gush_Helka", how="left")


# ── UI ────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Nadlan Scraper", page_icon="🏠", layout="centered")

st.title("🏠 Nadlan.gov.il Scraper")
st.caption("Automated real estate data extractor")

with st.form("search_form"):
    location = st.text_input(
        "Location (Hebrew)",
        placeholder="e.g.  רמת אביב",
        help="Type the location exactly as it appears on nadlan.gov.il",
    )
    max_pages = st.slider("Max pages to scrape", 1, 99, 99)
    submitted = st.form_submit_button("▶  Start Scraping", type="primary")

if submitted and location.strip():
    coords_df = load_coords()
    driver    = None

    try:
        with st.status("Scraping in progress…", expanded=True) as status:

            def log(msg: str):
                st.write(msg)

            log("Opening headless browser…")
            driver = get_driver()

            log(f"Searching for:  {location}")
            search_location(driver, location.strip(), log)

            try:
                loc_el        = driver.find_element(By.CLASS_NAME, "locationLink")
                location_name = loc_el.text.strip().replace(" ", "_")
            except Exception:
                location_name = location.strip().replace(" ", "_")

            log(f"Location confirmed: {location_name}")
            log("Scraping pages…")
            rows_data, pages = scrape_all_pages(driver, max_pages, log)

            log(f"Processing {len(rows_data)} rows…")
            df = process(rows_data)

            log("Merging coordinates…")
            df = merge_coords(df, coords_df)

            status.update(
                label=f"Done — {len(df):,} rows from {pages} page(s)",
                state="complete",
            )

        # ── Results ───────────────────────────────────────────────────────────
        st.success(f"**{len(df):,} rows** scraped from **{pages} page(s)**")
        st.dataframe(df, use_container_width=True)

        now      = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"{location_name}_{now}.csv"
        csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

        st.download_button(
            "⬇  Download CSV",
            data=csv_bytes,
            file_name=filename,
            mime="text/csv",
            type="primary",
        )

    except Exception as exc:
        st.error(f"**Error:** {exc}")
        with st.expander("Details"):
            st.code(traceback.format_exc())

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
