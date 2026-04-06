import os
import sys
import subprocess
import datetime
import traceback

import pandas as pd
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
MAX_PAGES   = 99
COORDS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "coordinates.csv")

HEADERS = [
    "Serial Number", "Address", "Area (sqm)", "Transaction Date",
    "Transaction Price", "Parcel", "Property Type", "Rooms", "Floor", "Change Trend",
]

# ── Install Playwright browser once per container lifetime ────────────────────
@st.cache_resource(show_spinner="Installing browser (first run only)…")
def _install_browser():
    result = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Browser install failed:\n{result.stderr}")
    return True

# ── Coordinates ───────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner="Loading coordinates…")
def load_coords():
    if not os.path.exists(COORDS_FILE):
        return None
    df = pd.read_csv(COORDS_FILE)
    df.columns = df.columns.str.strip().str.lower()
    return df.rename(columns={"gush_helka": "Gush_Helka", "x": "X", "y": "Y"})[
        ["Gush_Helka", "X", "Y"]
    ]

# ── Scraping (Playwright) ─────────────────────────────────────────────────────
def scrape(location: str, max_pages: int, log) -> tuple[list, int, str]:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )

        # ── Navigate ──────────────────────────────────────────────────────────
        log("Opening nadlan.gov.il…")
        page.goto("https://www.nadlan.gov.il/", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=30_000)

        # ── Search ────────────────────────────────────────────────────────────
        log("Waiting for search box…")
        page.wait_for_selector("#myInput2", timeout=30_000)
        page.click("#myInput2")
        page.type("#myInput2", location, delay=80)   # slow typing triggers autosuggest
        log(f"Typed: {location}")

        # ── Pick first suggestion ─────────────────────────────────────────────
        try:
            page.wait_for_selector("li.react-autosuggest__suggestion", timeout=10_000)
            page.click("li.react-autosuggest__suggestion")
            log("Suggestion selected.")
        except PWTimeout:
            raise RuntimeError(
                "No autocomplete suggestions appeared for this location. "
                "Try a different spelling."
            )

        # ── Wait for results table ────────────────────────────────────────────
        log("Waiting for results table…")
        page.wait_for_selector("#dealsTable", timeout=30_000)

        try:
            location_name = page.text_content(".locationLink").strip().replace(" ", "_")
        except Exception:
            location_name = location.replace(" ", "_")

        log(f"Location confirmed: {location_name}")

        # ── Scrape pages ──────────────────────────────────────────────────────
        rows_data = []
        page_num  = 1

        while page_num <= max_pages:
            rows = page.query_selector_all("#dealsTable tbody tr")
            page_rows = 0
            for row in rows:
                cells  = row.query_selector_all("td")
                values = [c.inner_text().strip() for c in cells[: len(HEADERS)]]
                if any(values):
                    rows_data.append(values)
                    page_rows += 1

            log(f"Page {page_num} — {page_rows} rows  (total: {len(rows_data)})")

            # Check next button
            next_btn = page.query_selector("#next")
            if not next_btn:
                break

            classes    = next_btn.get_attribute("class") or ""
            is_disabled = next_btn.get_attribute("disabled")
            style      = (next_btn.get_attribute("style") or "").replace(" ", "")

            if (
                "disabled" in classes.lower()
                or is_disabled is not None
                or "display:none" in style
            ):
                break

            # Capture first cell text before click to detect page change
            try:
                prev_first = page.query_selector(
                    "#dealsTable tbody tr td"
                ).inner_text().strip()
            except Exception:
                prev_first = ""

            next_btn.scroll_into_view_if_needed()
            next_btn.click()

            page.wait_for_function(
                """(prev) => {
                    const td = document.querySelector('#dealsTable tbody tr td');
                    return td && td.innerText.trim() !== prev;
                }""",
                arg=prev_first,
                timeout=15_000,
            )

            page_num += 1

        browser.close()
        return rows_data, page_num, location_name


# ── Data processing ───────────────────────────────────────────────────────────
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
            .str.replace("green arrow up",  "", regex=False)
            .str.replace("tooltip 16 copy", "", regex=False)
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

_install_browser()   # runs once, cached

with st.form("search_form"):
    location  = st.text_input(
        "Location (Hebrew)",
        placeholder="e.g.  רמת אביב",
        help="Type the location name in Hebrew as it appears on nadlan.gov.il",
    )
    max_pages = st.slider("Max pages to scrape", 1, 99, 99)
    submitted = st.form_submit_button("▶  Start Scraping", type="primary")

if submitted and location.strip():
    coords_df = load_coords()

    try:
        with st.status("Scraping in progress…", expanded=True) as status:
            def log(msg: str):
                st.write(msg)

            rows_data, pages, location_name = scrape(location.strip(), max_pages, log)

            log(f"Processing {len(rows_data)} rows…")
            df = process(rows_data)

            log("Merging coordinates…")
            df = merge_coords(df, coords_df)

            status.update(
                label=f"Done — {len(df):,} rows from {pages} page(s)",
                state="complete",
            )

        st.success(f"**{len(df):,} rows** scraped from **{pages} page(s)**")
        st.dataframe(df, use_container_width=True)

        now       = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename  = f"{location_name}_{now}.csv"
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
