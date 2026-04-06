# full_pipeline.py
import sys
import os
import csv
import datetime
import time
import threading
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === CONFIGURATION ===
MAX_PAGES = 99
WAIT_SECONDS = 0.5
COORDS_FILENAME = "coordinates.csv"
english_headers = [
    "Serial Number", "Address", "Area (sqm)", "Transaction Date",
    "Transaction Price", "Parcel", "Property Type", "Rooms", "Floor", "Change Trend"
]

# ── Colours ──────────────────────────────────────────────────────────────────
BG       = "#0f0f1a"
PANEL    = "#1a1a2e"
ACCENT   = "#e94560"
ACCENT2  = "#0f3460"
TEXT     = "#d0d0e0"
MUTED    = "#555577"
SUCCESS  = "#4caf50"
WARNING  = "#ffcc44"
ERROR    = "#ff4c4c"
FONT     = "Segoe UI"


class NadlanApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Nadlan Scraper")
        self.root.geometry("640x540")
        self.root.resizable(False, False)
        self.root.configure(bg=BG)

        # set window icon
        try:
            base = sys._MEIPASS if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))
            self.root.iconbitmap(os.path.join(base, "app_icon.ico"))
        except Exception:
            pass

        # centre on screen
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth()  - 640) // 2
        y = (self.root.winfo_screenheight() - 540) // 2
        self.root.geometry(f"640x540+{x}+{y}")

        self._build_styles()
        self._build_ui()

        self.driver        = None
        self.csv_filename  = None
        self.output_folder = None
        self.continue_event  = threading.Event()
        self._folder_event   = threading.Event()
        self._stop_flag      = False

    # ── Styles ────────────────────────────────────────────────────────────────
    def _build_styles(self):
        s = ttk.Style()
        s.theme_use("clam")
        s.configure("Bar.Horizontal.TProgressbar",
                    troughcolor=PANEL, background=ACCENT,
                    lightcolor=ACCENT, darkcolor=ACCENT, borderwidth=0)

    # ── UI Layout ─────────────────────────────────────────────────────────────
    def _build_ui(self):
        # Header
        tk.Label(self.root, text="Nadlan.gov.il Scraper",
                 font=(FONT, 20, "bold"), bg=BG, fg=ACCENT).pack(pady=(22, 2))
        tk.Label(self.root, text="Real Estate Data Extractor",
                 font=(FONT, 10), bg=BG, fg=MUTED).pack(pady=(0, 14))

        # Step indicators
        steps_frame = tk.Frame(self.root, bg=BG)
        steps_frame.pack(padx=30, fill="x")
        self.step_labels = []
        self._steps = ["1. Open Browser", "2. Scrape", "3. Process", "4. Coordinates"]
        for s in self._steps:
            lbl = tk.Label(steps_frame, text=f"○  {s}",
                           font=(FONT, 9), bg=BG, fg=MUTED, anchor="w")
            lbl.pack(side="left", expand=True)
            self.step_labels.append(lbl)

        # Separator
        tk.Frame(self.root, bg=MUTED, height=1).pack(fill="x", padx=30, pady=10)

        # Log panel
        log_outer = tk.Frame(self.root, bg=PANEL, bd=0)
        log_outer.pack(padx=30, fill="both", expand=True)

        scrollbar = tk.Scrollbar(log_outer, bg=PANEL, troughcolor=PANEL,
                                 activebackground=ACCENT, relief="flat", bd=0)
        scrollbar.pack(side="right", fill="y", padx=(0, 4), pady=6)

        self.log_text = tk.Text(
            log_outer, font=("Consolas", 10), bg=PANEL, fg=TEXT,
            relief="flat", bd=0, wrap="word", state="disabled",
            height=12, yscrollcommand=scrollbar.set,
            insertbackground=TEXT, selectbackground=ACCENT2)
        self.log_text.pack(side="left", padx=10, pady=8, fill="both", expand=True)
        scrollbar.config(command=self.log_text.yview)

        self.log_text.tag_configure("ok",      foreground=SUCCESS)
        self.log_text.tag_configure("warn",     foreground=WARNING)
        self.log_text.tag_configure("err",      foreground=ERROR)
        self.log_text.tag_configure("action",   foreground="#7ecfff")

        # Progress bar + label
        prog_frame = tk.Frame(self.root, bg=BG)
        prog_frame.pack(padx=30, pady=(8, 0), fill="x")
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            prog_frame, variable=self.progress_var,
            maximum=MAX_PAGES, style="Bar.Horizontal.TProgressbar", length=580)
        self.progress_bar.pack(fill="x")

        self.progress_label = tk.Label(prog_frame, text="",
                                       font=(FONT, 9), bg=BG, fg=MUTED)
        self.progress_label.pack(anchor="e", pady=(2, 0))

        # Buttons
        btn_frame = tk.Frame(self.root, bg=BG)
        btn_frame.pack(pady=14)

        self.start_btn = self._btn(btn_frame, "▶  Start", ACCENT, self._on_start)
        self.start_btn.pack(side="left", padx=6)

        self.continue_btn = self._btn(btn_frame, "Continue  →", ACCENT2, self._on_continue,
                                      state="disabled")
        self.continue_btn.pack(side="left", padx=6)

        self.stop_btn = self._btn(btn_frame, "■  Stop", "#555577", self._on_stop,
                                  state="disabled")
        self.stop_btn.pack(side="left", padx=6)

    def _btn(self, parent, text, color, cmd, state="normal"):
        b = tk.Button(parent, text=text,
                      font=(FONT, 11, "bold"), bg=color, fg="white",
                      relief="flat", bd=0, padx=20, pady=9,
                      cursor="hand2", activebackground=color,
                      activeforeground="white", command=cmd, state=state)
        b.bind("<Enter>", lambda e: b.configure(bg=self._lighten(color)))
        b.bind("<Leave>", lambda e: b.configure(bg=color))
        return b

    @staticmethod
    def _lighten(hex_color):
        r, g, b = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
        r, g, b = min(255, r + 30), min(255, g + 30), min(255, b + 30)
        return f"#{r:02x}{g:02x}{b:02x}"

    # ── Logging ───────────────────────────────────────────────────────────────
    def log(self, msg, kind=""):
        def _do():
            self.log_text.configure(state="normal")
            prefix = {"ok": "✓", "warn": "⚠", "err": "✗", "action": "▸"}.get(kind, "▸")
            self.log_text.insert("end", f"{prefix}  {msg}\n", kind or None)
            self.log_text.configure(state="disabled")
            self.log_text.see("end")
        self.root.after(0, _do)

    # ── Step indicator ────────────────────────────────────────────────────────
    def set_step(self, idx):
        def _do():
            for i, lbl in enumerate(self.step_labels):
                if i < idx:
                    lbl.configure(fg=SUCCESS, text=f"✓  {self._steps[i]}")
                elif i == idx:
                    lbl.configure(fg=ACCENT,  text=f"●  {self._steps[i]}")
                else:
                    lbl.configure(fg=MUTED,   text=f"○  {self._steps[i]}")
        self.root.after(0, _do)

    # ── Progress ──────────────────────────────────────────────────────────────
    def set_progress(self, page):
        def _do():
            self.progress_var.set(page)
            self.progress_label.configure(text=f"Page {page} / {MAX_PAGES}")
        self.root.after(0, _do)

    # ── Continue gate ─────────────────────────────────────────────────────────
    def wait_for_continue(self, prompt):
        self.log(prompt, "action")
        self.root.after(0, lambda: self.continue_btn.configure(state="normal"))
        self.continue_event.clear()
        self.continue_event.wait()
        self.root.after(0, lambda: self.continue_btn.configure(state="disabled"))

    def _pick_save_folder(self):
        folder = filedialog.askdirectory(
            title="Choose folder to save output files",
            parent=self.root)
        self.output_folder = folder if folder else os.path.expanduser("~")
        self._folder_event.set()

    def _on_continue(self):
        self.continue_event.set()

    def _on_stop(self):
        self._stop_flag = True
        self.root.after(0, lambda: self.stop_btn.configure(state="disabled"))
        self.log("Stop requested — finishing current page…", "warn")

    # ── Start ─────────────────────────────────────────────────────────────────
    def _on_start(self):
        self._stop_flag = False
        self.start_btn.configure(state="disabled")
        self.progress_var.set(0)
        self.progress_label.configure(text="")
        threading.Thread(target=self._pipeline, daemon=True).start()

    # ═════════════════════════════════════════════════════════════════════════
    #  PIPELINE
    # ═════════════════════════════════════════════════════════════════════════
    def _pipeline(self):
        try:
            # ── Step 1: open browser ─────────────────────────────────────────
            self.set_step(0)
            self.log("Opening Chrome…")
            self.driver = webdriver.Chrome()
            self.driver.get("https://www.nadlan.gov.il/")

            self.wait_for_continue(
                "Browser is open.  Search for the location on the site, "
                "wait for the results table to appear, then click  Continue →")

            # ── Step 2: scrape ───────────────────────────────────────────────
            self.set_step(1)
            self.log("Waiting for results table…")
            WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "dealsTable")))

            try:
                loc_el = self.driver.find_element(By.CLASS_NAME, "locationLink")
                location_name = loc_el.text.strip().replace(" ", "_")
            except Exception:
                location_name = "UnknownLocation"

            self.log(f"Location: {location_name}", "ok")

            # Ask user where to save before creating any files
            self.log("Choose a folder to save output files…", "action")
            self._folder_event.clear()
            self.root.after(0, self._pick_save_folder)
            self._folder_event.wait()
            self.log(f"Saving to: {self.output_folder}", "ok")

            now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            self.csv_filename = os.path.join(self.output_folder, f"{location_name}_{now}.csv")

            self.root.after(0, lambda: self.stop_btn.configure(state="normal"))
            pages = self._scrape(location_name)
            self.root.after(0, lambda: self.stop_btn.configure(state="disabled"))
            self.log(f"Scraping complete — {pages} page(s) saved.", "ok")

            # ── Step 3: process ──────────────────────────────────────────────
            self.set_step(2)
            self.log("Cleaning and processing data…")
            df = self._process()
            self.log("Data cleaned.", "ok")

            # ── Step 4: coordinates ──────────────────────────────────────────
            self.set_step(3)
            self.log("Merging coordinates…")
            final_csv = self._merge_coords(df)
            if final_csv:
                self.log(f"Final file: {os.path.basename(final_csv)}", "ok")

            # Done
            for lbl in self.step_labels:
                lbl.configure(fg=SUCCESS)
            self.log("All done!", "ok")
            folder = os.path.dirname(os.path.abspath(self.csv_filename))
            fname  = os.path.basename(final_csv) if final_csv else ""
            self.root.after(0, lambda: self._show_success(fname, folder))

        except Exception as exc:
            self.log(f"Error: {exc}", "err")
            self.root.after(0, lambda: self._show_error(str(exc)))

        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except Exception:
                    pass
            self.root.after(0, lambda: self.start_btn.configure(state="normal"))

    # ── Scrape pages ──────────────────────────────────────────────────────────
    def _scrape(self, location_name):
        page = 1
        with open(self.csv_filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            writer.writerow([f"Transactions for {location_name.replace('_', ' ')} — Extracted {ts}"])
            writer.writerow([])
            writer.writerow(english_headers)

            while page <= MAX_PAGES:
                table = self.driver.find_element(By.ID, "dealsTable")
                tbody = table.find_element(By.TAG_NAME, "tbody")
                rows  = tbody.find_elements(By.TAG_NAME, "tr")

                for row in rows:
                    cells  = row.find_elements(By.TAG_NAME, "td")
                    values = [c.text.strip() for c in cells[:len(english_headers)]]
                    if any(values):
                        writer.writerow(values)

                self.set_progress(page)
                self.log(f"Page {page} scraped")

                if self._stop_flag:
                    self.log(f"Stopped by user at page {page}.", "warn")
                    break

                # ── next-button detection ────────────────────────────────────
                try:
                    next_btn = self.driver.find_element(By.ID, "next")
                except Exception:
                    self.log("Next button not found — stopping.", "warn")
                    break

                classes  = next_btn.get_attribute("class") or ""
                disabled = next_btn.get_attribute("disabled")
                style    = next_btn.get_attribute("style") or ""

                if (not next_btn.is_enabled()
                        or "disabled" in classes.lower()
                        or disabled is not None
                        or "display: none" in style
                        or "display:none" in style):
                    self.log(f"Last page reached at page {page}.", "ok")
                    break

                # capture first row before click so we can detect page change
                try:
                    prev_first = rows[0].find_elements(By.TAG_NAME, "td")[0].text.strip()
                except Exception:
                    prev_first = ""

                self.driver.execute_script("arguments[0].scrollIntoView();", next_btn)
                self.driver.execute_script("arguments[0].click();", next_btn)
                time.sleep(WAIT_SECONDS)

                WebDriverWait(self.driver, 15).until(
                    lambda d: (
                        d.find_element(By.ID, "dealsTable")
                         .find_element(By.TAG_NAME, "tbody")
                         .find_elements(By.TAG_NAME, "tr")[0]
                         .find_elements(By.TAG_NAME, "td")[0]
                         .text.strip()
                    ) != prev_first
                )

                page += 1

        return page

    # ── Process / clean ───────────────────────────────────────────────────────
    def _process(self):
        df = pd.read_csv(self.csv_filename, skiprows=2)

        # delete the raw CSV — only the final file will remain
        try:
            os.remove(self.csv_filename)
        except Exception:
            pass

        if "Serial Number" in df.columns:
            df = df.drop(columns=["Serial Number"])

        if "Transaction Price" in df.columns:
            df["Transaction Price"] = (
                df["Transaction Price"].astype(str)
                .str.replace("\u20aa", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.strip())
            df["Transaction Price"] = pd.to_numeric(df["Transaction Price"], errors="coerce")

        if "Transaction Date" in df.columns:
            df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], errors="coerce")

        if "Parcel" in df.columns:
            parts = df["Parcel"].str.split("-", expand=True)
            df["_p1"] = pd.to_numeric(parts[0], errors="coerce")
            df["_p2"] = pd.to_numeric(parts[1], errors="coerce")
            df["_p3"] = pd.to_numeric(parts[2], errors="coerce")
            df["Gush_Helka"] = (df["_p1"].astype("Int64").astype(str) + "/"
                                + df["_p2"].astype("Int64").astype(str))

        if "Change Trend" in df.columns:
            ct = (df["Change Trend"].astype(str)
                  .str.replace("green arrow up", "", regex=False)
                  .str.replace("tooltip 16 copy", "", regex=False))
            df["Change Trend"]       = ct
            df["Percentage"]         = pd.to_numeric(ct.str[:6].str.replace("%", "").str.strip(), errors="coerce")
            df["Number_of_years"]    = pd.to_numeric(
                ct.str[-9:].str.replace(r"[\u05e9\u05e0\u05d9\u05dd\u05d1]", "", regex=True).str.strip(),
                errors="coerce")
            df["Percentage_Per_Year"] = df["Percentage"] / df["Number_of_years"]

        if "Area (sqm)" in df.columns:
            df["Area (sqm)"]    = pd.to_numeric(df["Area (sqm)"], errors="coerce")
            df["Price_per_sqm"] = (df["Transaction Price"] / df["Area (sqm)"]).round(0).astype("Int64")

        df = df.drop(columns=[c for c in ["_p1", "_p2", "_p3"] if c in df.columns])
        return df

    # ── Merge coordinates ─────────────────────────────────────────────────────
    def _merge_coords(self, df):
        base = sys._MEIPASS if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))
        coords_path = os.path.join(base, COORDS_FILENAME)

        if not os.path.exists(coords_path):
            self.log(f"Coordinates file not found: {coords_path}", "warn")
            # save what we have without coords
            final_csv = self.csv_filename.replace(".csv", "_processed.csv")
            df.to_csv(final_csv, index=False, encoding="utf-8-sig")
            return final_csv

        coords = pd.read_csv(coords_path)
        coords.columns = coords.columns.str.strip().str.lower()
        coords = coords.rename(columns={"gush_helka": "Gush_Helka", "x": "X", "y": "Y"})

        merged    = pd.merge(df, coords[["Gush_Helka", "X", "Y"]], on="Gush_Helka", how="left")
        final_csv = self.csv_filename.replace(".csv", "_with_coords.csv")
        merged.to_csv(final_csv, index=False, encoding="utf-8-sig")
        return final_csv

    # ── Custom dialogs ────────────────────────────────────────────────────────
    def _show_success(self, filename, folder):
        dlg = tk.Toplevel(self.root)
        dlg.title("")
        dlg.resizable(False, False)
        dlg.configure(bg=PANEL)
        dlg.grab_set()

        w, h = 420, 230
        px = self.root.winfo_x() + (640 - w) // 2
        py = self.root.winfo_y() + (540 - h) // 2
        dlg.geometry(f"{w}x{h}+{px}+{py}")

        # green top bar
        tk.Frame(dlg, bg=SUCCESS, height=4).pack(fill="x")

        tk.Label(dlg, text="✓", font=(FONT, 32, "bold"),
                 bg=PANEL, fg=SUCCESS).pack(pady=(18, 4))

        tk.Label(dlg, text="Download Complete",
                 font=(FONT, 15, "bold"), bg=PANEL, fg=TEXT).pack()

        tk.Label(dlg, text=filename,
                 font=("Consolas", 9), bg=PANEL, fg=MUTED).pack(pady=(4, 0))

        tk.Label(dlg, text=folder,
                 font=(FONT, 8), bg=PANEL, fg=MUTED,
                 wraplength=380).pack(pady=(2, 14))

        self._btn(dlg, "Open Folder", ACCENT2,
                  lambda: (os.startfile(folder), dlg.destroy())).pack(side="left", padx=(40, 8), pady=10)
        self._btn(dlg, "Close", ACCENT,
                  dlg.destroy).pack(side="left", padx=(8, 40), pady=10)

    def _show_error(self, message):
        dlg = tk.Toplevel(self.root)
        dlg.title("")
        dlg.resizable(False, False)
        dlg.configure(bg=PANEL)
        dlg.grab_set()

        w, h = 420, 200
        px = self.root.winfo_x() + (640 - w) // 2
        py = self.root.winfo_y() + (540 - h) // 2
        dlg.geometry(f"{w}x{h}+{px}+{py}")

        tk.Frame(dlg, bg=ERROR, height=4).pack(fill="x")

        tk.Label(dlg, text="✗", font=(FONT, 28, "bold"),
                 bg=PANEL, fg=ERROR).pack(pady=(16, 4))

        tk.Label(dlg, text="Something went wrong",
                 font=(FONT, 13, "bold"), bg=PANEL, fg=TEXT).pack()

        tk.Label(dlg, text=message,
                 font=(FONT, 9), bg=PANEL, fg=MUTED,
                 wraplength=380).pack(pady=(6, 16))

        self._btn(dlg, "Close", ACCENT, dlg.destroy).pack(pady=4)

    # ── Run ───────────────────────────────────────────────────────────────────
    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    NadlanApp().run()
