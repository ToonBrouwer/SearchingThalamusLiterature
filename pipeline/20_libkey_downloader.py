"""
20_libkey_downloader.py — LibKey downloader with background PDF processing.

Architecture:
  - Main thread: opens LibKey tabs, closes them after a delay
  - Background thread: continuously scans Downloads for new PDFs,
    extracts text, matches to PMID, saves — never misses a download

Usage:
  conda activate thalamus-embed
  python -u 20_libkey_downloader.py                         # defaults
  python -u 20_libkey_downloader.py --parallel 3 --min-year 1995
  python -u 20_libkey_downloader.py --status
"""
import sys, os, time, re, json, webbrowser, argparse, threading
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

import pandas as pd
from pathlib import Path
from datetime import datetime
import shutil

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INT  = ROOT / "Data" / "interim"
TEXT_DIR = INT / "institutional_text"
PDF_DIR  = INT / "institutional_pdfs"
LOG_FILE = PROC / "institutional_download_log.csv"
DOWNLOADS = Path.home() / "Downloads"

TEXT_DIR.mkdir(parents=True, exist_ok=True)
PDF_DIR.mkdir(parents=True, exist_ok=True)

LIBKEY_BASE = "https://libkey.io/libraries/320"
PARALLEL = 3
TAB_STAGGER = 1.5      # seconds between opening tabs
BATCH_PAUSE = 10       # seconds to wait after opening a batch before closing
SCANNER_INTERVAL = 0.5 # how often the background scanner checks for new PDFs


# ═══════════════════════════════════════════════════════════════════════
# TAB MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════

def close_browser_tabs(n=1):
    try:
        import ctypes
        VK_CONTROL, VK_W = 0x11, 0x57
        KEYEVENTF_KEYUP = 0x0002
        user32 = ctypes.windll.user32
        for _ in range(n):
            user32.keybd_event(VK_CONTROL, 0, 0, 0)
            user32.keybd_event(VK_W, 0, 0, 0)
            time.sleep(0.05)
            user32.keybd_event(VK_W, 0, KEYEVENTF_KEYUP, 0)
            user32.keybd_event(VK_CONTROL, 0, KEYEVENTF_KEYUP, 0)
            time.sleep(0.15)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════
# PDF PROCESSING
# ═══════════════════════════════════════════════════════════════════════

def extract_text_from_pdf(pdf_path):
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        text = "".join(p.get_text() + "\n" for p in doc)
        doc.close()
        return text.strip()
    except:
        return ""


def match_pdf_to_pmid(text, doi_pmid_map):
    """Find DOI in PDF text and map to PMID."""
    text_lower = text.lower()[:6000]
    for doi, pmid in doi_pmid_map.items():
        if doi.lower() in text_lower:
            return pmid, doi
    for m in re.finditer(r'(10\.\d{4,}/[^\s,;]+)', text[:6000]):
        found = m.group(1).rstrip('.)')
        if found in doi_pmid_map:
            return doi_pmid_map[found], found
    return None, None


def get_doi_prefix(doi):
    m = re.match(r'(10\.\d+/)', doi)
    return m.group(1) if m else "unknown"


# ═══════════════════════════════════════════════════════════════════════
# BACKGROUND PDF SCANNER
# ═══════════════════════════════════════════════════════════════════════

class PdfScanner(threading.Thread):
    """Background thread that continuously scans Downloads for new PDFs.

    Uses two queues:
      - scan loop detects new files quickly (no blocking)
      - worker thread processes them (text extraction + matching)
    """

    def __init__(self, downloads_dir, doi_pmid_map, title_map=None):
        super().__init__(daemon=True)
        self.downloads_dir = downloads_dir
        self.doi_pmid_map = doi_pmid_map
        self.title_map = title_map or {}
        self.seen_files = set(f.name for f in downloads_dir.glob("*.pdf"))
        self.lock = threading.Lock()
        self.running = True
        self.process_queue = []
        self.queue_lock = threading.Lock()

        # Stats
        self.success = 0
        self.failed = 0
        self.total_chars = 0
        self.total_thalam = 0
        self.log_entries = []
        self.prefix_stats = {}

        # FIFO queue of expected downloads: [(pmid, doi, timestamp), ...]
        # Main thread pushes here when opening a tab,
        # worker pops when matching a PDF
        self.expected_queue = []
        self.expected_lock = threading.Lock()

        # Start worker thread for processing
        self.worker = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker.start()

    def run(self):
        """Fast scan loop — only detects files, never blocks on extraction."""
        while self.running:
            try:
                self._scan_once()
            except Exception as e:
                print(f"    [scanner] error: {e}")
            time.sleep(SCANNER_INTERVAL)

    def _scan_once(self):
        current = set()
        for f in self.downloads_dir.glob("*.pdf"):
            current.add(f.name)

        new_files = current - self.seen_files
        if not new_files:
            return

        crdownloads = {f.stem for f in self.downloads_dir.glob("*.crdownload")}

        for fname in sorted(new_files):
            stem = fname.replace(".pdf", "")
            if stem in crdownloads:
                continue

            pdf_path = self.downloads_dir / fname
            try:
                size = pdf_path.stat().st_size
            except:
                continue
            if size < 1000:
                continue

            # Quick stability check
            time.sleep(0.2)
            try:
                size2 = pdf_path.stat().st_size
            except:
                continue
            if size2 != size:
                continue

            self.seen_files.add(fname)
            # Queue for processing — don't block the scan loop
            with self.queue_lock:
                self.process_queue.append(pdf_path)

    def _worker_loop(self):
        """Separate thread that processes queued PDFs."""
        sweep_counter = 0
        while self.running:
            pdf_path = None
            with self.queue_lock:
                if self.process_queue:
                    pdf_path = self.process_queue.pop(0)
            if pdf_path:
                self._process_pdf(pdf_path)
                sweep_counter += 1
            else:
                time.sleep(0.2)
                sweep_counter += 1

            # Every ~60 seconds, do a full sweep of Downloads
            # to catch any PDFs the scanner missed
            if sweep_counter >= 120:  # 120 * 0.5s ≈ 60s
                sweep_counter = 0
                self._sweep_downloads()

    def _sweep_downloads(self):
        """Re-scan Downloads for PDFs we missed or failed to process.
        Only re-queues files not already in the queue or successfully saved."""
        already_saved = {f.stem for f in TEXT_DIR.glob("*.txt")
                         if f.stat().st_size > 100}
        with self.queue_lock:
            queued_names = {p.name for p in self.process_queue}

        found = 0
        for pdf_path in self.downloads_dir.glob("*.pdf"):
            fname = pdf_path.name
            if fname in queued_names:
                continue  # already queued

            try:
                size = pdf_path.stat().st_size
            except:
                continue
            if size < 1000:
                continue

            # Skip if still downloading
            crdownload = self.downloads_dir / (fname + ".crdownload")
            if crdownload.exists():
                continue

            # Check if this might be one we missed
            # (either never seen, or seen but no text saved)
            needs_processing = fname not in self.seen_files

            if not needs_processing and fname in self.seen_files:
                # We saw it before — only re-queue if recent AND not yet saved
                try:
                    age = time.time() - pdf_path.stat().st_mtime
                    if age < 300:  # less than 5 minutes old
                        # Quick check: extract text and see if PMID is already saved
                        text = extract_text_from_pdf(pdf_path)
                        if len(text) > 200:
                            pmid, _ = match_pdf_to_pmid(text, self.doi_pmid_map)
                            if pmid:
                                existing = TEXT_DIR / f"{pmid}.txt"
                                if existing.exists() and existing.stat().st_size > 100:
                                    # Already have it — just delete the PDF
                                    self._safe_delete(pdf_path)
                                    continue
                                else:
                                    needs_processing = True
                except:
                    pass

            if needs_processing:
                self.seen_files.add(fname)
                with self.queue_lock:
                    self.process_queue.append(pdf_path)
                found += 1

        if found > 0:
            print(f"    [sweep] re-queued {found} PDFs from Downloads")

        # Clean stale entries from expected queue (>2 min old)
        now = time.time()
        with self.expected_lock:
            stale = [e for e in self.expected_queue if now - e[2] > 120]
            if stale:
                for pmid, doi, _ in stale:
                    self._record_prefix(get_doi_prefix(doi), False)
                self.expected_queue = [
                    e for e in self.expected_queue if now - e[2] <= 120
                ]

    def _process_pdf(self, pdf_path):
        # Check file still exists
        if not pdf_path.exists():
            return
        try:
            if pdf_path.stat().st_size < 1000:
                return
        except:
            return

        text = extract_text_from_pdf(pdf_path)
        if len(text) < 200:
            with self.lock:
                self.failed += 1
            return  # keep PDF — might be readable later

        # === MATCHING: 3 strategies ===

        pmid, doi = None, None

        # Strategy 1: DOI found in PDF text → definitive match
        pmid, doi = match_pdf_to_pmid(text, self.doi_pmid_map)
        match_method = "doi_in_text"

        # Strategy 2: FIFO with title verification — try each expected
        # entry and verify the PDF content matches the paper title
        if pmid is None:
            with self.expected_lock:
                for i, (exp_pmid, exp_doi, exp_time) in enumerate(self.expected_queue):
                    existing = TEXT_DIR / f"{exp_pmid}.txt"
                    if existing.exists() and existing.stat().st_size > 100:
                        continue  # already have this one

                    # Verify: do title words from this paper appear in the PDF?
                    if verify_fifo_match(text, exp_pmid, self.title_map):
                        pmid, doi = exp_pmid, exp_doi
                        self.expected_queue.pop(i)
                        match_method = "fifo_verified"
                        break
                    # else: title doesn't match — try next expected entry

        # Skip if we already have text for this PMID
        if pmid:
            existing = TEXT_DIR / f"{pmid}.txt"
            if existing.exists() and existing.stat().st_size > 100:
                self._safe_delete(pdf_path)
                return

        thal_count = text.lower().count("thalam")

        if pmid:
            # Save text
            txt_path = TEXT_DIR / f"{pmid}.txt"
            txt_path.write_text(text, encoding="utf-8")

            # Verify before deleting
            if txt_path.exists() and txt_path.stat().st_size > 100:
                self._safe_delete(pdf_path)

            # Also remove from expected queue if matched by DOI
            if match_method == "doi_in_text":
                with self.expected_lock:
                    self.expected_queue = [
                        (p, d, t) for p, d, t in self.expected_queue
                        if p != pmid
                    ]

            prefix = get_doi_prefix(doi) if doi else "unknown"
            with self.lock:
                self.success += 1
                self.total_chars += len(text)
                self.total_thalam += thal_count
                self._record_prefix(prefix, True)
                self.log_entries.append({
                    "pmid": pmid, "doi": doi, "status": "success",
                    "n_chars": len(text), "n_thalam": thal_count,
                    "source_file": pdf_path.name,
                    "match_method": match_method,
                    "timestamp": datetime.now().isoformat(),
                })

            print(f"    OK  PMID {pmid:>10}  |  {len(text):>7,} chars  "
                  f"|  {thal_count:>3} thalam  |  {match_method}")
        else:
            with self.lock:
                self.failed += 1

    def _safe_delete(self, pdf_path):
        """Delete a PDF from Downloads to save storage."""
        try:
            pdf_path.unlink()
        except Exception:
            pass

    def _record_prefix(self, prefix, success):
        if prefix not in self.prefix_stats:
            self.prefix_stats[prefix] = {"attempts": 0, "hits": 0}
        self.prefix_stats[prefix]["attempts"] += 1
        if success:
            self.prefix_stats[prefix]["hits"] += 1

    def expect_download(self, pmid, doi):
        """Called by main thread when opening a tab — registers expected download."""
        with self.expected_lock:
            self.expected_queue.append((pmid, doi, time.time()))

    def record_timeout(self, doi):
        """Called by main thread for papers that timed out."""
        prefix = get_doi_prefix(doi)
        with self.lock:
            self._record_prefix(prefix, False)

    def should_skip_prefix(self, prefix, threshold=0.1, min_attempts=20):
        with self.lock:
            ps = self.prefix_stats.get(prefix)
        if not ps or ps["attempts"] < min_attempts:
            return False
        return ps["hits"] / ps["attempts"] < threshold

    def flush_log(self):
        """Write log entries to CSV and clear buffer."""
        with self.lock:
            entries = self.log_entries.copy()
            self.log_entries.clear()
        if entries:
            df = pd.DataFrame(entries)
            if LOG_FILE.exists():
                old = pd.read_csv(LOG_FILE)
                df = pd.concat([old, df], ignore_index=True)
                df = df.drop_duplicates(subset="pmid", keep="last")
            df.to_csv(LOG_FILE, index=False)

    def get_stats(self):
        with self.lock:
            return dict(success=self.success, failed=self.failed,
                        total_chars=self.total_chars,
                        total_thalam=self.total_thalam)


# ═══════════════════════════════════════════════════════════════════════
# TARGETS
# ═══════════════════════════════════════════════════════════════════════

def get_targets(min_year=1990):
    targets = pd.read_csv(PROC / "scihub_download_list.csv")
    already_have = set()

    pmc_path = INT / "fulltext" / "pmc_fulltext_2026-03-26.csv"
    if pmc_path.exists():
        pmc = pd.read_csv(pmc_path, usecols=["pmid", "full_text"], low_memory=False)
        already_have.update(
            pmc[pmc["full_text"].notna() & (pmc["full_text"].str.len() > 100)]["pmid"].unique()
        )
    for d in [INT / "scihub_text", TEXT_DIR]:
        if d.exists():
            for f in d.glob("*.txt"):
                if f.stat().st_size > 100:
                    try: already_have.add(int(f.stem))
                    except: pass

    targets = targets[~targets["pmid"].isin(already_have)].reset_index(drop=True)

    if min_year and "year" in targets.columns:
        before = len(targets)
        targets = targets[targets["year"] >= min_year].reset_index(drop=True)
        print(f"  Year filter (>={min_year}): {before:,} → {len(targets):,}")

    return targets, len(already_have)


def build_full_doi_map(targets):
    """Build complete DOI→PMID map for all targets."""
    return {str(row["doi"]): int(row["pmid"]) for _, row in targets.iterrows()}


def build_pmid_title_map():
    """Build PMID→title map from raw PubMed CSV for FIFO verification."""
    raw_csv = ROOT / "Data" / "raw" / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv"
    if raw_csv.exists():
        df = pd.read_csv(raw_csv, usecols=["pmid", "title"], low_memory=False)
        return {int(row["pmid"]): str(row["title"]).lower().strip()
                for _, row in df.iterrows() if pd.notna(row["title"])}
    return {}


def verify_fifo_match(text, pmid, title_map, threshold=3):
    """Verify a FIFO-assigned PMID by checking if title words appear in text.
    Returns True if enough title words match."""
    title = title_map.get(pmid, "")
    if not title:
        return True  # no title to check — accept the match

    # Extract significant words from title (>3 chars, not common words)
    stop = {"with", "from", "that", "this", "were", "have", "been", "their",
            "into", "than", "them", "then", "these", "those", "which", "where",
            "what", "when", "will", "each", "does", "both", "between", "after",
            "before", "during", "under", "over", "other", "more", "most",
            "some", "such", "only", "also", "about", "through", "using",
            "role", "study", "analysis", "effect", "effects", "evidence"}
    title_words = [w for w in re.findall(r'[a-z]{4,}', title)
                   if w not in stop]

    if len(title_words) < 2:
        return True  # title too short to verify

    text_lower = text.lower()[:8000]
    matches = sum(1 for w in title_words if w in text_lower)

    # Need at least 'threshold' words or 40% of title words to match
    return matches >= threshold or matches >= len(title_words) * 0.4


# ═══════════════════════════════════════════════════════════════════════
# MAIN DOWNLOADER
# ═══════════════════════════════════════════════════════════════════════

def run_downloader(start_idx=0, max_papers=None, parallel=PARALLEL,
                   min_year=1990, skip_threshold=0.1):
    targets, n_already = get_targets(min_year=min_year)

    print("=" * 60)
    print("  LIBKEY DOWNLOADER (background processing)")
    print("=" * 60)
    print(f"  Already have text:  {n_already:,}")
    print(f"  Remaining:          {len(targets):,}")
    print(f"  Downloads folder:   {DOWNLOADS}")
    print(f"  Parallel tabs:      {parallel}")
    print(f"  Tab stagger:        {TAB_STAGGER}s")
    print(f"  Batch pause:        {BATCH_PAUSE}s")
    print(f"  Min year:           {min_year}")
    print(f"  Auto-skip journals: <{skip_threshold*100:.0f}% hit rate")
    print(f"  Starting from:      #{start_idx + 1}")
    if max_papers:
        print(f"  Max this session:   {max_papers}")
    print()
    print("  First paper: check 'remember format 24h' + DOWNLOAD PDF")
    print("  After that it's hands-free. Ctrl+C to stop.")
    print("=" * 60)
    print()

    end_idx = len(targets)
    if max_papers:
        end_idx = min(start_idx + max_papers, len(targets))

    # Build maps and start background scanner
    doi_map = build_full_doi_map(targets)
    print(f"  Loading title map for FIFO verification...")
    title_map = build_pmid_title_map()
    print(f"  Titles loaded: {len(title_map):,}")
    scanner = PdfScanner(DOWNLOADS, doi_map, title_map)
    scanner.start()
    print(f"  Background PDF scanner started ({len(doi_map):,} DOIs loaded)")
    print()

    n_opened = 0
    n_skipped_journal = 0
    t0 = time.time()

    try:
        idx = start_idx
        while idx < end_idx:
            # Build batch, skipping dead publishers
            batch_rows = []
            while len(batch_rows) < parallel and idx < end_idx:
                row = targets.iloc[idx]
                doi = str(row["doi"])
                prefix = get_doi_prefix(doi)

                if scanner.should_skip_prefix(prefix, skip_threshold):
                    n_skipped_journal += 1
                    idx += 1
                    continue

                batch_rows.append(row)
                idx += 1

            if not batch_rows:
                continue

            # Status line
            s = scanner.get_stats()
            elapsed = time.time() - t0
            rate = s["success"] / (elapsed / 60) if elapsed > 60 else 0
            print(f"  Opening [{idx-len(batch_rows)+1}–{idx}]  |  "
                  f"ok={s['success']}  fail={s['failed']}  "
                  f"skip={n_skipped_journal}  "
                  f"rate={rate:.1f}/min")

            # Open tabs and register expected downloads
            for row in batch_rows:
                pmid = int(row["pmid"])
                doi = str(row["doi"])
                url = f"{LIBKEY_BASE}/{doi}"
                scanner.expect_download(pmid, doi)
                webbrowser.open(url)
                n_opened += 1
                time.sleep(TAB_STAGGER)

            # Wait for downloads to happen
            time.sleep(BATCH_PAUSE)

            # Close tabs
            close_browser_tabs(len(batch_rows) + 1)
            time.sleep(0.3)

            # Record timeouts for papers in this batch that didn't download
            # (the scanner handles successes automatically)

            # Flush log every 10 batches
            if n_opened % (parallel * 10) == 0:
                scanner.flush_log()

                # Print publisher stats
                dead = [(p, ps) for p, ps in scanner.prefix_stats.items()
                        if ps["attempts"] >= 20 and ps["hits"]/ps["attempts"] < skip_threshold]
                if dead:
                    print(f"\n    Auto-skipping {len(dead)} low-hit publishers")

    except KeyboardInterrupt:
        print("\n\n  Stopping...")

    # Stop scanner and flush
    scanner.running = False
    time.sleep(1)  # let scanner finish current scan
    scanner.flush_log()

    # Final summary
    s = scanner.get_stats()
    elapsed = time.time() - t0
    print()
    print("=" * 60)
    print("  SESSION SUMMARY")
    print("=" * 60)
    print(f"  Duration:          {elapsed/60:.1f} minutes")
    print(f"  Tabs opened:       {n_opened}")
    print(f"  PDFs processed:    {s['success']}")
    print(f"  Failed extracts:   {s['failed']}")
    print(f"  Skipped (journal): {n_skipped_journal}")
    print(f"  Total text:        {s['total_chars']:,} chars")
    print(f"  Total thalam:      {s['total_thalam']:,} mentions")
    if elapsed > 60:
        print(f"  Rate:              {s['success'] / (elapsed/60):.1f} papers/min")

    n_total = len([f for f in TEXT_DIR.glob("*.txt") if f.stat().st_size > 100])
    print(f"\n  Total institutional texts: {n_total}")
    print(f"  Log: {LOG_FILE}")

    # Publisher stats
    if scanner.prefix_stats:
        print(f"\n  Publisher stats (top 15):")
        sorted_p = sorted(scanner.prefix_stats.items(),
                          key=lambda x: x[1]["attempts"], reverse=True)
        for p, ps in sorted_p[:15]:
            rate = ps["hits"] / ps["attempts"] * 100 if ps["attempts"] > 0 else 0
            skip = " SKIP" if ps["attempts"] >= 20 and rate < skip_threshold * 100 else ""
            print(f"    {p:15s}  {ps['hits']:>4}/{ps['attempts']:<4}  "
                  f"({rate:5.1f}%){skip}")


# ═══════════════════════════════════════════════════════════════════════
# STATUS
# ═══════════════════════════════════════════════════════════════════════

def show_status():
    targets, n_already = get_targets()
    n_inst = len([f for f in TEXT_DIR.glob("*.txt")
                  if f.stat().st_size > 100 and not f.name.startswith("unmatched")])

    print("=" * 60)
    print("  DOWNLOAD STATUS")
    print("=" * 60)
    print(f"  Have full text (all sources): {n_already:,}")
    print(f"  Remaining:                    {len(targets):,}")
    print(f"  Institutional texts:          {n_inst}")
    print(f"  Institutional PDFs:           {len(list(PDF_DIR.glob('*.pdf')))}")

    if LOG_FILE.exists():
        log = pd.read_csv(LOG_FILE)
        print(f"\n  Log: {len(log)} entries")
        for status, count in log["status"].value_counts().items():
            print(f"    {status:25s}: {count:,}")
        ok = log[log["status"].str.startswith("success")]
        if len(ok) > 0:
            print(f"\n  Successful downloads:")
            print(f"    Total chars:  {ok['n_chars'].sum():,}")
            print(f"    Avg chars:    {ok['n_chars'].mean():,.0f}")
            if "n_thalam" in ok.columns:
                print(f"    Total thalam: {ok['n_thalam'].sum():,}")


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LibKey downloader with background PDF processing")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--max", type=int, default=None)
    parser.add_argument("--parallel", type=int, default=PARALLEL,
                        help=f"Tabs at once (default {PARALLEL})")
    parser.add_argument("--min-year", type=int, default=1990)
    parser.add_argument("--skip-threshold", type=float, default=0.1,
                        help="Auto-skip publisher below this hit rate (default 0.1)")
    parser.add_argument("--stagger", type=float, default=TAB_STAGGER,
                        help=f"Seconds between tabs (default {TAB_STAGGER})")
    parser.add_argument("--pause", type=float, default=BATCH_PAUSE,
                        help=f"Seconds to wait per batch (default {BATCH_PAUSE})")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--downloads-dir", type=str, default=None)
    args = parser.parse_args()

    if args.downloads_dir:
        DOWNLOADS = Path(args.downloads_dir)
    TAB_STAGGER = args.stagger
    BATCH_PAUSE = args.pause

    if args.status:
        show_status()
    else:
        run_downloader(start_idx=args.start, max_papers=args.max,
                       parallel=args.parallel, min_year=args.min_year,
                       skip_threshold=args.skip_threshold)
