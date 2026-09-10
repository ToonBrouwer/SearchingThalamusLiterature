"""
20_generate_libkey_links.py — Automated LibKey PDF downloader + watcher.

Workflow:
  1. --generate   Creates HTML batch pages with auto-open functionality
  2. --watch      Monitors Downloads folder, extracts text, tracks progress
  3. Open batch_1.html → check "remember format 24h" → click DOWNLOAD PDF once
     Then click "Auto-Open All" → remaining links open with delays → auto-download

Usage:
  python 20_generate_libkey_links.py --generate
  python 20_generate_libkey_links.py --generate --max-batches 5
  python 20_generate_libkey_links.py --watch
  python 20_generate_libkey_links.py --status
"""
import sys, os, time, argparse, re, json
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INT  = ROOT / "Data" / "interim"
TEXT_DIR = INT / "institutional_text"
PDF_DIR  = INT / "institutional_pdfs"
LINKS_DIR = INT / "libkey_batches"
LOG_FILE = PROC / "institutional_download_log.csv"
MAPPING_FILE = PROC / "doi_pmid_mapping.json"

TEXT_DIR.mkdir(parents=True, exist_ok=True)
PDF_DIR.mkdir(parents=True, exist_ok=True)
LINKS_DIR.mkdir(parents=True, exist_ok=True)

LIBKEY_BASE = "https://libkey.io/libraries/320"
BATCH_SIZE = 20


# ══════════════════════════════════════════════════════════════════════════
# TARGETS
# ══════════════════════════════════════════════════════════════════════════

def get_remaining_targets():
    """Get DOIs we still need full text for."""
    targets = pd.read_csv(PROC / "scihub_download_list.csv")

    already_have = set()

    # PMC fulltext
    pmc_path = INT / "fulltext" / "pmc_fulltext_2026-03-26.csv"
    if pmc_path.exists():
        pmc = pd.read_csv(pmc_path, usecols=["pmid", "full_text"], low_memory=False)
        pmc_ft = pmc[pmc["full_text"].notna() & (pmc["full_text"].str.len() > 100)]
        already_have.update(pmc_ft["pmid"].unique())

    # OA text
    oa_dir = INT / "scihub_text"
    if oa_dir.exists():
        for f in oa_dir.glob("*.txt"):
            if f.stat().st_size > 100:
                try: already_have.add(int(f.stem))
                except: pass

    # Institutional text already done
    for f in TEXT_DIR.glob("*.txt"):
        if f.stat().st_size > 100:
            try: already_have.add(int(f.stem))
            except: pass

    n_total = len(targets)
    n_have = len(already_have)
    targets = targets[~targets["pmid"].isin(already_have)].reset_index(drop=True)
    return targets, n_total, n_have


# ══════════════════════════════════════════════════════════════════════════
# GENERATE HTML BATCHES
# ══════════════════════════════════════════════════════════════════════════

def generate_html_batches(max_batches=None):
    targets, n_total, n_have = get_remaining_targets()
    print(f"  Total DOI papers:   {n_total:,}")
    print(f"  Already have text:  {n_have:,}")
    print(f"  Remaining:          {len(targets):,}")

    if len(targets) == 0:
        print("  Nothing to generate!")
        return

    n_batches = (len(targets) + BATCH_SIZE - 1) // BATCH_SIZE
    if max_batches:
        n_batches = min(n_batches, max_batches)

    print(f"  Generating {n_batches} batches ({BATCH_SIZE} per batch)...")

    # DOI→PMID mapping for the watcher
    mapping = {}
    # Also save full target list for tracking
    all_targets = []

    for batch_idx in range(n_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(targets))
        batch = targets.iloc[start:end]

        rows_html = []
        for row_idx, (_, row) in enumerate(batch.iterrows()):
            pmid = int(row["pmid"])
            doi = row["doi"]
            link = f"{LIBKEY_BASE}/{doi}"
            mapping[doi] = pmid
            all_targets.append({"pmid": pmid, "doi": doi, "batch": batch_idx + 1})
            rows_html.append(
                f'<tr id="row-{row_idx}">'
                f'<td>{pmid}</td>'
                f'<td><code style="font-size:11px">{doi}</code></td>'
                f'<td><a href="{link}" target="_blank" class="libkey-link" '
                f'data-idx="{row_idx}" '
                f'onclick="markOpened({row_idx})" '
                f'style="font-size:14px; font-weight:bold; color:#0066cc;">'
                f'OPEN</a></td>'
                f'<td class="status" id="status-{row_idx}">—</td>'
                f'</tr>'
            )

        links_json = json.dumps([
            {"doi": row["doi"], "url": f"{LIBKEY_BASE}/{row['doi']}"}
            for _, row in batch.iterrows()
        ])

        html = f"""<!DOCTYPE html>
<html><head>
<title>LibKey Batch {batch_idx+1}/{n_batches}</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 20px; background: #f8f9fa; }}
  h1 {{ color: #333; margin-bottom: 5px; }}
  .subtitle {{ color: #666; margin-bottom: 15px; }}
  table {{ border-collapse: collapse; width: 100%; background: white; }}
  th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; }}
  th {{ background-color: #0072B2; color: white; font-size: 13px; }}
  tr:nth-child(even) {{ background-color: #f2f2f2; }}
  tr.opened {{ background-color: #d4edda !important; }}
  a {{ text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .controls {{ background: #e8f4f8; border: 2px solid #0072B2;
               padding: 15px; border-radius: 8px; margin-bottom: 15px; }}
  .btn {{ padding: 10px 20px; font-size: 14px; font-weight: bold; border: none;
          border-radius: 5px; cursor: pointer; margin-right: 10px; }}
  .btn-start {{ background: #28a745; color: white; }}
  .btn-start:hover {{ background: #218838; }}
  .btn-stop {{ background: #dc3545; color: white; }}
  .btn-stop:hover {{ background: #c82333; }}
  .btn-skip {{ background: #6c757d; color: white; }}
  .progress {{ margin-top: 10px; font-size: 14px; }}
  .progress-bar {{ width: 100%; height: 20px; background: #e0e0e0;
                   border-radius: 10px; overflow: hidden; margin-top: 5px; }}
  .progress-fill {{ height: 100%; background: #28a745; transition: width 0.5s; }}
  .nav {{ margin: 15px 0; font-size: 14px; }}
  .warning {{ background: #fff3cd; border: 1px solid #ffc107;
              padding: 10px; border-radius: 5px; margin-bottom: 10px; }}
</style>
</head><body>
<h1>LibKey Batch {batch_idx+1} of {n_batches}</h1>
<p class="subtitle">Papers {start+1}–{end} of {len(targets)} remaining</p>

<div class="warning">
  <b>First time?</b> Open the first link manually, check
  "<b>Automatically remember format choice for 24 hours</b>",
  then click DOWNLOAD PDF. After that, Auto-Open will download PDFs automatically!
</div>

<div class="controls">
  <button class="btn btn-start" onclick="startAutoOpen()">
    ▶ Auto-Open All (5s delay)
  </button>
  <button class="btn btn-stop" onclick="stopAutoOpen()" id="btn-stop" disabled>
    ⏹ Stop
  </button>
  <button class="btn btn-skip" onclick="setDelay()">
    ⏱ Change Delay
  </button>
  <div class="progress">
    <span id="progress-text">Ready — click Auto-Open to start</span>
    <div class="progress-bar">
      <div class="progress-fill" id="progress-fill" style="width: 0%"></div>
    </div>
  </div>
</div>

<div class="nav">
  {'<a href="batch_' + str(batch_idx) + '.html">← Previous batch</a> | ' if batch_idx > 0 else ''}
  {'<a href="batch_' + str(batch_idx+2) + '.html">Next batch →</a>' if batch_idx < n_batches-1 else '<b>Last batch!</b>'}
</div>

<table>
  <tr><th>#</th><th>PMID</th><th>DOI</th><th>Link</th><th>Status</th></tr>
  {''.join(f'<tr id="row-{i}"><td>{i+1}</td>{r[4:]}'
           for i, r in enumerate(rows_html))}
</table>

<script>
const links = {links_json};
let autoTimer = null;
let currentIdx = 0;
let delay = 5000;

function markOpened(idx) {{
  document.getElementById('row-' + idx).classList.add('opened');
  document.getElementById('status-' + idx).textContent = 'opened';
  updateProgress();
}}

function updateProgress() {{
  const opened = document.querySelectorAll('.opened').length;
  const total = links.length;
  const pct = (opened / total * 100).toFixed(0);
  document.getElementById('progress-text').textContent =
    opened + '/' + total + ' opened (' + pct + '%)';
  document.getElementById('progress-fill').style.width = pct + '%';
}}

function startAutoOpen() {{
  document.getElementById('btn-stop').disabled = false;
  openNext();
}}

function openNext() {{
  if (currentIdx >= links.length) {{
    document.getElementById('progress-text').textContent =
      'Done! All ' + links.length + ' links opened.';
    document.getElementById('btn-stop').disabled = true;
    return;
  }}
  window.open(links[currentIdx].url, '_blank');
  markOpened(currentIdx);
  currentIdx++;
  document.getElementById('progress-text').textContent =
    currentIdx + '/' + links.length + ' opened — next in ' + (delay/1000) + 's...';
  autoTimer = setTimeout(openNext, delay);
}}

function stopAutoOpen() {{
  clearTimeout(autoTimer);
  document.getElementById('progress-text').textContent =
    'Stopped at ' + currentIdx + '/' + links.length;
  document.getElementById('btn-stop').disabled = true;
}}

function setDelay() {{
  const d = prompt('Delay between links (seconds):', delay/1000);
  if (d && !isNaN(d)) {{
    delay = parseFloat(d) * 1000;
    alert('Delay set to ' + d + ' seconds');
  }}
}}
</script>
</body></html>"""

        out_path = LINKS_DIR / f"batch_{batch_idx+1}.html"
        out_path.write_text(html, encoding="utf-8")

    # Save mapping
    MAPPING_FILE.write_text(json.dumps(mapping, indent=2))

    # Save target list
    pd.DataFrame(all_targets).to_csv(PROC / "institutional_targets.csv", index=False)

    print(f"  Saved {n_batches} HTML files in {LINKS_DIR}")
    print(f"  Saved DOI→PMID mapping ({len(mapping)} entries)")
    print(f"\n  Open first batch in your browser:")
    print(f"    {LINKS_DIR / 'batch_1.html'}")
    print(f"\n  Then start the watcher in Anaconda Prompt:")
    print(f"    conda activate thalamus-embed")
    print(f"    python -u \"{Path(__file__)}\" --watch")


# ══════════════════════════════════════════════════════════════════════════
# TEXT EXTRACTION
# ══════════════════════════════════════════════════════════════════════════

def extract_text_from_pdf(pdf_path):
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        text = "".join(p.get_text() + "\n" for p in doc)
        doc.close()
        return text.strip()
    except:
        return ""


def identify_pmid_from_pdf(pdf_path, text, mapping):
    """Match a downloaded PDF to a PMID via DOI lookup."""
    # Method 1: filename might be a PMID
    try:
        return int(pdf_path.stem)
    except:
        pass

    # Method 2: find DOI in the PDF text → map to PMID
    for doi, pmid in mapping.items():
        # Normalize DOI for matching
        doi_lower = doi.lower()
        if doi_lower in text.lower()[:5000]:
            return pmid

    # Method 3: regex for DOI pattern in first few pages
    for m in re.finditer(r'(10\.\d{4,}/[^\s,;]+)', text[:5000]):
        found_doi = m.group(1).rstrip('.)')
        if found_doi in mapping:
            return mapping[found_doi]

    return None


# ══════════════════════════════════════════════════════════════════════════
# WATCHER
# ══════════════════════════════════════════════════════════════════════════

def update_log(entry):
    """Append to download log CSV."""
    df = pd.DataFrame([entry])
    if LOG_FILE.exists():
        old = pd.read_csv(LOG_FILE)
        df = pd.concat([old, df], ignore_index=True)
        df = df.drop_duplicates(subset="pmid", keep="last")
    df.to_csv(LOG_FILE, index=False)


def watch_downloads(downloads_dir=None, interval=2):
    if downloads_dir is None:
        downloads_dir = Path.home() / "Downloads"

    print("=" * 60)
    print("  PDF WATCHER RUNNING")
    print("=" * 60)
    print(f"  Watching:    {downloads_dir}")
    print(f"  Text output: {TEXT_DIR}")
    print(f"  PDF archive: {PDF_DIR}")
    print(f"  Log file:    {LOG_FILE}")
    print()

    # Load mapping
    if MAPPING_FILE.exists():
        mapping = json.loads(MAPPING_FILE.read_text())
        print(f"  DOI→PMID mappings: {len(mapping)}")
    else:
        mapping = {}
        print("  WARNING: No mapping file. Run --generate first!")

    # Track existing files
    seen = set(f.name for f in downloads_dir.glob("*.pdf"))
    n_existing_inst = len(list(TEXT_DIR.glob("*.txt")))

    print(f"  Existing PDFs in Downloads (ignoring): {len(seen)}")
    print(f"  Already extracted texts: {n_existing_inst}")
    print()
    print("  Waiting for new PDFs...")
    print("  " + "-" * 50)
    print()

    stats = {"processed": 0, "matched": 0, "unmatched": 0,
             "failed": 0, "total_chars": 0, "total_thalam": 0}

    try:
        while True:
            current = set(f.name for f in downloads_dir.glob("*.pdf"))
            new_files = current - seen
            crdownloads = set(f.stem for f in downloads_dir.glob("*.crdownload"))

            for fname in sorted(new_files):
                if fname.replace(".pdf", "") in crdownloads:
                    continue  # still downloading

                pdf_path = downloads_dir / fname
                seen.add(fname)

                # Skip tiny files
                try:
                    size = pdf_path.stat().st_size
                except:
                    continue
                if size < 5000:
                    print(f"  SKIP (too small, {size}b): {fname}")
                    stats["failed"] += 1
                    continue

                # Extract text
                text = extract_text_from_pdf(pdf_path)
                if not text or len(text) < 200:
                    print(f"  SKIP (no text extracted): {fname}")
                    stats["failed"] += 1
                    continue

                thal_count = text.lower().count("thalam")
                pmid = identify_pmid_from_pdf(pdf_path, text, mapping)

                if pmid:
                    # Save text + archive PDF
                    txt_path = TEXT_DIR / f"{pmid}.txt"
                    txt_path.write_text(text, encoding="utf-8")
                    import shutil
                    shutil.copy2(str(pdf_path), str(PDF_DIR / f"{pmid}.pdf"))

                    stats["matched"] += 1
                    stats["total_chars"] += len(text)
                    stats["total_thalam"] += thal_count

                    update_log({
                        "pmid": pmid, "status": "success",
                        "n_chars": len(text), "n_thalam": thal_count,
                        "source_file": fname,
                        "timestamp": datetime.now().isoformat(),
                    })

                    print(f"  OK  PMID {pmid:>10}  |  {len(text):>7,} chars  |  "
                          f"{thal_count:>3} thalam  |  {fname[:40]}")
                else:
                    # Save with unknown tag
                    safe = re.sub(r'[^\w\-.]', '_', pdf_path.stem)[:50]
                    txt_path = TEXT_DIR / f"unmatched_{safe}.txt"
                    txt_path.write_text(text, encoding="utf-8")
                    stats["unmatched"] += 1
                    stats["total_chars"] += len(text)

                    print(f"  ??  unknown PMID   |  {len(text):>7,} chars  |  "
                          f"{thal_count:>3} thalam  |  {fname[:40]}")

                stats["processed"] = stats["matched"] + stats["unmatched"]

                # Print running stats every 10 papers
                if stats["processed"] % 10 == 0 and stats["processed"] > 0:
                    print()
                    print(f"  --- Stats: {stats['matched']} matched, "
                          f"{stats['unmatched']} unmatched, "
                          f"{stats['failed']} failed, "
                          f"{stats['total_chars']:,} total chars, "
                          f"{stats['total_thalam']:,} thalam mentions ---")
                    print()

            time.sleep(interval)

    except KeyboardInterrupt:
        print()
        print("=" * 60)
        print("  WATCHER STOPPED")
        print("=" * 60)
        print(f"  Matched:   {stats['matched']}")
        print(f"  Unmatched: {stats['unmatched']}")
        print(f"  Failed:    {stats['failed']}")
        print(f"  Total text: {stats['total_chars']:,} chars")
        print(f"  Total thalam mentions: {stats['total_thalam']:,}")
        n_inst = len(list(TEXT_DIR.glob("*.txt")))
        print(f"  Total institutional texts: {n_inst}")
        print(f"  Log: {LOG_FILE}")


# ══════════════════════════════════════════════════════════════════════════
# STATUS
# ══════════════════════════════════════════════════════════════════════════

def show_status():
    """Show current download progress."""
    print("=" * 60)
    print("  INSTITUTIONAL DOWNLOAD STATUS")
    print("=" * 60)

    targets, n_total, n_have = get_remaining_targets()
    n_inst = len(list(TEXT_DIR.glob("*.txt")))
    n_inst_matched = len([f for f in TEXT_DIR.glob("*.txt")
                          if not f.name.startswith("unmatched_")])

    print(f"  Total DOI papers:        {n_total:,}")
    print(f"  Have full text (all):    {n_have:,}")
    print(f"  Remaining:               {len(targets):,}")
    print()
    print(f"  Institutional downloads: {n_inst}")
    print(f"    Matched to PMID:       {n_inst_matched}")
    print(f"    Unmatched:             {n_inst - n_inst_matched}")

    if LOG_FILE.exists():
        log = pd.read_csv(LOG_FILE)
        print(f"\n  Download log: {len(log)} entries")
        if "n_chars" in log.columns:
            print(f"    Total chars: {log['n_chars'].sum():,}")
        if "n_thalam" in log.columns:
            print(f"    Total thalam: {log['n_thalam'].sum():,}")

    # Check batches
    batches = list(LINKS_DIR.glob("batch_*.html"))
    print(f"\n  Batch files: {len(batches)}")
    if batches:
        print(f"    Location: {LINKS_DIR}")


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LibKey PDF downloader + watcher for UvA")
    parser.add_argument("--generate", action="store_true",
                        help="Generate HTML batch files")
    parser.add_argument("--max-batches", type=int, default=None,
                        help="Limit number of batch files")
    parser.add_argument("--watch", action="store_true",
                        help="Watch Downloads folder for new PDFs")
    parser.add_argument("--watch-dir", type=str, default=None,
                        help="Custom Downloads folder path")
    parser.add_argument("--status", action="store_true",
                        help="Show current download progress")
    args = parser.parse_args()

    if args.generate:
        generate_html_batches(max_batches=args.max_batches)
    elif args.watch:
        watch_dir = Path(args.watch_dir) if args.watch_dir else None
        watch_downloads(watch_dir)
    elif args.status:
        show_status()
    else:
        parser.print_help()
        print("\n  Quick start:")
        print("    1. python 20_generate_libkey_links.py --generate")
        print("    2. python -u 20_generate_libkey_links.py --watch  (Anaconda Prompt)")
        print("    3. Open batch_1.html in Chrome → check 'remember 24h' → DOWNLOAD PDF")
        print("    4. Click 'Auto-Open All' → PDFs download automatically!")
        print("    5. python 20_generate_libkey_links.py --status")
