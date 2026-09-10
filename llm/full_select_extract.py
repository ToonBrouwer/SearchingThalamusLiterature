"""
full_select_extract.py — select and extract the FULL Tier-1 sample
(run AFTER 27_repair_institutional.py).

Sample design (agreed 2026-09-02):
  A. Stratified: up to 2 papers per (domain x decade) cell,
     domains = all 22, decades 1970s-2020s  -> ~200 papers
  B. Influential reviews  (influential_reviews_selected.csv)
  C. Influential non-reviews (influential_nonreviews_selected.csv)
     B/C capped at 100 each, ordered as in the selection files.

Eligibility: intro + discussion/conclusion text available from either
  - the institutional text file (verdict 'ok' in the title QC, or
    re-attributed by the repair map), or
  - the paper's PMC XML (sec-title buckets).

Outputs:
  Data/processed/v18/llm_full/<pmid>.txt      (one file per paper)
  Data/processed/v18/llm_full/manifest.csv
"""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "Code"))
sys.path.insert(0, str(ROOT / "Code" / "paper"))
import v18_data as D

PROC = ROOT / "Data" / "processed"
V18 = PROC / "v18"
OUT = V18 / "llm_full"
OUT.mkdir(parents=True, exist_ok=True)
TXT = ROOT / "Data" / "interim" / "institutional_text"
XML_DIR = ROOT / "Data" / "raw" / "pmc_xml"

MAX_WORDS = 1100

HEADER_PATTERNS = [
    ("abstract", r"abstract"),
    ("introduction", r"introduction|background"),
    ("methods", r"(materials?\s+and\s+)?methods?|materials|"
                 r"experimental\s+procedures?|study\s+design"),
    ("results", r"results?(\s+and\s+discussion)?|findings"),
    ("discussion", r"(general\s+)?discussion"),
    ("conclusion", r"conclusions?|concluding\s+remarks|summary"),
    ("references", r"references?|bibliography|literature\s+cited|"
                    r"acknowledge?ments?"),
]
HEADER_RE = re.compile(
    r"^\s{0,8}(?:[0-9IVX]+[\.\)]?\s*)?(" +
    "|".join(f"(?P<{n}>{p})" for n, p in HEADER_PATTERNS) +
    r")\s*[:.]?\s*$", re.IGNORECASE)


def txt_sections(text: str) -> dict:
    spans, pos = [], 0
    for line in text.splitlines(keepends=True):
        st = line.strip()
        if 0 < len(st) <= 45:
            m = HEADER_RE.match(line)
            if m:
                b = next(n for n, _ in HEADER_PATTERNS if m.group(n))
                spans.append((pos, b))
        pos += len(line)
    out = {}
    for i, (p, b) in enumerate(spans):
        end = spans[i + 1][0] if i + 1 < len(spans) else len(text)
        out[b] = out.get(b, "") + text[p:end]
    return out


def classify_title(t: str) -> str:
    for b, pat in HEADER_PATTERNS[1:]:
        if re.search(pat, t or "", re.I):
            return b
    return "other"


def pmc_sections(xml_path: Path) -> dict | None:
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError:
        return None
    root = tree.getroot()
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    body = root.find(".//body")
    if body is None:
        return None
    out = {}
    for sec in body.findall("sec"):
        title_el = sec.find("title")
        b = classify_title(title_el.text if title_el is not None else "")
        text = " ".join(" ".join(p.itertext()) for p in sec.iter("p"))
        out[b] = out.get(b, "") + " " + text
    return out


def clean(text: str) -> str:
    words = re.sub(r"\s+", " ", text or "").split()
    return " ".join(words[:MAX_WORDS])


# ── Build eligibility indices ────────────────────────────────────────────
qc = pd.read_csv(V18 / "institutional_title_check.csv")
ok_inst = set(qc[qc.verdict == "ok"].pmid)
repair = pd.read_csv(V18 / "institutional_repair_map.csv")
file_of = {int(r.new_pmid): TXT / f"{int(r.old_pmid)}.txt"
           for r in repair.itertuples()}
for p in ok_inst:
    file_of.setdefault(int(p), TXT / f"{int(p)}.txt")

pmc_of = {}
for f in XML_DIR.glob("*.xml"):
    pass  # pmid lookup below via section map (cheaper than parsing all)
pmc_pmids = set(pd.read_csv(V18 / "pmc_sentence_sections.csv",
                             usecols=["pmid"]).pmid.unique())
# map pmid -> PMC file via article-id would need parsing; instead parse
# lazily at extraction time using an index built from the sections step
pmc_index_file = V18 / "pmc_pmid_to_file.csv"
if pmc_index_file.exists():
    pmc_of = {int(r.pmid): Path(r.path) for r in
              pd.read_csv(pmc_index_file).itertuples()}
else:
    print("Building PMC pmid->file index (one-off) ...")
    rows = []
    for f in sorted(XML_DIR.glob("*.xml")):
        try:
            head = f.read_text(encoding="utf-8", errors="replace")[:4000]
        except OSError:
            continue
        m = re.search(r'pub-id-type="pmid">(\d+)<', head)
        if m:
            rows.append((int(m.group(1)), str(f)))
    pd.DataFrame(rows, columns=["pmid", "path"]).to_csv(pmc_index_file,
                                                         index=False)
    pmc_of = {p: Path(fp) for p, fp in rows}
print(f"Sources: {len(file_of):,} institutional, {len(pmc_of):,} PMC")


def extract(pmid: int) -> tuple[str, str] | None:
    """Return (intro, discussion-or-conclusion) or None."""
    if pmid in file_of and file_of[pmid].exists():
        secs = txt_sections(file_of[pmid].read_text(encoding="utf-8",
                                                     errors="replace"))
        intro = clean(secs.get("introduction", ""))
        disc = clean(secs.get("discussion", "")
                      or secs.get("conclusion", ""))
        if len(intro.split()) >= 150 and len(disc.split()) >= 150:
            return intro, disc
    if pmid in pmc_of:
        secs = pmc_sections(pmc_of[pmid])
        if secs:
            intro = clean(secs.get("introduction", ""))
            disc = clean(secs.get("discussion", "")
                          or secs.get("conclusion", ""))
            if len(intro.split()) >= 150 and len(disc.split()) >= 150:
                return intro, disc
    return None


# ── Assemble the sample ──────────────────────────────────────────────────
dom = D.load_domains()
raw = pd.read_csv(D.RAW / "pubmed" /
                  "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
                  usecols=["pmid", "title", "journal", "year"],
                  low_memory=False).drop_duplicates("pmid")
raw["year"] = pd.to_numeric(raw.year, errors="coerce")
meta = raw.set_index("pmid")
pool = dom.merge(raw, on="pmid", how="left").dropna(subset=["year"])
pool["year"] = pool.year.astype(int)
pool["decade"] = (pool.year // 10) * 10
pool = pool[(pool.decade >= 1970) & (pool.decade <= 2020)]
eligible_any = set(file_of) | set(pmc_of)
pool = pool[pool.pmid.isin(eligible_any)]

picks: list[tuple[int, str]] = []
seen = set()
for (d_, dec), sub in pool.groupby(["domain", "decade"]):
    for pmid in sub.sample(frac=1, random_state=42).pmid.head(4):
        if pmid not in seen:
            picks.append((int(pmid), "stratified"))
            seen.add(pmid)
        if sum(1 for p, k in picks
               if k == "stratified" and p in set(sub.pmid)) >= 2:
            break

for name, kind, cap in [("influential_reviews_selected.csv", "review", 100),
                         ("influential_nonreviews_selected.csv",
                          "nonreview", 100)]:
    sel = pd.read_csv(PROC / name, low_memory=False)
    n = 0
    for pmid in sel.pmid:
        pmid = int(pmid)
        if pmid in seen or pmid not in eligible_any:
            continue
        picks.append((pmid, kind))
        seen.add(pmid)
        n += 1
        if n >= cap:
            break

print(f"Candidate picks: {len(picks)} "
      f"({pd.Series([k for _, k in picks]).value_counts().to_dict()})")

# ── Extract and write ────────────────────────────────────────────────────
rows, n_fail = [], 0
for i, (pmid, kind) in enumerate(picks):
    if i % 50 == 0:
        print(f"  extracting {i}/{len(picks)}")
    got = extract(pmid)
    if got is None:
        n_fail += 1
        continue
    intro, disc = got
    m = meta.loc[pmid]
    body = (f"Title: {m.title}\nYear: {int(m.year)}\n"
            f"Journal: {m.journal}\n\n"
            f"=== INTRODUCTION ===\n{intro}\n\n"
            f"=== DISCUSSION ===\n{disc}\n")
    (OUT / f"{pmid}.txt").write_text(body, encoding="utf-8")
    drow = dom[dom.pmid == pmid]
    rows.append({
        "pmid": pmid, "kind": kind, "year": int(m.year),
        "decade": int(m.year) // 10 * 10,
        "domain": int(drow.domain.iloc[0]) if len(drow) else -1,
        "domain_name": (drow.domain_name.iloc[0] if len(drow)
                         else "(not clustered)"),
        "journal": m.journal, "title": str(m.title)[:140],
        "n_words": len(intro.split()) + len(disc.split()),
        "source": ("institutional" if pmid in file_of else "pmc"),
    })

man = pd.DataFrame(rows)
man.to_csv(OUT / "manifest.csv", index=False)
print(f"\nExtracted {len(man):,} papers ({n_fail} failed extraction)")
print(man.kind.value_counts().to_string())
print("\nBy decade:")
print(man.decade.value_counts().sort_index().to_string())
