"""
Analysis 3 — 'Not merely a relay': explicit challenges to the relay framing.

Searches all thalamus sentences (not just the framing subset) for phrases that
directly contest or complicate the relay metaphor, such as "not merely a relay",
"more than just a relay", or "relay station … but".  Produces a temporal trend
of how many such challenge sentences appear per year, and examines what
alternative language replaces 'relay' in these sentences.

Inputs (Data/interim/sentence_extraction/):
    thalamus_all_sentences.csv   — full sentence corpus from extract_framing_sentences.py

Outputs (Data/processed/):
    not_merely_relay_sentences.csv   — all matched challenge sentences with sid/pmid/year

Outputs (Graphs/):
    fig9_not_merely_relay_trend.png  — 2-panel: raw counts + % of relay-mentioning
                                       sentences (5-yr rolling)

Note on paths:
    PROC, INTERIM, and GRAPH are resolved from __file__; Code/config.py /
    paths.py are not wired in here.
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import re
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT   = _ANALYSIS_DIR.parents[1]
PROC   = ROOT / "Data" / "processed"
INTERIM= ROOT / "Data" / "interim"
GRAPH  = ROOT / "Graphs"

# ── Load data ────────────────────────────────────────────────────────────────
# Use all thalamus sentences (not just framing) to catch challenge language
# that may not have been picked up by the framing detectors.
all_s = pd.read_csv(INTERIM / "sentence_extraction" / "thalamus_all_sentences.csv",
                    dtype={"pmid": str, "pmcid": str})
all_s["year"] = pd.to_numeric(all_s["year"], errors="coerce")

# ── Patterns: challenging the relay framing ───────────────────────────────────
CHALLENGE_PATTERNS = [
    r"not\s+(?:merely|simply|just|only)\s+a\s+relay",
    r"more\s+than\s+(?:a|just\s+a|simply\s+a)\s+relay",
    r"beyond\s+(?:a|its\s+role\s+as\s+a)\s+relay",
    r"not\s+(?:merely|simply|just)\s+(?:a\s+)?(?:passive\s+)?relay",
    r"relay\s+(?:station\s+)?(?:is\s+)?(?:an\s+)?(?:over)?simplif",
    r"(?:classical|traditional|conventional|simple|passive)\s+relay",
    r"relay\s+station[^.]{0,80}(?:but|however|rather|more|also|beyond)",
    r"(?:but|however|rather|more|also)\s+[^.]{0,60}relay\s+station",
    r"misconception[^.]{0,60}relay",
    r"relay[^.]{0,60}misconception",
    r"redefin[^.]{0,40}relay",
    r"outdated[^.]{0,40}(?:notion|concept|view)[^.]{0,40}relay",
]

combined = re.compile("|".join(CHALLENGE_PATTERNS), re.IGNORECASE)

mask = all_s["sentence"].apply(lambda s: bool(combined.search(str(s))))
challenge_df = all_s[mask].copy()
challenge_df["sid"] = challenge_df["pmid"] + "_" + challenge_df["sentence_index"].astype(str)

print(f"Sentences challenging relay framing: {len(challenge_df)}")
print(f"Unique papers: {challenge_df['pmid'].nunique()}")
print(f"Year range: {challenge_df['year'].dropna().min():.0f} – {challenge_df['year'].dropna().max():.0f}")

# Save
challenge_df[["sid","pmid","pmcid","year","journal","sentence"]].to_csv(
    PROC / "not_merely_relay_sentences.csv", index=False)

# ── Temporal trend ────────────────────────────────────────────────────────────
yr_challenge = (challenge_df.groupby("year").size()
                            .rename("challenge"))
yr_all       = (all_s[all_s["sentence"].str.contains(r"\brelay\b", case=False, na=False)]
                     .groupby("year").size()
                     .rename("relay_total"))
trend = pd.concat([yr_challenge, yr_all], axis=1).fillna(0)
trend = trend[(trend.index >= 1980) & (trend.index <= 2026)]
trend["challenge_pct"] = (trend["challenge"] / trend["relay_total"].replace(0,1) * 100)
trend_smooth = trend[["challenge_pct"]].rolling(5, min_periods=2, center=True).mean()

fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)

axes[0].bar(yr_challenge.index, yr_challenge.values, color="#E8342A", alpha=0.7, width=0.8)
axes[0].set_ylabel("n sentences"); axes[0].set_title("'Not merely a relay' sentences per year")

axes[1].plot(trend_smooth.index, trend_smooth["challenge_pct"],
             color="#E8342A", lw=2)
axes[1].fill_between(trend_smooth.index, trend_smooth["challenge_pct"],
                     alpha=0.2, color="#E8342A")
axes[1].set_ylabel("% of relay-mentioning sentences")
axes[1].set_xlabel("Year")
axes[1].set_title("Challenge sentences as % of all relay-mentioning sentences (5-yr rolling)")

plt.tight_layout()
fig.savefig(GRAPH / "fig9_not_merely_relay_trend.png", dpi=150)
plt.close()

# ── What replaces relay in these sentences? ───────────────────────────────────
replace_patterns = {
    "hub":         r"\bhub\b",
    "integrat":    r"\bintegrat",
    "modula":      r"\bmodulat",
    "gate / filter": r"\b(gat(e|eway)|filter)\b",
    "active":      r"\bactive\b",
    "dynamic":     r"\bdynamic\b",
    "complex":     r"\bcomplex\b",
    "network":     r"\bnetwork\b",
    "coordinator": r"\bcoordinat",
}
for label, pat in replace_patterns.items():
    n = challenge_df["sentence"].str.contains(pat, case=False, na=False).sum()
    print(f"  {label:20s}: {n} ({n/len(challenge_df)*100:.1f}%)")

# ── Print examples ────────────────────────────────────────────────────────────
print("\n── Example sentences ─────────────────────────────────────")
for _, row in challenge_df.sample(min(15, len(challenge_df)), random_state=42).iterrows():
    print(f"\n  [{int(row['year']) if pd.notna(row['year']) else '?'}] {str(row['sentence'])[:300]}")

print("\nSaved fig9_not_merely_relay_trend.png")
print("Saved not_merely_relay_sentences.csv")
