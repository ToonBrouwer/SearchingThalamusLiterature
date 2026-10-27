# Setting up on a new computer

The project has two parts that travel separately:

- **Code** (`Code/`): this git repository. It can come from GitHub or from the drive.
- **Everything else** (`Data/`, `Graphs/`, `Paper/`, root files): 4.1 GB, which
  travels only by drive. `MANIFEST.csv` in the project root lists every file
  with its size and SHA-256 checksum.

## 1. On the old computer: copy to the drive

Rebuild the manifest first, in case files changed since it was last written
(about 2 minutes):

```bash
python Code/tools/data_manifest.py build
```

Then copy the whole project folder. On Windows, robocopy can resume an
interrupted copy. Replace `E:` with the drive letter:

```powershell
robocopy "C:\Users\brouw\Documents\PhD\Projects\LiteratureQueeryThalamusTime" "E:\LiteratureQueeryThalamusTime" /E /R:1 /W:1 /XD __pycache__ .ipynb_checkpoints chrome_profile_copy /XF .env *.pyc
```

This copies `Code/` together with its `.git` folder, so the drive holds the
full git history. Left out on purpose:

| Excluded | Why |
|----------|-----|
| `.env` | Holds the Anthropic API key. It is only used by legacy scripts; recreate it from `.env.example` if needed |
| `Data/interim/chrome_profile_copy/` | A copy of a Chrome browser profile (LibKey downloader), which can contain login cookies. Never needed again |
| `__pycache__`, `*.pyc` | Python caches |

Check the copy on the drive before wiping or leaving the old computer:

```bash
python Code/tools/data_manifest.py verify --root E:/LiteratureQueeryThalamusTime
```

## 2. On the new computer: place the folder

Copy `LiteratureQueeryThalamusTime/` from the drive to any location and keep
the layout as it is. Any parent folder works.

If you would rather take the code from GitHub, copy everything except `Code/`
and clone into it:

```bash
git clone https://github.com/ToonBrouwer/SearchingThalamusLiterature.git Code
```

Either way, check the data. The tool needs only Python's standard library, so
it runs before the conda environment exists:

```bash
python Code/tools/data_manifest.py verify            # full checksum check
python Code/tools/data_manifest.py verify --quick    # sizes only, seconds
```

## 3. Python environment

Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html), then from the project root:

```bash
conda env create -f Code/environment.yml
conda activate thalamus-embed
```

- `environment.yml` is portable (Windows, macOS, Linux), with versions pinned
  to the working setup.
- `environment.lock.yml` is the exact Windows + NVIDIA export (CUDA 12.6
  torch). Use it only on a comparable Windows machine.
- A GPU is needed only to re-embed sentences. Everything in `paper/` and
  `llm/` runs on CPU.

On Windows, set UTF-8 output before running scripts. Some scripts print
symbols such as τ and →:

```powershell
$env:PYTHONIOENCODING = "utf-8"
```

## 4. Smoke test

```bash
python Code/paper/v18_figures.py --figs 3            # one figure, ~1 min
python Code/paper/v18_figures.py --figs all          # all 16, a few minutes
python Code/llm/relay_sentence_merge.py              # relay coding report
```

Figures go to `Graphs/pub/`. The relay report goes to
`Data/processed/v18/llm_relay/relay_sentence_report.txt`. On 2026-10-27 a copy
holding only the code and the `core` data tier, at a different location,
rebuilt all 16 figures and reproduced the relay report exactly.

## 5. Claude Code

Open Claude Code in the project root. `CLAUDE.md` there loads
`Code/CLAUDE.md`, which has the project rules and pointers. The decision log
with all results is `Paper/v18_figure_plan.md`.

Claude's auto-memory from the old machine is copied to `Paper/claude_memory/`
for reference. To restore it, copy those files into the new machine's memory
folder for this project (`~/.claude/projects/<project-path-slug>/memory/`).
Claude Code creates that folder after the first session in the project.
