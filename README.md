# KristenRaceDirector

Automation daemon for MultiViewer for F1 that dynamically rotates onboard camera windows during live races.

## Prerequisites

- **MultiViewer for F1** running with one or more onboard player windows open
- **OpenF1 subscription** for live data (or use unauthenticated for historical-only)
- Python 3.11+

## Quick start

### 1. Install

```bash
cd RaceDirector
pip install -e .
```

### 2. Configure credentials (for live data)

**Option A – env vars (recommended):**

```bash
export OPENF1_USERNAME="your-email@example.com"
export OPENF1_PASSWORD="your-api-key"
```

**Option B – config file:**

Create `config.local.yaml` (gitignored) with your credentials:

```yaml
openf1:
  username: "your-email@example.com"
  password: "your-api-key"
```

### 3. Test auth (optional)

```bash
OPENF1_USERNAME="..." OPENF1_PASSWORD="..." python scripts/test_auth.py
```

If you see "Auth test PASSED", credentials and token flow work.

### 4. Run the daemon

1. Start **MultiViewer for F1** and open some onboard player windows.
2. Run the daemon:

   ```bash
   # With env vars
   python -m race_director

   # Or with config file
   python -m race_director -c config.local.yaml
   ```

3. On race day, the daemon will poll OpenF1, score drivers, and switch camera windows to the most interesting onboards.

### Dry run (no camera switching)

To verify everything without changing MultiViewer:

```bash
python -m race_director --dry-run
```

### Run in background

```bash
nohup python -m race_director > race_director.log 2>&1 &
# Or with env vars:
nohup env OPENF1_USERNAME="..." OPENF1_PASSWORD="..." python -m race_director > race_director.log 2>&1 &
```

## What the daemon needs from you

- **OpenF1 credentials** (email + password) – for live session data
- **MultiViewer running** – with 1 or more onboard player windows open
- The daemon works with any number of onboard windows (1 to X); it detects and manages them automatically
