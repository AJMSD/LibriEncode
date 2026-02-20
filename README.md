# LibriEncode

LibriEncode is a safety-first tool that prepares your TV episodes for AV1 encoding and organizes outputs for Jellyfin.

## Goal
- Take episodes from an `_incoming` folder.
- Plan where each encoded file should go in your library.
- Track progress in a local SQLite database so reruns are safe.
- Encode to AV1, verify output, and only then delete originals.
- Recover cleanly after interruptions by resetting in-progress jobs and removing leftover temp files.
- Remove empty staging season/show folders after successful processing.
- Apply optional per-show encoding profiles for tuned quality/bitrate per series.
- Optionally quarantine failed inputs/finals and optionally trigger Jellyfin refresh after successful runs.

## Simple Project Layout
- `libriencode.py`: main script (single-file implementation)
- `config.example.yaml`: example configuration

## Requirements
- Python 3.10+
- `ffmpeg` and `ffprobe` available in your `PATH`
- `PyYAML` for config loading

Install dependency:

```bash
pip install pyyaml
```

## Quick Start
1. Copy `config.example.yaml` to your own config file (for example `config.yaml`).
2. Edit folder paths (`input_root`, `output_root`) to match your machine.
3. Run a dry run first to see what will be processed.
4. Run normal mode to encode and clean up successful files safely.

### Dry Run (No Writes)

```bash
python libriencode.py --config config.yaml --dry-run
```

What happens:
- Scans `input_root/<Show>/<Season */>`
- Finds eligible video files
- Builds planned output paths
- Prints the plan only

### Normal Run (Encoding Enabled)

```bash
python libriencode.py --config config.yaml
```

What happens:
- Scans and plans work
- Encodes each file to a temp output
- Verifies output with `ffprobe` (AV1 codec + duration check + minimum size)
- Atomically renames temp to final file
- Marks DB state as `done`
- Deletes source only after `done`
- Keeps bad/corrupt inputs and continues
- Removes empty staging season/show folders when possible

## Common CLI Overrides

```bash
python libriencode.py --config config.yaml --input-root A:/mnt/Extreme500/Anime/_incoming --output-root A:/mnt/Extreme500/Anime --container mkv --crf 28 --preset 6
```

You can override config values at runtime for:
- roots: `--input-root`, `--output-root`, `--state-db`
- behavior: `--dry-run`, `--on-missing-root`, `--retry-seconds`, `--retry-count`
- logging: `--log-path`, `--json-logs`
- encoding knobs (stored in profile hash for state): `--crf`, `--preset`, `--tenbit`, `--no-tenbit`, `--audio`, `--audio-bitrate`, `--container`, `--concurrency`
- safety: `--max-attempts`, `--delete-bad-final`
- advanced config-only options:
  - `show_profiles` for per-show encode overrides
  - `safety.quarantine_failed_inputs_root` / `safety.quarantine_failed_finals_root`
  - `safety.progress_stall_timeout_seconds` to stop a stuck ffmpeg process (set `0` to disable)
  - `jellyfin.enabled`, `jellyfin.base_url`, `jellyfin.api_key`

## Logs and State
- Human logs: console + optional file (`logging.log_path`)
- Optional JSON events: enabled via `--json-logs` or config
- State DB default: `output_root/.av1-encode-state.sqlite`
- Per-file statuses: `pending`, `encoding`, `verifying`, `done`, `failed`, `skipped`
- Startup recovery: lingering temp files are removed and `encoding`/`verifying` rows are reset to `pending`
- If ffmpeg stops making progress for too long, LibriEncode aborts that encode attempt and records a clear stall error.
- End summary includes top failure reasons
- End summary also includes quarantine counts and Jellyfin refresh status

## Safety Notes
- Source files are deleted only after successful encode + verify + state commit.
- Final files are published via atomic rename from temp to final path.
- Existing final files are reconciled before re-encoding.
- Corrupt files are marked failed and kept in staging.
- Empty staging season/show folders are removed only when actually empty.
- Optional quarantine moves failed inputs/finals into configured quarantine roots.
- `--dry-run` avoids persistent writes.
