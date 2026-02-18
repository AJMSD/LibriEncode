# LibriEncode

LibriEncode is a safety-first tool that prepares your TV episodes for AV1 encoding and organizes outputs for Jellyfin.

## Goal (Plain English)
- Take episodes from an `_incoming` folder.
- Plan where each encoded file should go in your library.
- Track progress in a local SQLite database so reruns are safe.
- Encode to AV1, verify output, and only then delete originals.

Phase 2 adds real encoding, verification, atomic finalize, and safe delete rules.

## Current Progress
- Phase 1: Completed on `2026-02-18`
- Phase 2: Completed on `2026-02-18`
- Phase 3: Not started

Detailed tracking lives in `CHECKLIST.md`.

## Simple Project Layout
- `libriencode.py`: main script (single-file implementation)
- `config.example.yaml`: example configuration
- `CHECKLIST.md`: phase-by-phase execution tracker
- `PRD.md`: product requirements source

## Requirements
- Python 3.10+
- `ffmpeg` and `ffprobe` available in your `PATH`
- `PyYAML` for config loading

Install dependency:

```bash
pip install pyyaml
```

## Quick Start (Layman Friendly)
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

## Logs and State
- Human logs: console + optional file (`logging.log_path`)
- Optional JSON events: enabled via `--json-logs` or config
- State DB default: `output_root/.av1-encode-state.sqlite`
- Per-file statuses: `pending`, `encoding`, `verifying`, `done`, `failed`, `skipped`

## Safety Notes
- Source files are deleted only after successful encode + verify + state commit.
- Final files are published via atomic rename from temp to final path.
- Existing final files are reconciled before re-encoding.
- Corrupt files are marked failed and kept in staging.
- `--dry-run` avoids persistent writes.

## Next
Phase 3 will add startup temp cleanup, empty staging folder cleanup, and full acceptance hardening.
