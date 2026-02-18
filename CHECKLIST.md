# LibriEncode v1 Checklist

Goal: ship a safe, resumable AV1 encode pipeline with a minimal project layout.

## Simple Directory Plan
- [x] Keep implementation in one main file (example: `libriencode.py`).
- [x] Keep one example config file (`config.example.yaml`).
- [x] Keep one checklist file (`CHECKLIST.md`) as the execution tracker.
- [x] Only split into modules after v1 is stable.

## Quality Guardrails (Ongoing)
- [x] Phase 1 code kept professional and maintainable (clear structure, type hints, minimal duplication).
- [x] Phase 1 avoids comment-heavy style (comments only where needed).
- [x] Phase 1 avoids redundant logic paths and unnecessary work.
- [x] Phase 2 code kept professional and maintainable (clear function boundaries, minimal duplication).
- [x] Phase 2 preserves existing Phase 1 dry-run and state bootstrap behavior.
- [x] Phase 2 keeps comments minimal and non-redundant.

## Phase 1 - Foundation (Config, CLI, State, Logging)
Definition of done: tool can run a dry pass, discover work, and persist state without encoding.

- [x] Create CLI with flags:
  - [x] roots: `--input-root`, `--output-root`, `--state-db`
  - [x] config/logging: `--config`, `--log-path`, `--json-logs`, `--dry-run`
  - [x] encoding knobs: `--crf`, `--preset`, `--tenbit`, `--no-tenbit`, `--audio`, `--audio-bitrate`, `--container`, `--concurrency`
  - [x] safety knobs: `--max-attempts`, `--on-missing-root`, `--retry-seconds`, `--retry-count`, `--delete-bad-final`
- [x] Add config loading (YAML) and CLI-overrides-config behavior.
- [x] Implement startup validation:
  - [x] ffmpeg/ffprobe availability check
  - [x] input/output root policy (`retry|skip|fail`)
  - [x] create output root if missing (when policy allows)
- [x] Create SQLite state DB at default: `output_root/.av1-encode-state.sqlite`.
- [x] Create `jobs` table with required fields and indexes (status, show_name, season_name).
- [x] Add human logs and optional JSONL event logs.
- [x] Add run summary counters structure (done/skipped/failed/deleted/cleanup counts).
- [x] Implement scanner + planner only (no encode yet):
  - [x] detect `input_root/<Show>/<Season */files>`
  - [x] extension allowlist from config
  - [x] planned destination path under `output_root/<Show>/<Season>/`
  - [x] filename sanitization for invalid path characters
- [x] Implement `--dry-run` output: planned actions only, no writes/deletes.
- [x] Phase 1 validation:
  - [x] run with test tree and confirm planner output
  - [x] rerun dry-run and confirm idempotent results
  - [x] confirm DB rows created/updated without encoding

## Phase 2 - Core Pipeline (Encode, Verify, Finalize, Safe Delete)
Definition of done: one full pass safely encodes, verifies, finalizes atomically, and deletes source only on success.

- [x] Implement per-file state flow:
  - [x] `pending -> encoding -> verifying -> done`
  - [x] `failed` path with `attempt_count` and `last_error`
  - [x] skip `done` files on reruns
- [x] Build ffmpeg command generator:
  - [x] video: `libsvtav1`, configurable `crf/preset`, optional 10-bit
  - [x] audio: `aac` or `opus` with bitrate setting
  - [x] subtitles: copy for MKV default
  - [x] stream map default: `-map 0:v:0 -map 0:a? -map 0:s?`
- [x] Encode to temp file in destination season folder:
  - [x] default temp suffix `.tmp`
  - [x] ensure same-directory temp/final for atomic rename
- [x] Implement verification gate using ffprobe:
  - [x] final probe succeeds
  - [x] video codec is AV1
  - [x] duration sanity check against input (tolerance configurable)
  - [x] min output bytes threshold check
- [x] Finalize flow:
  - [x] atomic rename temp -> final
  - [x] mark state `done`
  - [x] delete original input only after state commit
- [x] Reconciliation rules on rerun:
  - [x] if final exists and state not `done`, verify and reconcile
  - [x] if invalid final, mark failed and keep for inspection unless `--delete-bad-final`
- [x] Naming strategy implementation:
  - [x] preserve show and season folder names
  - [x] best-effort episode extraction (SxxEyy / Episode NN / EpNN / `- NN`)
  - [x] fallback to sanitized original base name
- [x] Phase 2 validation:
  - [x] successful file encodes and source deleted
  - [x] corrupt file fails and source retained
  - [x] final file appears only after rename (no partial final filename exposure)

## Phase 3 - Resilience, Cleanup, and Acceptance
Definition of done: interruption-safe long-run behavior plus acceptance scenarios from PRD.

- [ ] Startup recovery:
  - [ ] detect and delete leftover `.tmp` outputs
  - [ ] reset/requeue non-`done` in-progress states for retry
- [ ] Error tolerance loop:
  - [ ] continue processing after per-file errors
  - [ ] enforce `max_attempts`
  - [ ] stable exit codes for automation
- [ ] Missing root/network resilience:
  - [ ] implement retry/backoff with `retry_seconds` + `retry_count`
  - [ ] honor `retry|skip|fail` policy without unsafe deletes
- [ ] Staging cleanup logic:
  - [ ] remove empty staging season folders
  - [ ] remove empty staging show folders
  - [ ] never remove non-empty folders
- [ ] End-of-run observability:
  - [ ] top failure reasons
  - [ ] totals for done/skipped/failed/source deletes/folder deletes
- [ ] Acceptance test checklist (from PRD):
  - [ ] 3-episode happy path in `_incoming/ShowA/Season 01`
  - [ ] interruption mid-encode then rerun resumes without duplicate work
  - [ ] corrupt input is logged, skipped, and preserved
  - [ ] Jellyfin-safe finalize behavior verified
- [ ] Ops readiness:
  - [ ] provide sample command lines (Windows path + Linux path)
  - [ ] add example systemd service/timer snippets to docs
  - [ ] document recommended default `concurrency=1` for network mapping

## Stretch (Post-v1, Optional)
- [ ] Jellyfin refresh trigger after completed show/season.
- [ ] Per-show encoding profiles.
- [ ] Metadata integration (TVDB/TMDB).
- [ ] Quarantine folder for failed inputs/finals.
