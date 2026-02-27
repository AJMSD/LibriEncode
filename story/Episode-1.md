# Episode 1

## 1. Context for This Episode
This episode covered the build-out and hardening of LibriEncode from planning into a usable end-to-end workflow.

The rough goal was to move from PRD/checklist-driven phase execution (Phase 1 through Phase 4) into a stable daily-use encoder flow with clear docs, safe behavior, better observability, and fixes for real runtime edge cases.

## 2. Main Problems We Faced
### Issue A: Phase Execution Needed Tight Quality Control
- Symptoms: repeated requirements to fully implement each phase, avoid regressions, avoid syntax errors, keep code professional, avoid redundancy, and keep commits incremental.
- Why it mattered: this guarded architecture quality while features were added quickly across multiple phases.

### Issue B: Repo Hygiene and Privacy Gaps
- Symptoms: request to remove `PRD.md` and `CHECKLIST.md` from GitHub history going forward and remove personal information from `config.example.yaml`.
- Why it mattered: shipping internal planning docs and personal values in examples creates privacy and repository cleanliness risks.

### Issue C: Config Semantics Were Unclear (`show_profiles.match`)
- Symptoms: confusion around “show patterns” and whether an empty value matches anything.
- Why it mattered: misunderstanding pattern matching can silently disable per-show encoding overrides.

### Issue D: Processing Order Was Wrong (Lexicographic vs Numeric)
- Symptoms: run jumped from episode 1 to episode 10 while episode 2 existed.
- Why it mattered: wrong ordering harms trust, makes progress feel random, and complicates long-batch operational monitoring.

### Issue E: Progress Visibility Was Insufficient
- Symptoms: user asked whether progress logging was possible and requested file-level completion counts by default.
- Why it mattered: long encodes without clear progress make operations hard to manage and estimate.

### Issue F: Audio Codec/Container Choice for Jellyfin Needed Clarity
- Symptoms: user asked about using stereo audio instead of Opus, what config change is needed, and whether AAC or Opus is better for AV1 + Jellyfin.
- Why it mattered: codec choices impact compatibility, transcoding behavior, and playback reliability.

### Issue G: ffmpeg Appeared to Hang Near Completion
- Symptoms: logs reached ~98.6% then repeated `ffmpeg running speed N/A` every ~5 seconds for a long period.
- Why it mattered: an indefinite hang blocks throughput and can stall large batch runs for hours.

## 3. Debugging Path & Options Considered
### Issue A: Phase Execution Needed Tight Quality Control
- Helpful: use phased checklist + README updates as control points while implementing features.
- Helpful: enforce incremental push/commit discipline so rollback is easier.
- Dead end: none explicit; this was process control rather than a single bug.

### Issue B: Repo Hygiene and Privacy Gaps
- Helpful: treat this as a publishing concern (what goes to GitHub) rather than a runtime bug.
- Option considered: keep docs local but not push; chosen direction was to remove sensitive/non-public artifacts from published repo state.
- Dead end: none explicit.

### Issue C: Config Semantics Were Unclear (`show_profiles.match`)
- Helpful: clarify that pattern matching is based on show-name matching rules and empty patterns do not match shows.
- Option considered: leave pattern empty vs define explicit pattern; explicit pattern is the usable path.
- Dead end: assuming empty would “do something.”

### Issue D: Processing Order Was Wrong (Lexicographic vs Numeric)
- Helpful: identify that filesystem/string sorting causes `1,10,11,...,2` behavior.
- Option considered: keep current order vs implement natural numeric sort; natural sort was selected.
- Dead end: relying on plain lexical ordering for episodic media.

### Issue E: Progress Visibility Was Insufficient
- Helpful: add periodic progress logs and include completed-count out of total files.
- Option considered: make progress display optional via CLI flag vs default behavior; default behavior was selected.
- Dead end: keeping minimal logs for long-running jobs.

### Issue F: Audio Codec/Container Choice for Jellyfin Needed Clarity
- Helpful: treat codec as config-level decision, not code change.
- Options discussed: Opus vs AAC; stereo output via codec/channel configuration.
- Decision direction: prioritize playback compatibility expectations for Jellyfin workflows when selecting codec.
- Dead end: none explicit.

### Issue G: ffmpeg Appeared to Hang Near Completion
- Helpful discovery: parser was repeatedly logging `speed N/A` without detecting that meaningful progress had stopped.
- Helpful discovery: this looked like a real stall pattern (not just a short finalize phase) due to prolonged repetition.
- Options considered: add a stall watchdog timeout and fail fast, or keep waiting indefinitely and rely on manual interruption.
- Chosen path: the watchdog option.
- Dead end: assuming `N/A` lines would naturally resolve quickly every time.

## 4. Final Solution Used (For This Chat)
### Issue A: Phase Execution Needed Tight Quality Control
- Decision: continue phase-by-phase implementation with explicit “no breakage/no syntax errors” guardrails and short incremental commits.
- Files/layers involved: implementation in `libriencode.py`, tracking in `CHECKLIST.md`, user-facing updates in `README.md`.

### Issue B: Repo Hygiene and Privacy Gaps
- Decision: remove non-public planning docs from GitHub scope and sanitize example config values.
- Files/layers involved: repository content policy + `config.example.yaml` cleanup.

### Issue C: Config Semantics Were Unclear (`show_profiles.match`)
- Decision: document/clarify that empty match patterns do not match anything, so explicit patterns are required for overrides.
- Files/layers involved: configuration behavior and user guidance.

### Issue D: Processing Order Was Wrong (Lexicographic vs Numeric)
- Decision: switch to true numeric/natural ordering for episodic files.
- Files/layers involved: planner/scan ordering logic in `libriencode.py`.
- Conceptual change: ordering moved from lexical filename sort to number-aware sort.

### Issue E: Progress Visibility Was Insufficient
- Decision: implement default progress logging (no extra flag) and include encoded-count context versus total files.
- Files/layers involved: runtime logging in `libriencode.py`, usage notes in `README.md`.

### Issue F: Audio Codec/Container Choice for Jellyfin Needed Clarity
- Decision: keep codec choice configurable via YAML and guide user on what to change in config (rather than changing core logic).
- Files/layers involved: `config.yaml` / `config.example.yaml` encoding settings (`audio_codec`, bitrate, related profile fields).

### Issue G: ffmpeg Appeared to Hang Near Completion
- Decision: implement progress-stall watchdog and fail a stuck ffmpeg process after timeout instead of logging `speed N/A` forever.
- Files/layers involved: `libriencode.py` (added `progress_stall_timeout_seconds` config usage, progress parsing fallback, stall detection/termination path), `config.example.yaml` (added `safety.progress_stall_timeout_seconds` default), and `README.md` (documented stall-timeout behavior).
- Conceptual change: progress loop became liveness-aware, not just log-forwarding.

## 5. Tools, APIs, and Concepts Used
- Python CLI app design (`argparse`): used to drive configurable operational behavior.
- YAML config (`PyYAML`): used for runtime defaults and per-show override policy.
- ffmpeg + ffprobe: used for encode execution, progress reporting, and output verification.
- SQLite state tracking: used to make reruns safe and resumable.
- Structured + human logging: used for operational visibility and troubleshooting.
- Natural sorting: used to preserve expected episode sequence.
- Safe finalize workflow: temp output + verify + atomic rename + source delete after success.
- Git incremental commits/pushes: used as rollback-friendly delivery checkpoints.

## 6. Lessons Learned (For This Episode)
- Encode pipelines need liveness checks, not just progress prints.
- Natural sort is mandatory for episodic media UX.
- Safety guarantees should be explicit: verify first, publish atomically, delete source last.
- Operational logs must include both per-file progress and batch-level context.
- Keep defaults practical; do not hide critical observability behind optional flags.
- Configuration clarity prevents silent misconfiguration (especially pattern-based overrides).
- Incremental commits reduce risk during fast, multi-phase implementation.
- Repo hygiene and example-config sanitization are part of production readiness.

## 7. Addendum - Naming Normalization Follow-up
- The naming flow was tightened to a strict final format: `<Show> SXXE...` with no source-title suffix.
- Season and episode parsing were expanded to unlimited digits to avoid truncation (for example, `Episode 1129` -> `E1129`).
- Fractional episodes were standardized to `p` notation (for example, `Episode 1061.5` -> `E1061p5`) to avoid collisions.
- Show/season naming context is now taken from destination folder components used by the planner.
- Files with unparseable season or episode tokens are now skipped at planning time with explicit warning logs/events.
