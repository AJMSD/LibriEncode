#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import fnmatch
import hashlib
import json
import logging
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    yaml = None


DEFAULT_CONFIG: dict[str, Any] = {
    "input_root": "./data/incoming",
    "output_root": "./data/library",
    "state_db": None,
    "scan": {
        "season_folder_glob": "Season *",
        "extensions": [".mkv", ".mp4", ".avi", ".mov", ".webm"],
    },
    "encoding": {
        "codec": "libsvtav1",
        "crf": 28,
        "preset": 6,
        "ten_bit": True,
        "container": "mkv",
        "audio_codec": "opus",
        "audio_bitrate_kbps": 128,
        "concurrency": 1,
    },
    "safety": {
        "max_attempts": 3,
        "delete_bad_final": False,
        "temp_suffix": ".tmp",
        "delete_leftover_temp_on_start": True,
        "progress_stall_timeout_seconds": 300,
        "quarantine_failed_inputs_root": None,
        "quarantine_failed_finals_root": None,
        "min_output_bytes": 5000000,
        "duration_tolerance_seconds": 3.0,
    },
    "show_profiles": [],
    "jellyfin": {
        "enabled": False,
        "base_url": "",
        "api_key": "",
        "refresh_after_run": True,
        "min_done_to_refresh": 1,
        "timeout_seconds": 10,
    },
    "roots": {
        "on_missing_root": "retry",
        "retry_seconds": 10,
        "retry_count": 30,
    },
    "logging": {
        "log_path": "./logs/av1-encode.log",
        "json_logs": False,
        "level": "info",
    },
}


INVALID_FILENAME_CHARS = '<>:"/\\|?*'
VALID_STATUSES = {"pending", "encoding", "verifying", "done", "failed", "skipped"}
BINARY_NAMES = ("ffmpeg", "ffprobe")
SEASON_NUMBER_RE = re.compile(r"season\s*(\d{1,2})", re.IGNORECASE)
SXXEYY_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})")
EPISODE_RE = re.compile(r"(?:episode|ep)\s*[-_. ]*(\d{1,3})", re.IGNORECASE)
DASH_NUMBER_RE = re.compile(r"(?:^|[\s_.-])-\s*(\d{1,3})(?:[\s_.-]|$)")
NATURAL_SPLIT_RE = re.compile(r"(\d+)")


@dataclass(frozen=True)
class PlannedJob:
    input_path: str
    show_name: str
    season_name: str
    output_final_path: str
    output_temp_path: str
    ffmpeg_profile_hash: str
    input_size_bytes: int
    input_mtime: float


@dataclass
class Summary:
    discovered_files: int = 0
    planned_jobs: int = 0
    db_upserts: int = 0
    recovered_jobs: int = 0
    deleted_temp_files: int = 0
    done: int = 0
    skipped: int = 0
    failed: int = 0
    deleted_originals: int = 0
    quarantined_inputs: int = 0
    quarantined_finals: int = 0
    deleted_season_folders: int = 0
    deleted_show_folders: int = 0
    top_failure_reasons: list[tuple[str, int]] | None = None
    jellyfin_refresh_attempted: bool = False
    jellyfin_refresh_success: bool = False


class JsonlLogger:
    def __init__(self, enabled: bool, path: str | None, dry_run: bool):
        self.enabled = enabled
        self.path = path
        self._handle = None
        if not enabled:
            return
        if not path:
            self.enabled = False
            return
        if dry_run:
            return
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._handle = target.open("a", encoding="utf-8")

    def emit(self, *, level: str, stage: str, message: str, **fields: Any) -> None:
        if not self.enabled:
            return
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "stage": stage,
            "message": message,
        }
        payload.update(fields)
        line = json.dumps(payload, ensure_ascii=True)
        if self._handle:
            self._handle.write(line + "\n")
            self._handle.flush()
        else:
            print(line)

    def close(self) -> None:
        if self._handle:
            self._handle.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="libriencode",
        description="Safe AV1 library encoder.",
    )
    parser.add_argument("--config", type=str, help="Path to YAML config file.")
    parser.add_argument("--input-root", type=str)
    parser.add_argument("--output-root", type=str)
    parser.add_argument("--state-db", type=str)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-path", type=str)
    parser.add_argument("--json-logs", dest="json_logs", action="store_true")
    parser.add_argument("--crf", type=int)
    parser.add_argument("--preset", type=int)
    parser.add_argument("--tenbit", dest="tenbit", action="store_true")
    parser.add_argument("--no-tenbit", dest="tenbit", action="store_false")
    parser.add_argument("--audio", choices=("aac", "opus"))
    parser.add_argument("--audio-bitrate", type=int)
    parser.add_argument("--container", choices=("mkv", "mp4"))
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--max-attempts", type=int)
    parser.add_argument("--on-missing-root", choices=("retry", "skip", "fail"))
    parser.add_argument("--retry-seconds", type=int)
    parser.add_argument("--retry-count", type=int)
    parser.add_argument("--delete-bad-final", dest="delete_bad_final", action="store_true")
    parser.set_defaults(tenbit=None, json_logs=None, delete_bad_final=None)
    return parser.parse_args()


def deep_merge(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in updates.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_yaml_config(path: str) -> dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is not installed. Install with: pip install pyyaml")
    config_path = Path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("Config file root must be a mapping/object.")
    return data


def apply_cli_overrides(config: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    updated = copy.deepcopy(config)
    if args.input_root is not None:
        updated["input_root"] = args.input_root
    if args.output_root is not None:
        updated["output_root"] = args.output_root
    if args.state_db is not None:
        updated["state_db"] = args.state_db
    if args.log_path is not None:
        updated["logging"]["log_path"] = args.log_path
    if args.json_logs is not None:
        updated["logging"]["json_logs"] = args.json_logs
    if args.crf is not None:
        updated["encoding"]["crf"] = args.crf
    if args.preset is not None:
        updated["encoding"]["preset"] = args.preset
    if args.tenbit is not None:
        updated["encoding"]["ten_bit"] = args.tenbit
    if args.audio is not None:
        updated["encoding"]["audio_codec"] = args.audio
    if args.audio_bitrate is not None:
        updated["encoding"]["audio_bitrate_kbps"] = args.audio_bitrate
    if args.container is not None:
        updated["encoding"]["container"] = args.container
    if args.concurrency is not None:
        updated["encoding"]["concurrency"] = args.concurrency
    if args.max_attempts is not None:
        updated["safety"]["max_attempts"] = args.max_attempts
    if args.on_missing_root is not None:
        updated["roots"]["on_missing_root"] = args.on_missing_root
    if args.retry_seconds is not None:
        updated["roots"]["retry_seconds"] = args.retry_seconds
    if args.retry_count is not None:
        updated["roots"]["retry_count"] = args.retry_count
    if args.delete_bad_final is not None:
        updated["safety"]["delete_bad_final"] = args.delete_bad_final
    return updated


def normalize_config(config: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    normalized = copy.deepcopy(config)
    normalized["input_root"] = str(Path(normalized["input_root"]))
    normalized["output_root"] = str(Path(normalized["output_root"]))
    state_db = normalized.get("state_db")
    if state_db:
        normalized["state_db"] = str(Path(state_db))
    else:
        normalized["state_db"] = str(Path(normalized["output_root"]) / ".av1-encode-state.sqlite")
    extensions = normalized["scan"].get("extensions", [])
    normalized["scan"]["extensions"] = sorted({f".{e.lstrip('.').lower()}" for e in extensions})
    normalized["scan"]["season_folder_glob"] = normalized["scan"].get("season_folder_glob", "Season *")
    normalized["encoding"]["container"] = normalized["encoding"].get("container", "mkv").lower()
    normalized["encoding"]["audio_codec"] = normalized["encoding"]["audio_codec"].lower()
    normalized["encoding"]["concurrency"] = int(normalized["encoding"].get("concurrency", 1))
    normalized["safety"]["max_attempts"] = int(normalized["safety"].get("max_attempts", 3))
    normalized["safety"]["delete_bad_final"] = bool(normalized["safety"].get("delete_bad_final", False))
    normalized["safety"]["temp_suffix"] = str(normalized["safety"].get("temp_suffix", ".tmp"))
    normalized["safety"]["delete_leftover_temp_on_start"] = bool(
        normalized["safety"].get("delete_leftover_temp_on_start", True)
    )
    stall_timeout = int(normalized["safety"].get("progress_stall_timeout_seconds", 300))
    normalized["safety"]["progress_stall_timeout_seconds"] = stall_timeout if stall_timeout >= 0 else 0
    quarantine_inputs = normalized["safety"].get("quarantine_failed_inputs_root")
    quarantine_finals = normalized["safety"].get("quarantine_failed_finals_root")
    normalized["safety"]["quarantine_failed_inputs_root"] = (
        str(Path(quarantine_inputs)) if quarantine_inputs else None
    )
    normalized["safety"]["quarantine_failed_finals_root"] = (
        str(Path(quarantine_finals)) if quarantine_finals else None
    )
    normalized["safety"]["min_output_bytes"] = int(normalized["safety"].get("min_output_bytes", 5000000))
    normalized["safety"]["duration_tolerance_seconds"] = float(
        normalized["safety"].get("duration_tolerance_seconds", 3.0)
    )

    raw_profiles = normalized.get("show_profiles") or []
    normalized_profiles: list[dict[str, Any]] = []
    if isinstance(raw_profiles, list):
        for profile in raw_profiles:
            if not isinstance(profile, dict):
                continue
            match = str(profile.get("match", "")).strip()
            if not match:
                continue
            enc = profile.get("encoding", {})
            if not isinstance(enc, dict):
                enc = {}
            normalized_profiles.append(
                {
                    "match": match,
                    "encoding": {
                        "crf": enc.get("crf"),
                        "preset": enc.get("preset"),
                        "ten_bit": enc.get("ten_bit"),
                        "audio_codec": str(enc.get("audio_codec", "")).lower() if enc.get("audio_codec") is not None else None,
                        "audio_bitrate_kbps": enc.get("audio_bitrate_kbps"),
                    },
                }
            )
    normalized["show_profiles"] = normalized_profiles

    jellyfin_cfg = normalized.get("jellyfin", {})
    if not isinstance(jellyfin_cfg, dict):
        jellyfin_cfg = {}
    normalized["jellyfin"] = {
        "enabled": bool(jellyfin_cfg.get("enabled", False)),
        "base_url": str(jellyfin_cfg.get("base_url", "")).strip().rstrip("/"),
        "api_key": str(jellyfin_cfg.get("api_key", "")).strip(),
        "refresh_after_run": bool(jellyfin_cfg.get("refresh_after_run", True)),
        "min_done_to_refresh": int(jellyfin_cfg.get("min_done_to_refresh", 1)),
        "timeout_seconds": int(jellyfin_cfg.get("timeout_seconds", 10)),
    }
    log_path = normalized["logging"].get("log_path")
    if log_path:
        normalized["logging"]["log_path"] = str(Path(log_path))
    normalized["dry_run"] = dry_run
    return normalized


def build_loggers(config: dict[str, Any]) -> tuple[logging.Logger, JsonlLogger]:
    logger = logging.getLogger("libriencode")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    log_path = config["logging"].get("log_path")
    dry_run = config["dry_run"]
    if log_path and not dry_run:
        path = Path(log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    json_log_path = None
    if config["logging"].get("json_logs"):
        if log_path:
            json_log_path = str(Path(log_path).with_suffix(".jsonl"))
        else:
            json_log_path = "./logs/av1-encode.jsonl"
    event_logger = JsonlLogger(
        enabled=bool(config["logging"].get("json_logs")),
        path=json_log_path,
        dry_run=dry_run,
    )
    return logger, event_logger


def hash_encoding_options(profile: dict[str, Any]) -> str:
    serialized = json.dumps(profile, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def resolve_encoding_for_show(config: dict[str, Any], show_name: str) -> tuple[dict[str, Any], str | None]:
    effective = copy.deepcopy(config["encoding"])
    matched = None
    for profile in config.get("show_profiles", []):
        pattern = str(profile.get("match", "")).strip()
        if not pattern:
            continue
        if not fnmatch.fnmatch(show_name.lower(), pattern.lower()):
            continue
        matched = pattern
        enc = profile.get("encoding", {})
        if not isinstance(enc, dict):
            break
        if enc.get("crf") is not None:
            effective["crf"] = int(enc["crf"])
        if enc.get("preset") is not None:
            effective["preset"] = int(enc["preset"])
        if enc.get("ten_bit") is not None:
            effective["ten_bit"] = bool(enc["ten_bit"])
        if enc.get("audio_codec") in {"aac", "opus"}:
            effective["audio_codec"] = enc["audio_codec"]
        if enc.get("audio_bitrate_kbps") is not None:
            effective["audio_bitrate_kbps"] = int(enc["audio_bitrate_kbps"])
        break

    return effective, matched


def check_binaries(dry_run: bool, logger: logging.Logger, events: JsonlLogger) -> bool:
    missing = [name for name in BINARY_NAMES if shutil.which(name) is None]
    if not missing:
        return True
    message = f"Missing binaries: {', '.join(missing)}"
    if dry_run:
        logger.warning("%s (continuing because --dry-run is enabled)", message)
        events.emit(level="warning", stage="startup", message=message, dry_run=True)
        return True
    logger.error("%s", message)
    events.emit(level="error", stage="startup", message=message, dry_run=False)
    return False


def wait_for_directory(
    path: Path,
    *,
    retry_seconds: int,
    retry_count: int,
    role: str,
    logger: logging.Logger,
    events: JsonlLogger,
) -> bool:
    if path.is_dir():
        return True
    for attempt in range(1, retry_count + 1):
        logger.warning(
            "%s root missing: %s (retry %d/%d in %ss)",
            role,
            path,
            attempt,
            retry_count,
            retry_seconds,
        )
        events.emit(
            level="warning",
            stage="roots",
            message="root_missing_retry",
            role=role,
            path=str(path),
            attempt=attempt,
            retry_count=retry_count,
        )
        time.sleep(retry_seconds)
        if path.is_dir():
            return True
    return False


def validate_roots(config: dict[str, Any], logger: logging.Logger, events: JsonlLogger) -> bool:
    dry_run = config["dry_run"]
    policy = config["roots"]["on_missing_root"]
    retry_seconds = int(config["roots"]["retry_seconds"])
    retry_count = int(config["roots"]["retry_count"])
    input_root = Path(config["input_root"])
    output_root = Path(config["output_root"])

    if not input_root.is_dir():
        if policy == "retry":
            if not wait_for_directory(
                input_root,
                retry_seconds=retry_seconds,
                retry_count=retry_count,
                role="input",
                logger=logger,
                events=events,
            ):
                raise FileNotFoundError(f"Input root missing after retries: {input_root}")
        elif policy == "skip":
            logger.warning("Input root missing, skipping run: %s", input_root)
            events.emit(
                level="warning",
                stage="roots",
                message="root_missing_skip",
                role="input",
                path=str(input_root),
            )
            return False
        else:
            raise FileNotFoundError(f"Input root missing: {input_root}")

    if output_root.is_dir():
        return True
    if policy == "fail":
        raise FileNotFoundError(f"Output root missing: {output_root}")
    if policy == "retry":
        if wait_for_directory(
            output_root,
            retry_seconds=retry_seconds,
            retry_count=retry_count,
            role="output",
            logger=logger,
            events=events,
        ):
            return True
    if policy == "skip":
        logger.warning("Output root missing, skipping run: %s", output_root)
        events.emit(
            level="warning",
            stage="roots",
            message="root_missing_skip",
            role="output",
            path=str(output_root),
        )
        return False
    if dry_run:
        logger.info("Output root missing: %s (would create in non-dry-run)", output_root)
        events.emit(
            level="info",
            stage="roots",
            message="would_create_output_root",
            path=str(output_root),
        )
        return True
    output_root.mkdir(parents=True, exist_ok=True)
    logger.info("Created output root: %s", output_root)
    events.emit(
        level="info",
        stage="roots",
        message="created_output_root",
        path=str(output_root),
    )
    return True


def ensure_state_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            input_path TEXT PRIMARY KEY,
            show_name TEXT NOT NULL,
            season_name TEXT NOT NULL,
            output_final_path TEXT NOT NULL,
            output_temp_path TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('pending','encoding','verifying','done','failed','skipped')),
            attempt_count INTEGER NOT NULL DEFAULT 0,
            ffmpeg_profile_hash TEXT NOT NULL,
            input_size_bytes INTEGER,
            output_size_bytes INTEGER,
            input_mtime REAL,
            started_at TEXT,
            finished_at TEXT,
            last_error TEXT,
            probe_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_show_name ON jobs(show_name);
        CREATE INDEX IF NOT EXISTS idx_jobs_season_name ON jobs(season_name);
        """
    )
    conn.commit()


def sanitize_component(value: str) -> str:
    translated = value
    for ch in INVALID_FILENAME_CHARS:
        translated = translated.replace(ch, "_")
    translated = translated.strip().strip(".")
    return translated or "unnamed"


def natural_sort_key(value: str) -> list[tuple[int, Any]]:
    parts = NATURAL_SPLIT_RE.split(value)
    key: list[tuple[int, Any]] = []
    for part in parts:
        if not part:
            continue
        if part.isdigit():
            key.append((0, int(part)))
        else:
            key.append((1, part.lower()))
    return key


def parse_season_number(season_name: str) -> int | None:
    match = SEASON_NUMBER_RE.search(season_name)
    if not match:
        return None
    return int(match.group(1))


def extract_episode_number(stem: str) -> int | None:
    sxxeyy = SXXEYY_RE.search(stem)
    if sxxeyy:
        return int(sxxeyy.group(2))
    for pattern in (EPISODE_RE, DASH_NUMBER_RE):
        match = pattern.search(stem)
        if match:
            return int(match.group(1))
    return None


def build_output_basename(show_name: str, season_name: str, source_stem: str) -> str:
    episode_number = extract_episode_number(source_stem)
    season_number = parse_season_number(season_name)
    if episode_number is not None and season_number is not None:
        composed = f"{show_name} S{season_number:02d}E{episode_number:02d} {source_stem}"
        return sanitize_component(composed)
    return sanitize_component(source_stem)


def scan_and_plan(config: dict[str, Any], logger: logging.Logger, events: JsonlLogger) -> list[PlannedJob]:
    input_root = Path(config["input_root"])
    output_root = Path(config["output_root"])
    season_glob = config["scan"]["season_folder_glob"]
    extension_set = set(config["scan"]["extensions"])
    target_extension = f".{config['encoding']['container'].lstrip('.')}"
    temp_suffix = config["safety"]["temp_suffix"]
    planned: list[PlannedJob] = []

    show_dirs = [p for p in sorted(input_root.iterdir(), key=lambda x: natural_sort_key(x.name)) if p.is_dir()]
    for show_dir in show_dirs:
        show_name = show_dir.name
        show_encoding, matched_profile = resolve_encoding_for_show(config, show_name)
        show_profile_hash = hash_encoding_options(show_encoding)
        if matched_profile:
            logger.info("Show profile matched: %s -> %s", show_name, matched_profile)
            events.emit(
                level="info",
                stage="scan",
                message="show_profile_match",
                show=show_name,
                profile=matched_profile,
            )
        season_dirs = [
            p
            for p in sorted(show_dir.iterdir(), key=lambda x: natural_sort_key(x.name))
            if p.is_dir() and fnmatch.fnmatch(p.name, season_glob)
        ]
        for season_dir in season_dirs:
            season_name = season_dir.name
            files = [
                p
                for p in sorted(season_dir.iterdir(), key=lambda x: natural_sort_key(x.name))
                if p.is_file() and p.suffix.lower() in extension_set
            ]
            for source in files:
                output_season = output_root / sanitize_component(show_name) / sanitize_component(season_name)
                output_stem = build_output_basename(show_name, season_name, source.stem)
                output_name = f"{output_stem}{target_extension}"
                stat = source.stat()
                planned.append(
                    PlannedJob(
                        input_path=str(source),
                        show_name=show_name,
                        season_name=season_name,
                        output_final_path=str(output_season / output_name),
                        output_temp_path=str(output_season / f"{output_name}{temp_suffix}"),
                        ffmpeg_profile_hash=show_profile_hash,
                        input_size_bytes=stat.st_size,
                        input_mtime=stat.st_mtime,
                    )
                )
    logger.info("Discovered %d encode-eligible files", len(planned))
    events.emit(
        level="info",
        stage="scan",
        message="scan_complete",
        planned_jobs=len(planned),
        season_glob=season_glob,
        extensions=sorted(extension_set),
    )
    return planned


def upsert_planned_jobs(conn: sqlite3.Connection, jobs: list[PlannedJob]) -> int:
    statement = """
    INSERT INTO jobs (
        input_path, show_name, season_name, output_final_path, output_temp_path,
        status, attempt_count, ffmpeg_profile_hash, input_size_bytes, input_mtime
    ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
    ON CONFLICT(input_path) DO UPDATE SET
        show_name = excluded.show_name,
        season_name = excluded.season_name,
        output_final_path = excluded.output_final_path,
        output_temp_path = excluded.output_temp_path,
        ffmpeg_profile_hash = excluded.ffmpeg_profile_hash,
        input_size_bytes = excluded.input_size_bytes,
        input_mtime = excluded.input_mtime
    """
    rows = [
        (
            job.input_path,
            job.show_name,
            job.season_name,
            job.output_final_path,
            job.output_temp_path,
            job.ffmpeg_profile_hash,
            job.input_size_bytes,
            job.input_mtime,
        )
        for job in jobs
    ]
    conn.executemany(statement, rows)
    conn.commit()
    return len(rows)


def emit_dry_run_plan(jobs: list[PlannedJob], logger: logging.Logger, events: JsonlLogger) -> None:
    if not jobs:
        logger.info("No eligible files found.")
        return
    logger.info("Dry run plan (%d jobs):", len(jobs))
    for index, job in enumerate(jobs, start=1):
        logger.info("[%d] %s -> %s", index, job.input_path, job.output_final_path)
        events.emit(
            level="info",
            stage="plan",
            message="planned_job",
            index=index,
            input=job.input_path,
            output=job.output_final_path,
        )


def run_startup_recovery(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    summary: Summary,
    logger: logging.Logger,
    events: JsonlLogger,
) -> None:
    delete_temp_on_start = bool(config["safety"]["delete_leftover_temp_on_start"])
    if delete_temp_on_start:
        rows = conn.execute(
            "SELECT output_temp_path FROM jobs WHERE status != 'done'"
        ).fetchall()
        for row in rows:
            temp_path = Path(row["output_temp_path"])
            if not temp_path.is_file():
                continue
            try:
                temp_path.unlink()
                summary.deleted_temp_files += 1
                events.emit(
                    level="info",
                    stage="startup_recovery",
                    message="leftover_temp_deleted",
                    temp_path=str(temp_path),
                )
            except OSError as exc:
                logger.warning("Failed to delete leftover temp %s: %s", temp_path, exc)
                events.emit(
                    level="warning",
                    stage="startup_recovery",
                    message="leftover_temp_delete_failed",
                    temp_path=str(temp_path),
                    error=str(exc),
                )

    reset_count = conn.execute(
        """
        UPDATE jobs
        SET status = 'pending', last_error = COALESCE(last_error, 'reset on startup')
        WHERE status IN ('encoding', 'verifying')
        """
    ).rowcount
    conn.commit()
    summary.recovered_jobs += int(reset_count or 0)
    if summary.recovered_jobs or summary.deleted_temp_files:
        logger.info(
            "Startup recovery: reset %d in-progress jobs, deleted %d temp files",
            summary.recovered_jobs,
            summary.deleted_temp_files,
        )
        events.emit(
            level="info",
            stage="startup_recovery",
            message="startup_recovery_complete",
            recovered_jobs=summary.recovered_jobs,
            deleted_temp_files=summary.deleted_temp_files,
        )


def collect_top_failure_reasons(conn: sqlite3.Connection, limit: int = 5) -> list[tuple[str, int]]:
    rows = conn.execute(
        """
        SELECT last_error
        FROM jobs
        WHERE status = 'failed' AND last_error IS NOT NULL AND last_error != ''
        """
    ).fetchall()
    counts: dict[str, int] = {}
    for row in rows:
        reason = str(row["last_error"]).splitlines()[0].strip()
        if not reason:
            continue
        counts[reason] = counts.get(reason, 0) + 1
    ordered = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return ordered[:limit]


def trigger_jellyfin_refresh(
    config: dict[str, Any],
    summary: Summary,
    logger: logging.Logger,
    events: JsonlLogger,
) -> None:
    jellyfin = config.get("jellyfin", {})
    if not jellyfin.get("enabled", False):
        return
    if not jellyfin.get("refresh_after_run", True):
        return
    min_done = int(jellyfin.get("min_done_to_refresh", 1))
    if summary.done < min_done:
        return

    base_url = str(jellyfin.get("base_url", "")).strip().rstrip("/")
    api_key = str(jellyfin.get("api_key", "")).strip()
    if not base_url or not api_key:
        logger.warning("Jellyfin refresh skipped: missing base_url or api_key")
        events.emit(
            level="warning",
            stage="jellyfin",
            message="refresh_skipped_missing_config",
        )
        return

    summary.jellyfin_refresh_attempted = True
    url = f"{base_url}/Library/Refresh"
    timeout_seconds = int(jellyfin.get("timeout_seconds", 10))
    req = urllib.request.Request(
        url=url,
        method="POST",
        data=b"{}",
        headers={
            "Content-Type": "application/json",
            "X-Emby-Token": api_key,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            if 200 <= resp.status < 300:
                summary.jellyfin_refresh_success = True
                logger.info("Jellyfin library refresh triggered successfully")
                events.emit(
                    level="info",
                    stage="jellyfin",
                    message="refresh_triggered",
                    status=resp.status,
                )
                return
            logger.warning("Jellyfin refresh returned status %s", resp.status)
            events.emit(
                level="warning",
                stage="jellyfin",
                message="refresh_http_status",
                status=resp.status,
            )
    except urllib.error.URLError as exc:
        logger.warning("Jellyfin refresh failed: %s", exc)
        events.emit(
            level="warning",
            stage="jellyfin",
            message="refresh_failed",
            error=str(exc),
        )


def cleanup_empty_staging_folders(
    config: dict[str, Any],
    summary: Summary,
    logger: logging.Logger,
    events: JsonlLogger,
) -> None:
    input_root = Path(config["input_root"])
    season_glob = str(config["scan"]["season_folder_glob"])
    if not input_root.is_dir():
        return

    show_dirs = [p for p in sorted(input_root.iterdir(), key=lambda x: x.name.lower()) if p.is_dir()]
    for show_dir in show_dirs:
        season_dirs = [
            p
            for p in sorted(show_dir.iterdir(), key=lambda x: x.name.lower())
            if p.is_dir() and fnmatch.fnmatch(p.name, season_glob)
        ]
        for season_dir in season_dirs:
            try:
                next(season_dir.iterdir())
            except StopIteration:
                try:
                    season_dir.rmdir()
                    summary.deleted_season_folders += 1
                    events.emit(
                        level="info",
                        stage="cleanup",
                        message="deleted_empty_season_folder",
                        path=str(season_dir),
                    )
                except OSError:
                    pass
            except OSError:
                continue

        try:
            next(show_dir.iterdir())
        except StopIteration:
            try:
                show_dir.rmdir()
                summary.deleted_show_folders += 1
                events.emit(
                    level="info",
                    stage="cleanup",
                    message="deleted_empty_show_folder",
                    path=str(show_dir),
                )
            except OSError:
                pass
        except OSError:
            continue

    if summary.deleted_season_folders or summary.deleted_show_folders:
        logger.info(
            "Cleanup: deleted %d empty season folders and %d empty show folders",
            summary.deleted_season_folders,
            summary.deleted_show_folders,
        )


def quarantine_file(
    source_path: Path,
    quarantine_root: str | None,
    row: sqlite3.Row,
    *,
    kind: str,
    summary: Summary,
    logger: logging.Logger,
    events: JsonlLogger,
) -> Path | None:
    if not quarantine_root or not source_path.exists():
        return None

    target_dir = (
        Path(quarantine_root)
        / sanitize_component(str(row["show_name"]))
        / sanitize_component(str(row["season_name"]))
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / source_path.name
    if target_path.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        target_path = target_dir / f"{target_path.stem}.{stamp}{target_path.suffix}"
    try:
        moved_to = Path(shutil.move(str(source_path), str(target_path)))
        if kind == "input":
            summary.quarantined_inputs += 1
        elif kind == "final":
            summary.quarantined_finals += 1
        logger.warning("Quarantined %s file: %s -> %s", kind, source_path, moved_to)
        events.emit(
            level="warning",
            stage="quarantine",
            message=f"{kind}_quarantined",
            source=str(source_path),
            target=str(moved_to),
        )
        return moved_to
    except OSError as exc:
        logger.warning("Failed to quarantine %s file %s: %s", kind, source_path, exc)
        events.emit(
            level="warning",
            stage="quarantine",
            message=f"{kind}_quarantine_failed",
            source=str(source_path),
            error=str(exc),
        )
        return None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_job_row(conn: sqlite3.Connection, input_path: str) -> sqlite3.Row | None:
    cursor = conn.execute("SELECT * FROM jobs WHERE input_path = ?", (input_path,))
    return cursor.fetchone()


def update_job_fields(conn: sqlite3.Connection, input_path: str, fields: dict[str, Any]) -> None:
    if not fields:
        return
    assignments = ", ".join(f"{key} = ?" for key in fields)
    params = list(fields.values()) + [input_path]
    conn.execute(f"UPDATE jobs SET {assignments} WHERE input_path = ?", params)
    conn.commit()


def mark_job_failed(conn: sqlite3.Connection, input_path: str, error: str) -> None:
    conn.execute(
        """
        UPDATE jobs
        SET
            status = 'failed',
            attempt_count = attempt_count + 1,
            last_error = ?,
            finished_at = ?
        WHERE input_path = ?
        """,
        (error, utc_now(), input_path),
    )
    conn.commit()


def mark_job_done(conn: sqlite3.Connection, input_path: str, output_size_bytes: int, probe_json: str) -> None:
    conn.execute(
        """
        UPDATE jobs
        SET
            status = 'done',
            output_size_bytes = ?,
            probe_json = ?,
            last_error = NULL,
            finished_at = ?
        WHERE input_path = ?
        """,
        (output_size_bytes, probe_json, utc_now(), input_path),
    )
    conn.commit()


def build_ffmpeg_command(encoding: dict[str, Any], input_path: Path, temp_output_path: Path) -> list[str]:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostats",
        "-progress",
        "pipe:1",
        "-y",
        "-i",
        str(input_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-map",
        "0:s?",
        "-c:v",
        str(encoding["codec"]),
        "-crf",
        str(encoding["crf"]),
        "-preset",
        str(encoding["preset"]),
    ]
    if encoding["ten_bit"]:
        cmd.extend(["-pix_fmt", "yuv420p10le"])
    audio_codec = encoding["audio_codec"]
    audio_bitrate = int(encoding["audio_bitrate_kbps"])
    if audio_codec == "aac":
        cmd.extend(["-c:a", "aac", "-b:a", f"{audio_bitrate}k"])
    else:
        cmd.extend(["-c:a", "libopus", "-b:a", f"{audio_bitrate}k"])
    if encoding["container"] == "mkv":
        cmd.extend(["-c:s", "copy"])
    else:
        cmd.extend(["-c:s", "mov_text"])
    muxer = "matroska" if encoding["container"] == "mkv" else "mp4"
    cmd.extend(["-f", muxer])
    cmd.append(str(temp_output_path))
    return cmd


def run_ffprobe(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip() or "ffprobe failed"
        return None, detail
    try:
        return json.loads(proc.stdout), None
    except json.JSONDecodeError as exc:
        return None, f"invalid ffprobe json: {exc}"


def parse_duration_seconds(probe: dict[str, Any]) -> float | None:
    duration_raw = probe.get("format", {}).get("duration")
    if duration_raw is None:
        return None
    try:
        value = float(duration_raw)
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None


def parse_ffmpeg_clock(value: str | None) -> float | None:
    if not value:
        return None
    parts = value.strip().split(":")
    if len(parts) != 3:
        return None
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = float(parts[2])
    except ValueError:
        return None
    total = (hours * 3600) + (minutes * 60) + seconds
    return total if total >= 0 else None


def parse_ffmpeg_progress_seconds(progress_state: dict[str, str]) -> float | None:
    out_seconds = parse_ffmpeg_clock(progress_state.get("out_time"))
    if out_seconds is not None:
        return out_seconds
    for key in ("out_time_ms", "out_time_us"):
        raw_value = progress_state.get(key)
        if raw_value is None:
            continue
        try:
            micros = int(raw_value)
        except ValueError:
            continue
        if micros >= 0:
            return micros / 1_000_000.0
    return None


def format_clock(seconds: float) -> str:
    total = max(0, int(seconds))
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def get_input_duration_seconds(input_path: Path) -> float | None:
    probe_json, _ = run_ffprobe(input_path)
    if probe_json is None:
        return None
    return parse_duration_seconds(probe_json)


def run_ffmpeg_with_progress(
    cmd: list[str],
    *,
    input_duration: float | None,
    progress_stall_timeout_seconds: int,
    job_index: int,
    total_jobs: int,
    logger: logging.Logger,
    events: JsonlLogger,
    row: sqlite3.Row,
) -> tuple[int, str]:
    progress_state: dict[str, str] = {}
    non_progress_lines: list[str] = []
    last_log_time = 0.0
    last_progress_advance_time = time.monotonic()
    last_out_seconds: float | None = None
    last_frame: str | None = None
    last_total_size: str | None = None

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        universal_newlines=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.strip()
        if not line:
            continue
        if "=" in line:
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key:
                progress_state[key] = value

            if key == "progress":
                now = time.monotonic()
                out_seconds = parse_ffmpeg_progress_seconds(progress_state)
                speed = progress_state.get("speed", "?")
                frame = progress_state.get("frame")
                total_size = progress_state.get("total_size")
                progressed = False
                if out_seconds is not None:
                    if last_out_seconds is None or out_seconds > last_out_seconds + 0.0001:
                        progressed = True
                    last_out_seconds = out_seconds
                if frame is not None:
                    if frame != last_frame:
                        progressed = True
                    last_frame = frame
                if total_size is not None:
                    if total_size != last_total_size:
                        progressed = True
                    last_total_size = total_size
                if value == "end":
                    progressed = True
                if progressed:
                    last_progress_advance_time = now
                idle_seconds = now - last_progress_advance_time
                if (
                    progress_stall_timeout_seconds > 0
                    and value == "continue"
                    and idle_seconds >= progress_stall_timeout_seconds
                ):
                    detail = (
                        "ffmpeg progress stalled for "
                        f"{int(idle_seconds)}s "
                        f"(last_out_time={progress_state.get('out_time', '?')}, speed={speed})"
                    )
                    logger.warning("[%d/%d] %s", job_index, total_jobs, detail)
                    events.emit(
                        level="warning",
                        stage="encoding_progress",
                        message="ffmpeg_stall_timeout",
                        input=row["input_path"],
                        job_index=job_index,
                        total_jobs=total_jobs,
                        idle_seconds=int(idle_seconds),
                        stall_timeout_seconds=progress_stall_timeout_seconds,
                        speed=speed,
                        last_out_time=progress_state.get("out_time"),
                    )
                    try:
                        proc.terminate()
                    except OSError:
                        pass
                    try:
                        proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        try:
                            proc.kill()
                        except OSError:
                            pass
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            pass
                    returncode = proc.returncode if proc.returncode is not None else 124
                    if returncode == 0:
                        returncode = 124
                    detail_lines = non_progress_lines[-40:]
                    detail_lines.append(detail)
                    return returncode, "\n".join(detail_lines).strip()

                should_log = value == "end" or (value == "continue" and (now - last_log_time) >= 5.0)
                if not should_log:
                    continue
                last_log_time = now

                display_out_seconds = out_seconds if out_seconds is not None else last_out_seconds
                if input_duration is not None and display_out_seconds is not None and input_duration > 0:
                    pct = min(100.0, max(0.0, (display_out_seconds / input_duration) * 100.0))
                    logger.info(
                        "[%d/%d] ffmpeg %.1f%% (%s/%s) speed %s",
                        job_index,
                        total_jobs,
                        pct,
                        format_clock(display_out_seconds),
                        format_clock(input_duration),
                        speed,
                    )
                    events.emit(
                        level="info",
                        stage="encoding_progress",
                        message="ffmpeg_progress",
                        input=row["input_path"],
                        job_index=job_index,
                        total_jobs=total_jobs,
                        progress_pct=round(pct, 2),
                        speed=speed,
                    )
                elif display_out_seconds is not None:
                    logger.info(
                        "[%d/%d] ffmpeg encoded %s speed %s",
                        job_index,
                        total_jobs,
                        format_clock(display_out_seconds),
                        speed,
                    )
                    events.emit(
                        level="info",
                        stage="encoding_progress",
                        message="ffmpeg_progress",
                        input=row["input_path"],
                        job_index=job_index,
                        total_jobs=total_jobs,
                        encoded_time=format_clock(display_out_seconds),
                        speed=speed,
                    )
                else:
                    logger.info("[%d/%d] ffmpeg running speed %s", job_index, total_jobs, speed)
                    events.emit(
                        level="info",
                        stage="encoding_progress",
                        message="ffmpeg_progress",
                        input=row["input_path"],
                        job_index=job_index,
                        total_jobs=total_jobs,
                        speed=speed,
                    )
                continue

        non_progress_lines.append(line)

    returncode = proc.wait()
    detail = "\n".join(non_progress_lines[-40:]).strip()
    return returncode, detail


def verify_output(
    config: dict[str, Any],
    output_path: Path,
    input_path: Path | None,
) -> tuple[bool, str, str, int]:
    if not output_path.is_file():
        return False, "", "output file does not exist", 0

    output_size = output_path.stat().st_size
    min_output_bytes = int(config["safety"]["min_output_bytes"])
    if output_size < min_output_bytes:
        return False, "", f"output too small ({output_size} < {min_output_bytes})", output_size

    output_probe, output_err = run_ffprobe(output_path)
    if output_probe is None:
        return False, "", f"ffprobe output failed: {output_err}", output_size

    video_streams = [s for s in output_probe.get("streams", []) if s.get("codec_type") == "video"]
    if not any(s.get("codec_name") == "av1" for s in video_streams):
        return False, "", "output video codec is not AV1", output_size

    if input_path is not None and input_path.is_file():
        input_probe, input_err = run_ffprobe(input_path)
        if input_probe is None:
            return False, "", f"ffprobe input failed: {input_err}", output_size
        output_duration = parse_duration_seconds(output_probe)
        input_duration = parse_duration_seconds(input_probe)
        if output_duration is not None and input_duration is not None:
            tolerance = float(config["safety"]["duration_tolerance_seconds"])
            if abs(output_duration - input_duration) > tolerance:
                return (
                    False,
                    "",
                    (
                        "duration drift too high "
                        f"(input={input_duration:.3f}s output={output_duration:.3f}s tolerance={tolerance:.3f}s)"
                    ),
                    output_size,
                )

    probe_json = json.dumps(output_probe, ensure_ascii=True)
    return True, probe_json, "", output_size


def reconcile_job_from_existing_final(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    row: sqlite3.Row,
    summary: Summary,
    logger: logging.Logger,
    events: JsonlLogger,
) -> bool:
    input_path = Path(row["input_path"])
    final_path = Path(row["output_final_path"])
    if row["status"] == "done" or not final_path.is_file():
        return False

    is_valid, probe_json, error, size = verify_output(
        config,
        final_path,
        input_path if input_path.exists() else None,
    )
    if is_valid:
        mark_job_done(conn, row["input_path"], size, probe_json)
        summary.done += 1
        logger.info("Reconciled existing final as done: %s", final_path)
        events.emit(
            level="info",
            stage="reconcile",
            message="existing_final_valid",
            input=row["input_path"],
            output=str(final_path),
        )
        if input_path.exists():
            input_path.unlink()
            summary.deleted_originals += 1
            logger.info("Deleted source after reconciliation: %s", input_path)
        return True

    mark_job_failed(conn, row["input_path"], f"existing final invalid: {error}")
    summary.failed += 1
    logger.warning("Existing final invalid for %s: %s", final_path, error)
    events.emit(
        level="warning",
        stage="reconcile",
        message="existing_final_invalid",
        input=row["input_path"],
        output=str(final_path),
        error=error,
    )
    if config["safety"]["delete_bad_final"]:
        try:
            final_path.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Failed to delete bad final %s: %s", final_path, exc)
    else:
        quarantine_file(
            final_path,
            config["safety"].get("quarantine_failed_finals_root"),
            row,
            kind="final",
            summary=summary,
            logger=logger,
            events=events,
        )
    return True


def run_encode_for_job(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    row: sqlite3.Row,
    job_index: int,
    total_jobs: int,
    summary: Summary,
    logger: logging.Logger,
    events: JsonlLogger,
) -> None:
    input_path = Path(row["input_path"])
    final_path = Path(row["output_final_path"])
    temp_path = Path(row["output_temp_path"])
    encoding, matched_profile = resolve_encoding_for_show(config, str(row["show_name"]))
    if not input_path.is_file():
        mark_job_failed(conn, row["input_path"], "input file missing")
        summary.failed += 1
        logger.warning("Input missing, marked failed: %s", input_path)
        return

    final_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path.unlink(missing_ok=True)
    update_job_fields(
        conn,
        row["input_path"],
        {"status": "encoding", "started_at": utc_now(), "last_error": None},
    )
    input_duration = get_input_duration_seconds(input_path)
    cmd = build_ffmpeg_command(encoding, input_path, temp_path)
    logger.info(
        "[%d/%d] Encoding started: %s",
        job_index,
        total_jobs,
        input_path,
    )
    events.emit(
        level="info",
        stage="encoding",
        message="encode_start",
        input=row["input_path"],
        show=row["show_name"],
        profile=matched_profile,
        job_index=job_index,
        total_jobs=total_jobs,
    )
    returncode, ffmpeg_output = run_ffmpeg_with_progress(
        cmd,
        input_duration=input_duration,
        progress_stall_timeout_seconds=int(config["safety"]["progress_stall_timeout_seconds"]),
        job_index=job_index,
        total_jobs=total_jobs,
        logger=logger,
        events=events,
        row=row,
    )
    if returncode != 0 or not temp_path.exists():
        detail = ffmpeg_output or f"ffmpeg exit code {returncode}"
        mark_job_failed(conn, row["input_path"], f"encode failed: {detail}")
        summary.failed += 1
        logger.warning("[%d/%d] Encode failed for %s: %s", job_index, total_jobs, input_path, detail)
        quarantine_file(
            input_path,
            config["safety"].get("quarantine_failed_inputs_root"),
            row,
            kind="input",
            summary=summary,
            logger=logger,
            events=events,
        )
        temp_path.unlink(missing_ok=True)
        return

    update_job_fields(conn, row["input_path"], {"status": "verifying"})
    valid_temp, _, temp_error, _ = verify_output(config, temp_path, input_path)
    if not valid_temp:
        mark_job_failed(conn, row["input_path"], f"temp verification failed: {temp_error}")
        summary.failed += 1
        logger.warning("[%d/%d] Temp verification failed for %s: %s", job_index, total_jobs, temp_path, temp_error)
        temp_path.unlink(missing_ok=True)
        return

    temp_path.replace(final_path)
    valid_final, probe_json, final_error, final_size = verify_output(config, final_path, input_path)
    if not valid_final:
        mark_job_failed(conn, row["input_path"], f"final verification failed: {final_error}")
        summary.failed += 1
        logger.warning("[%d/%d] Final verification failed for %s: %s", job_index, total_jobs, final_path, final_error)
        if config["safety"]["delete_bad_final"]:
            final_path.unlink(missing_ok=True)
        else:
            quarantine_file(
                final_path,
                config["safety"].get("quarantine_failed_finals_root"),
                row,
                kind="final",
                summary=summary,
                logger=logger,
                events=events,
            )
        return

    mark_job_done(conn, row["input_path"], final_size, probe_json)
    summary.done += 1
    logger.info(
        "[%d/%d] Completed: %s -> %s | encoded this run: %d/%d",
        job_index,
        total_jobs,
        input_path,
        final_path,
        summary.done,
        total_jobs,
    )
    try:
        input_path.unlink()
        summary.deleted_originals += 1
        logger.info("Deleted source: %s", input_path)
    except OSError as exc:
        logger.warning("Done but failed to delete source %s: %s", input_path, exc)


def process_jobs(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    jobs: list[PlannedJob],
    summary: Summary,
    logger: logging.Logger,
    events: JsonlLogger,
) -> None:
    max_attempts = int(config["safety"]["max_attempts"])
    total_jobs = len(jobs)
    if int(config["encoding"]["concurrency"]) > 1:
        logger.info("Concurrency %d requested; current runner executes sequentially.", int(config["encoding"]["concurrency"]))

    for job_index, job in enumerate(jobs, start=1):
        row = fetch_job_row(conn, job.input_path)
        if row is None:
            summary.skipped += 1
            continue
        status = str(row["status"])
        if status not in VALID_STATUSES:
            mark_job_failed(conn, row["input_path"], f"invalid status: {status}")
            summary.failed += 1
            continue

        final_path = Path(row["output_final_path"])
        if status == "done":
            if final_path.is_file():
                summary.skipped += 1
                continue
            update_job_fields(conn, row["input_path"], {"status": "pending", "last_error": "final missing for done state"})
            row = fetch_job_row(conn, job.input_path)
            if row is None:
                summary.skipped += 1
                continue

        if row["status"] in {"encoding", "verifying", "skipped"}:
            update_job_fields(conn, row["input_path"], {"status": "pending"})
            row = fetch_job_row(conn, job.input_path)
            if row is None:
                summary.skipped += 1
                continue

        if reconcile_job_from_existing_final(conn, config, row, summary, logger, events):
            continue
        row = fetch_job_row(conn, job.input_path)
        if row is None:
            summary.skipped += 1
            continue
        attempts = int(row["attempt_count"] or 0)
        if row["status"] == "failed" and attempts >= max_attempts:
            summary.skipped += 1
            logger.warning(
                "[%d/%d] Max attempts reached (%d), skipping: %s",
                job_index,
                total_jobs,
                max_attempts,
                row["input_path"],
            )
            continue
        run_encode_for_job(conn, config, row, job_index, total_jobs, summary, logger, events)


def print_summary(summary: Summary, logger: logging.Logger, events: JsonlLogger) -> None:
    top_reasons = summary.top_failure_reasons or []
    payload = {
        "planned_jobs": summary.planned_jobs,
        "db_upserts": summary.db_upserts,
        "recovered_jobs": summary.recovered_jobs,
        "deleted_temp_files": summary.deleted_temp_files,
        "done": summary.done,
        "skipped": summary.skipped,
        "failed": summary.failed,
        "deleted_originals": summary.deleted_originals,
        "quarantined_inputs": summary.quarantined_inputs,
        "quarantined_finals": summary.quarantined_finals,
        "deleted_season_folders": summary.deleted_season_folders,
        "deleted_show_folders": summary.deleted_show_folders,
        "jellyfin_refresh_attempted": summary.jellyfin_refresh_attempted,
        "jellyfin_refresh_success": summary.jellyfin_refresh_success,
        "top_failure_reasons": top_reasons,
    }
    logger.info("Run summary: %s", payload)
    events.emit(level="info", stage="summary", message="run_summary", **payload)


def load_effective_config(args: argparse.Namespace) -> dict[str, Any]:
    config = copy.deepcopy(DEFAULT_CONFIG)
    if args.config:
        config = deep_merge(config, load_yaml_config(args.config))
    config = apply_cli_overrides(config, args)
    return normalize_config(config, dry_run=args.dry_run)


def main() -> int:
    args = parse_args()
    try:
        config = load_effective_config(args)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    logger, events = build_loggers(config)
    summary = Summary()
    try:
        logger.info("Starting LibriEncode")
        events.emit(level="info", stage="startup", message="run_start", dry_run=config["dry_run"])
        if not check_binaries(config["dry_run"], logger, events):
            return 3
        if not validate_roots(config, logger, events):
            logger.info("Root validation requested skip. Exiting cleanly.")
            print_summary(summary, logger, events)
            return 0

        planned_jobs = scan_and_plan(config, logger, events)
        summary.planned_jobs = len(planned_jobs)
        if config["dry_run"]:
            emit_dry_run_plan(planned_jobs, logger, events)
            print_summary(summary, logger, events)
            return 0

        state_db = Path(config["state_db"])
        state_db.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(state_db)
        conn.row_factory = sqlite3.Row
        try:
            ensure_state_schema(conn)
            summary.db_upserts = upsert_planned_jobs(conn, planned_jobs)
            run_startup_recovery(conn, config, summary, logger, events)
            process_jobs(conn, config, planned_jobs, summary, logger, events)
            cleanup_empty_staging_folders(config, summary, logger, events)
            summary.top_failure_reasons = collect_top_failure_reasons(conn)
            trigger_jellyfin_refresh(config, summary, logger, events)
        finally:
            conn.close()

        logger.info("State database updated: %s (%d jobs)", state_db, summary.db_upserts)
        events.emit(
            level="info",
            stage="state",
            message="state_upsert_complete",
            state_db=str(state_db),
            rows=summary.db_upserts,
        )
        print_summary(summary, logger, events)
        return 0
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        events.emit(level="warning", stage="fatal", message="interrupted_by_user")
        return 130
    except Exception as exc:
        logger.exception("Unhandled error: %s", exc)
        events.emit(level="error", stage="fatal", message="unhandled_exception", error=str(exc))
        return 1
    finally:
        events.close()


if __name__ == "__main__":
    raise SystemExit(main())
