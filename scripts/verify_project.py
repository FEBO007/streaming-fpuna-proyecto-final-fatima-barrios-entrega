#!/usr/bin/env python3
"""Run the repository checks consistently on Windows, Linux and macOS."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_DIRECTORY = REPOSITORY_ROOT / "evidence"
CHECKSUM_FILE = EVIDENCE_DIRECTORY / "SHA256SUMS.txt"
SUPPORTED_PYTHON_MIN = (3, 12)
SUPPORTED_PYTHON_MAX_EXCLUSIVE = (3, 15)


def _display_command(command: list[str]) -> str:
    """Render a command for human-readable verification logs."""
    return subprocess.list2cmdline(command)


def _run(command: list[str]) -> None:
    """Run one check from the repository root and stop on failure."""
    print(f"\n$ {_display_command(command)}", flush=True)
    completed = subprocess.run(command, cwd=REPOSITORY_ROOT, check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def _require_supported_python() -> None:
    version = sys.version_info[:2]
    if not (SUPPORTED_PYTHON_MIN <= version < SUPPORTED_PYTHON_MAX_EXCLUSIVE):
        minimum = ".".join(map(str, SUPPORTED_PYTHON_MIN))
        maximum = ".".join(map(str, SUPPORTED_PYTHON_MAX_EXCLUSIVE))
        raise SystemExit(
            f"Python {minimum} or newer and lower than {maximum} is required; "
            f"found {sys.version.split()[0]}"
        )


def _repository_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "unavailable"


def _require_clean_worktree() -> None:
    completed = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise SystemExit("Git is required when --require-clean is used")
    if completed.stdout.strip():
        raise SystemExit("The Git worktree is not clean")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_evidence_checksums() -> int:
    """Verify every artifact listed in the evidence checksum manifest."""
    if not CHECKSUM_FILE.is_file():
        raise SystemExit(f"Missing checksum manifest: {CHECKSUM_FILE}")

    verified = 0
    evidence_root = EVIDENCE_DIRECTORY.resolve()
    for line_number, raw_line in enumerate(
        CHECKSUM_FILE.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split(maxsplit=1)
        if len(parts) != 2 or len(parts[0]) != 64:
            raise SystemExit(f"Invalid checksum entry at line {line_number}")

        expected, relative_name = parts
        candidate = (EVIDENCE_DIRECTORY / relative_name.lstrip("* ")).resolve()
        try:
            candidate.relative_to(evidence_root)
        except ValueError as error:
            raise SystemExit(
                f"Checksum entry escapes the evidence directory at line {line_number}"
            ) from error

        if not candidate.is_file():
            raise SystemExit(f"Missing evidence file: {candidate.name}")

        actual = _sha256(candidate)
        if actual != expected.lower():
            raise SystemExit(
                f"Checksum mismatch for {candidate.name}: expected {expected}, found {actual}"
            )
        verified += 1

    if verified == 0:
        raise SystemExit("The evidence checksum manifest is empty")
    return verified


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-compose",
        action="store_true",
        help="Skip Docker Compose syntax validation when Docker is unavailable",
    )
    parser.add_argument(
        "--require-clean",
        action="store_true",
        help="Fail if the Git worktree contains uncommitted or untracked files",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    _require_supported_python()
    if args.require_clean:
        _require_clean_worktree()

    print("PROJECT_VERIFICATION_START")
    print(f"REPOSITORY={REPOSITORY_ROOT}")
    print(f"COMMIT={_repository_commit()}")
    print(f"PYTHON={sys.version.split()[0]}")

    _run([sys.executable, "-m", "pytest", "-q"])
    _run([sys.executable, "-m", "ruff", "check", "."])
    _run([sys.executable, "-m", "ruff", "format", "--check", "."])

    verified_checksums = _verify_evidence_checksums()
    print(f"EVIDENCE_CHECKSUMS_VERIFIED={verified_checksums}")

    if args.skip_compose:
        print("COMPOSE_CONFIG=SKIPPED")
    else:
        docker = shutil.which("docker")
        if docker is None:
            raise SystemExit(
                "Docker was not found; install Docker or use --skip-compose "
                "for code-only verification"
            )
        _run([docker, "compose", "config", "--quiet"])
        print("COMPOSE_CONFIG=VALID")

    print("VERIFICATION_EXIT_CODE=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
