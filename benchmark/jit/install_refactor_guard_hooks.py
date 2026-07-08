#!/usr/bin/env python3
#
# Install the repository-local JIT refactor guard git hooks.

from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
HOOKS_PATH = "benchmark/jit/git_hooks"


def main() -> int:
    hooks_dir = ROOT / HOOKS_PATH
    for hook_name in ("pre-commit", "pre-push"):
        hook = hooks_dir / hook_name
        if not hook.is_file():
            raise RuntimeError(f"missing hook: {hook}")
    subprocess.run(["git", "config", "core.hooksPath", HOOKS_PATH], cwd=ROOT, check=True)
    configured = subprocess.check_output(["git", "config", "--get", "core.hooksPath"], cwd=ROOT, text=True).strip()
    if configured != HOOKS_PATH:
        raise RuntimeError(f"core.hooksPath was not installed: {configured!r}")
    print(f"installed JIT refactor guard hooks: {HOOKS_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
