"""Explicit Python-child execution without retries or evidence cleanup."""

import os
import subprocess
import sys
from pathlib import Path

import jobflow_gitlab_slurm


def python_child(code, *arguments):
    """Run the active interpreter with an explicit package root and 20s timeout."""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(
        Path(jobflow_gitlab_slurm.__file__).resolve().parent.parent
    )
    return subprocess.run(
        [sys.executable, "-c", code, *(str(value) for value in arguments)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
