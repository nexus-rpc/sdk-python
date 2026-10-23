"""Install an exact published release with bounded index-propagation retries."""

from __future__ import annotations

import argparse
import importlib.metadata
import subprocess
import sys
import time
from collections.abc import Sequence


def pip_install(args: Sequence[str], *, attempts: int = 1) -> None:
    """Retry only package installation, allowing indexes time to propagate."""
    for attempt in range(attempts):
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", *args], check=True)
            return
        except subprocess.CalledProcessError:
            if attempt == attempts - 1:
                raise
            print("Package installation failed; retrying in 30 seconds", flush=True)
            time.sleep(30)


def install(version: str, index_url: str, dependency_index_url: str | None) -> None:
    """Keep the release index separate from the dependency index."""
    pip_install(
        [
            "--no-cache-dir",
            "--only-binary=:all:",
            "--index-url",
            index_url,
            *(["--no-deps"] if dependency_index_url else []),
            f"nexus-rpc=={version}",
        ],
        attempts=31,
    )
    if dependency_index_url:
        requirements = importlib.metadata.requires("nexus-rpc") or []
        if requirements:
            pip_install(["--index-url", dependency_index_url, *requirements])
    subprocess.run([sys.executable, "-m", "pip", "check"], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--index-url", required=True)
    parser.add_argument("--dependency-index-url")
    args = parser.parse_args()
    install(args.version, args.index_url, args.dependency_index_url)
