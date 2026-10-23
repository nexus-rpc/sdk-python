"""Validate release distribution files before publication."""

from __future__ import annotations

import argparse
import email.parser
import pathlib
import re
import tarfile
import zipfile

import tomllib


def validate_version(version: str) -> str:
    """Require a canonical release version suitable for metadata and tags."""
    if not re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
        r"(?:(?:a|b|rc)(?:0|[1-9][0-9]*))?"
        r"(?:\.post(?:0|[1-9][0-9]*))?(?:\.dev(?:0|[1-9][0-9]*))?",
        version,
    ):
        raise ValueError(f"Expected a canonical release version, got {version!r}")
    return version


def _verify_metadata(data: bytes, version: str, requires_python: str) -> None:
    metadata = email.parser.BytesParser().parsebytes(data)
    name = re.sub(r"[-_.]+", "-", str(metadata["Name"])).lower()
    if name != "nexus-rpc" or metadata["Version"] != version:
        raise ValueError("Distribution metadata does not match nexus-rpc==" + version)
    if metadata["Requires-Python"] != requires_python:
        raise ValueError("Distribution has unexpected Requires-Python metadata")


def verify_dist(dist: pathlib.Path, version: str) -> None:
    """Check the exact artifact set, embedded metadata, and public package files."""
    validate_version(version)
    with (pathlib.Path(__file__).resolve().parents[1] / "pyproject.toml").open(
        "rb"
    ) as f:
        requires_python = tomllib.load(f)["project"]["requires-python"]
    wheel = f"nexus_rpc-{version}-py3-none-any.whl"
    sdist = f"nexus_rpc-{version}.tar.gz"
    # uv build writes a .gitignore alongside the distributions.
    files = sorted(path.name for path in dist.iterdir() if path.name != ".gitignore")
    if files != sorted([wheel, sdist]):
        raise ValueError("Expected exactly one universal wheel and one sdist")
    required = {"__init__.py", "py.typed", "handler/__init__.py"}
    with zipfile.ZipFile(dist / wheel) as archive:
        names = archive.namelist()
        metadata_path = f"nexus_rpc-{version}.dist-info/METADATA"
        if (
            len(names) != len(set(names))
            or {name for name in names if name.endswith(".dist-info/METADATA")}
            != {metadata_path}
            or not {f"nexusrpc/{name}" for name in required}.issubset(names)
        ):
            raise ValueError("Wheel is missing package files or has invalid metadata")
        _verify_metadata(archive.read(metadata_path), version, requires_python)
    with tarfile.open(dist / sdist, "r:gz") as archive:
        names = archive.getnames()
        prefix = f"nexus_rpc-{version}"
        if len(names) != len(set(names)) or not {
            f"{prefix}/src/nexusrpc/{name}" for name in required
        }.issubset(names):
            raise ValueError("Source distribution is missing package files")
        metadata = archive.extractfile(f"{prefix}/PKG-INFO")
        if metadata is None:
            raise ValueError("Source distribution is missing metadata")
        with metadata:
            _verify_metadata(metadata.read(), version, requires_python)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--dist-dir", type=pathlib.Path, default=pathlib.Path("dist"))
    args = parser.parse_args()
    verify_dist(args.dist_dir, args.version)
    print(f"Verified nexus-rpc=={args.version}")


if __name__ == "__main__":
    main()
