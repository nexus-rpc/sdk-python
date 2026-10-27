"""Exercise the installed Nexus package without importing the source checkout."""

from __future__ import annotations

import argparse
import importlib.metadata
import pathlib
import sys

import nexusrpc


def smoke(version: str) -> None:
    """Verify the installed version and a public service definition."""
    if importlib.metadata.version("nexus-rpc") != version:
        raise RuntimeError("Installed nexus-rpc version does not match the release")
    package_path = pathlib.Path(nexusrpc.__file__).resolve()
    if not package_path.is_relative_to(pathlib.Path(sys.prefix).resolve()):
        raise RuntimeError(f"Package did not load from the test venv: {package_path}")
    if not package_path.with_name("py.typed").is_file():
        raise RuntimeError("Installed package is missing py.typed")

    @nexusrpc.service
    class Echo:
        echo: nexusrpc.Operation[str, str]

    definition = nexusrpc.get_service_definition(Echo)
    if definition is None or definition.name != "Echo":
        raise RuntimeError("Service definition failed")
    if (Echo.echo.input_type, Echo.echo.output_type) != (str, str):
        raise RuntimeError("Operation types were not preserved")
    print(f"Smoke test passed for nexus-rpc=={version} at {package_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    smoke(parser.parse_args().version)
