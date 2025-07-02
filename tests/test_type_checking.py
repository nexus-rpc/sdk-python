import re
import subprocess
from pathlib import Path

import pytest


def pytest_generate_tests(metafunc):
    """Dynamically generate test cases for files with type error assertions."""
    if metafunc.function.__name__ == "test_type_checking":
        tests_dir = Path(__file__).parent
        files_with_assertions = []

        for test_file in tests_dir.rglob("test_*.py"):
            if test_file.name == "test_type_checking.py":
                continue

            if _has_type_error_assertions(test_file):
                files_with_assertions.append(test_file)

        metafunc.parametrize("test_file", files_with_assertions, ids=lambda f: f.name)


def test_type_checking(test_file: Path):
    """
    Validate type error assertions in a single test file.

    For each line with a comment of the form `# assert-type-error: "pattern"`,
    verify that mypy reports an error on that line matching the pattern.
    Also verify that there are no unexpected type errors.
    """
    expected_errors = _get_expected_errors(test_file)
    actual_errors = _get_actual_errors(test_file)

    # Check that all expected errors are present and match
    for line_num, expected_pattern in expected_errors.items():
        if line_num not in actual_errors:
            pytest.fail(
                f"{test_file}:{line_num}: Expected type error matching '{expected_pattern}' but no error found"
            )

        actual_msg = actual_errors[line_num]
        if not re.search(expected_pattern, actual_msg):
            pytest.fail(
                f"{test_file}:{line_num}: Expected error matching '{expected_pattern}' but got '{actual_msg}'"
            )

    # Check that there are no unexpected errors
    for line_num, actual_msg in actual_errors.items():
        if line_num not in expected_errors:
            pytest.fail(f"{test_file}:{line_num}: Unexpected type error: {actual_msg}")


def _has_type_error_assertions(test_file: Path) -> bool:
    """Check if a file contains any type error assertions."""
    with open(test_file) as f:
        for line in f:
            if re.search(r'# assert-type-error:\s*["\'](.+)["\']', line):
                return True
    return False


def _get_expected_errors(test_file: Path) -> dict[int, str]:
    """Parse expected type errors from comments in a file."""
    expected_errors = {}
    with open(test_file) as f:
        for line_num, line in enumerate(f, 1):
            match = re.search(r'# assert-type-error:\s*["\'](.+)["\']', line)
            if match:
                expected_errors[line_num] = match.group(1)
    return expected_errors


def _get_actual_errors(test_file: Path) -> dict[int, str]:
    """Run mypy on a file and parse the actual type errors."""
    result = subprocess.run(
        ["uv", "run", "mypy", "--check-untyped-defs", str(test_file)],
        capture_output=True,
        text=True,
    )

    actual_errors = {}
    for line in result.stdout.splitlines():
        # mypy output format: filename:line: error: message (uses relative path from cwd)
        rel_path = test_file.relative_to(Path.cwd())
        match = re.match(rf"{re.escape(str(rel_path))}:(\d+):\s*error:\s*(.+)", line)
        if match:
            line_num = int(match.group(1))
            error_msg = match.group(2)
            actual_errors[line_num] = error_msg

    return actual_errors
