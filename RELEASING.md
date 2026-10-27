# Releasing nexus-rpc

## Prepare and publish

1. Update `project.version` in `pyproject.toml` and run `uv lock` in a normal PR.
   Use a canonical three-part version, optionally with a prerelease, post-release,
   or development suffix (for example, `1.5.0rc1`). Review and merge the PR after
   CI passes. Release notes use GitHub's generated list of changes; no changelog
   files are required.
2. In GitHub Actions, dispatch **Release Publish** from `main`. The run uses the
   triggering commit throughout, even if `main` advances.
3. The workflow runs lint and tests, builds a universal wheel and sdist, verifies
   metadata and package contents, and installs both the wheel and a wheel rebuilt
   from the sdist in clean environments.
4. The verified artifact bundle is published to TestPyPI. Smoke tests install that
   exact version on Python 3.10 and 3.14 and exercise the public API. Dependencies
   come from PyPI, separately from the TestPyPI package.
5. Review the run and approve the `pypi` environment deployment. The same bundle
   is published to PyPI without rebuilding. PyPI smoke tests must pass before a
   draft GitHub release is created.
6. Review the generated notes and publish the draft release. Tags are bare
   versions, such as `1.5.0`, and target the verified commit.

Keep the run's `release-dist` artifact until the release is confirmed. Artifacts
are retained for 14 days.

## Failures and retries

- A failed build, validation, or TestPyPI smoke test blocks production publishing.
  Fix the problem and use a new version if already-published files would change.
- Index installation retries are bounded to accommodate propagation delays.
  After a transient smoke-test failure, rerun failed jobs in the original run.
- TestPyPI skips existing filenames to allow retries. Never assume this proves
  that previously uploaded files match a new build; use a fresh version when
  testing different artifacts.
- PyPI does not skip existing files. For partial or uncertain publication, compare
  the published files and SHA-256 hashes with the original verified bundle before
  taking action. Do not dispatch a new build or enable blind skipping to finish
  a partial release. A maintainer must either upload only the missing original
  files through an approved recovery procedure or release a new version.
- If PyPI publication succeeded but smoke tests or draft creation failed, rerun
  only failed jobs in the original run; do not republish. Check for an existing
  release and tag before retrying draft creation. An existing tag must point to
  the verified commit.

## Trusted Publishing setup

Create `testpypi` and `pypi` GitHub environments in `nexus-rpc/sdk-python`.
Restrict deployments to `main` and require approval, particularly for production.
Prevent self-approval and administrator bypass where supported.

Configure GitHub Trusted Publishers for the `nexus-rpc` project separately on
TestPyPI and PyPI:

| Setting | TestPyPI | PyPI |
| --- | --- | --- |
| Repository owner | `nexus-rpc` | `nexus-rpc` |
| Repository | `sdk-python` | `sdk-python` |
| Workflow file | `release-publish.yml` | `release-publish.yml` |
| Environment | `testpypi` | `pypi` |

Use **`release-publish.yml`**: it contains both publishing jobs. Do not leave the
publisher's environment field empty. TestPyPI and PyPI have separate accounts
and publisher configurations; configuring one does not configure the other.

Publishing uses GitHub OIDC. No stored PyPI API token or publishing username is needed.
See [PyPI Trusted Publishing setup](https://docs.pypi.org/trusted-publishers/adding-a-publisher/).
