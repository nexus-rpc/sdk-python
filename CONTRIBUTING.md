### Type-checking, Linting, and Formatting

```sh
uv run pyright
uv run mypy --check-untyped-defs .
uv run ruff check --select I
uv run ruff format --check
```

### Formatting
```
uv run ruff check --select I --fix
uv run ruff format
```