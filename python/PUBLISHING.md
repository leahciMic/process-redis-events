# Publishing to PyPI

This guide explains how to publish the `process-redis-events` package to PyPI so others can install it with `pip install process-redis-events`.

## Prerequisites

1. **Create a PyPI account**
   - Go to https://pypi.org/account/register/
   - Verify your email address

2. **Create a TestPyPI account** (for testing before publishing)
   - Go to https://test.pypi.org/account/register/
   - Verify your email address

3. **Install build tools**
   ```bash
   pip install build twine
   ```

## Publishing Steps

### 1. Update Version Number

Edit `pyproject.toml` and update the version number:
```toml
version = "5.0.2"  # Increment this for each release
```

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR** version (5.x.x) - incompatible API changes
- **MINOR** version (x.0.x) - new functionality, backwards compatible
- **PATCH** version (x.x.2) - bug fixes, backwards compatible

### 2. Clean Previous Builds

```bash
rm -rf dist/ build/ *.egg-info
```

### 3. Build the Package

```bash
python -m build
```

This creates two files in the `dist/` directory:
- `process_redis_events-5.0.2-py3-none-any.whl` (wheel)
- `process_redis_events-5.0.2.tar.gz` (source distribution)

### 4. Test on TestPyPI (Recommended First Time)

Upload to TestPyPI first to make sure everything works:

```bash
python -m twine upload --repository testpypi dist/*
```

Enter your TestPyPI credentials when prompted.

Test installation from TestPyPI:
```bash
pip install --index-url https://test.pypi.org/simple/ process-redis-events
```

### 5. Publish to PyPI

Once you've verified everything works on TestPyPI:

```bash
python -m twine upload dist/*
```

Enter your PyPI credentials when prompted.

### 6. Verify Installation

Test that users can install your package:

```bash
pip install process-redis-events
```

## Using API Tokens (Recommended)

Instead of entering credentials each time, use API tokens:

### For PyPI:

1. Go to https://pypi.org/manage/account/token/
2. Create a new API token with scope "Entire account" or specific to this project
3. Create `~/.pypirc`:

```ini
[pypi]
username = __token__
password = pypi-AgEIcHlwaS5vcmc...  # Your actual token here
```

### For TestPyPI:

1. Go to https://test.pypi.org/manage/account/token/
2. Create a token
3. Add to `~/.pypirc`:

```ini
[testpypi]
username = __token__
password = pypi-AgENdGVzdC5weXBpLm9yZw...  # Your actual token here
```

Full `~/.pypirc` example:
```ini
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-AgEIcHlwaS5vcmc...

[testpypi]
username = __token__
password = pypi-AgENdGVzdC5weXBpLm9yZw...
```

## Automation with GitHub Actions (Optional)

You can automate publishing with GitHub Actions. Create `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install build twine

      - name: Build package
        run: python -m build

      - name: Publish to PyPI
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
        run: twine upload dist/*
```

Then add your PyPI token as a GitHub secret named `PYPI_API_TOKEN`.

## Quick Reference

```bash
# Complete publishing workflow:
cd python
rm -rf dist/ build/ *.egg-info
python -m build
python -m twine upload --repository testpypi dist/*  # Test first
python -m twine upload dist/*  # Publish to PyPI
```

## After Publishing

Users can now install your package with:

```bash
pip install process-redis-events
```

And use it in their code:

```python
from process_redis_events import Stream, StartFrom
from redis.asyncio import Redis

stream = Stream(
    name="my-stream",
    create_redis=lambda: Redis.from_url("redis://localhost"),
)
```

## Important Notes

- **You cannot delete or re-upload the same version** to PyPI. If you need to fix something, increment the version number.
- Always test on TestPyPI first
- Make sure all tests pass before publishing
- Update the README.md with any important changes
- Consider creating a CHANGELOG.md to track version changes
