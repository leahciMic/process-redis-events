# Pre-Publishing Checklist

Before publishing to PyPI, make sure you've completed these steps:

## Code Quality

- [ ] All tests pass (`pytest tests/ -v`)
- [ ] Type checking passes (`mypy process_redis_events --pretty`)
- [ ] Code is formatted (`black process_redis_events tests`)
- [ ] No linting errors (`ruff check process_redis_events`)

## Documentation

- [ ] README.md has clear installation instructions
- [ ] README.md has usage examples
- [ ] All public APIs are documented
- [ ] CHANGELOG.md is updated (if you create one)

## Package Metadata

- [ ] Version number is correct in `pyproject.toml`
- [ ] Author information is correct
- [ ] License is specified (ISC in your case)
- [ ] Dependencies are up to date
- [ ] `requires-python` is set correctly (>=3.11)

## Files

- [ ] LICENSE file exists
- [ ] README.md exists
- [ ] No test files in package (check MANIFEST.in)
- [ ] No sensitive data (API keys, passwords, etc.)

## PyPI Setup

- [ ] PyPI account created (https://pypi.org)
- [ ] TestPyPI account created (https://test.pypi.org)
- [ ] API tokens generated (recommended)
- [ ] `~/.pypirc` configured (if using tokens)

## Testing

- [ ] Tested on TestPyPI first
- [ ] Verified installation from TestPyPI works
- [ ] Checked that imported module works correctly

## Final Steps

- [ ] Build package (`python -m build`)
- [ ] Check package contents (`tar -tzf dist/*.tar.gz`)
- [ ] Upload to TestPyPI (`python -m twine upload --repository testpypi dist/*`)
- [ ] Upload to PyPI (`python -m twine upload dist/*`)

## After Publishing

- [ ] Test installation: `pip install process-redis-events`
- [ ] Verify package page on PyPI
- [ ] Tag release in git (e.g., `git tag v5.0.2`)
- [ ] Push tags to GitHub (`git push --tags`)
- [ ] Create GitHub release with changelog

## Quick Start Commands

Install build tools:
```bash
pip install build twine
```

Build and publish to TestPyPI:
```bash
./publish.sh test
```

Publish to production PyPI:
```bash
./publish.sh prod
```

Or manually:
```bash
rm -rf dist/
python -m build
python -m twine upload --repository testpypi dist/*  # Test first
python -m twine upload dist/*  # Then production
```
