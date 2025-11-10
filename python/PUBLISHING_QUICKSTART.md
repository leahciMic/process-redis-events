# Publishing Your Python Package - Quick Summary

## What You Need

Your package is **ready to publish**! Here's what's already set up:

✅ **pyproject.toml** - Package configuration
✅ **README.md** - Installation & usage docs
✅ **LICENSE** - ISC license
✅ **Tests** - All 8 tests passing
✅ **Type hints** - Full mypy coverage
✅ **Version** - 5.0.2 in both `__init__.py` and `pyproject.toml`

## One-Time Setup (5 minutes)

### 1. Install Publishing Tools

```bash
cd python
source venv/bin/activate
pip install build twine
```

### 2. Create PyPI Accounts

- **Production PyPI**: https://pypi.org/account/register/
- **Test PyPI**: https://test.pypi.org/account/register/

### 3. Generate API Tokens

**For TestPyPI:**
1. Go to https://test.pypi.org/manage/account/token/
2. Click "Add API token"
3. Name it "process-redis-events-test"
4. Scope: "Entire account" (or specific to project later)
5. Copy the token (starts with `pypi-...`)

**For PyPI:**
1. Go to https://pypi.org/manage/account/token/
2. Click "Add API token"
3. Name it "process-redis-events"
4. Scope: "Entire account"
5. Copy the token

### 4. Configure Credentials

Create `~/.pypirc`:

```bash
cat > ~/.pypirc << 'EOF'
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-PASTE_YOUR_PYPI_TOKEN_HERE

[testpypi]
username = __token__
password = pypi-PASTE_YOUR_TESTPYPI_TOKEN_HERE
EOF

chmod 600 ~/.pypirc  # Secure the file
```

## Publishing (Every Release)

### Option 1: Use the Script (Easiest)

```bash
cd python

# Test on TestPyPI first (recommended)
./publish.sh test

# Then publish to production
./publish.sh prod
```

### Option 2: Manual Steps

```bash
cd python

# 1. Clean previous builds
rm -rf dist/ build/ *.egg-info

# 2. Run tests
pytest tests/ -v

# 3. Build package
python -m build

# 4. Upload to TestPyPI (test first!)
python -m twine upload --repository testpypi dist/*

# 5. Test installation
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ process-redis-events

# 6. If everything works, upload to PyPI
python -m twine upload dist/*
```

## After Publishing

Users can install your package:

```bash
pip install process-redis-events
```

And use it:

```python
from process_redis_events import Stream, StartFrom
from redis.asyncio import Redis

stream = Stream(
    name="my-stream",
    create_redis=lambda: Redis.from_url("redis://localhost"),
)
```

## Version Updates

For each new release:

1. Update version in **both**:
   - `pyproject.toml` → `version = "5.0.3"`
   - `process_redis_events/__init__.py` → `__version__ = "5.0.3"`

2. Follow semantic versioning:
   - `5.0.3` → Patch (bug fixes)
   - `5.1.0` → Minor (new features, backwards compatible)
   - `6.0.0` → Major (breaking changes)

3. Rebuild and publish

## Troubleshooting

**"Package already exists"**
- Can't re-upload same version. Increment version number.

**"Invalid credentials"**
- Check `~/.pypirc` has correct tokens
- Verify tokens at pypi.org/manage/account/token/

**"Missing dependencies in installed package"**
- Check `dependencies` in `pyproject.toml`
- Rebuild with `python -m build`

**"Module not found after pip install"**
- Check package is installed: `pip show process-redis-events`
- Try: `pip install --upgrade process-redis-events`

## Useful Links

- **Your package page**: https://pypi.org/project/process-redis-events/ (after publishing)
- **TestPyPI page**: https://test.pypi.org/project/process-redis-events/ (after test upload)
- **PyPI docs**: https://packaging.python.org/
- **Twine docs**: https://twine.readthedocs.io/

## Next Steps

1. Run `./publish.sh test` to upload to TestPyPI
2. Verify it installs correctly
3. Run `./publish.sh prod` to publish to PyPI
4. Celebrate! 🎉

Your package will be available to millions of Python developers worldwide!
