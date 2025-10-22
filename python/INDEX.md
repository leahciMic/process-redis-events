# 📚 Documentation Index

Welcome to the Python port of `process-redis-events`! This index will help you find exactly what you need.

## 🚀 Getting Started (Start Here!)

1. **[OVERVIEW.md](OVERVIEW.md)** - Complete overview of what was delivered
2. **[QUICKSTART.md](QUICKSTART.md)** - Get up and running in 5 minutes
3. **[examples/README.md](examples/README.md)** - Learn by example

## 📖 Main Documentation

### For Users

- **[README.md](README.md)** - Full API documentation and usage guide
- **[QUICKSTART.md](QUICKSTART.md)** - Quick installation and basic usage
- **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** - Switching from TypeScript? Start here!

### For Developers

- **[DEVELOPMENT.md](DEVELOPMENT.md)** - Contributing and development guide
- **[STRUCTURE.md](STRUCTURE.md)** - Project structure and file organization
- **[PORT_SUMMARY.md](PORT_SUMMARY.md)** - Technical details of the port

### Reference

- **[examples/](examples/)** - 3 working examples
  - `basic_usage.py` - Complete workflow
  - `error_handling.py` - Retry logic and DLQ
  - `telemetry_example.py` - OpenTelemetry setup

## 🗺️ Navigation Guide

### "I want to..."

#### **...get started quickly**

→ [QUICKSTART.md](QUICKSTART.md)

#### **...understand the API**

→ [README.md](README.md)

#### **...see working code**

→ [examples/](examples/)

#### **...migrate from TypeScript**

→ [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)

#### **...contribute to development**

→ [DEVELOPMENT.md](DEVELOPMENT.md)

#### **...understand the project structure**

→ [STRUCTURE.md](STRUCTURE.md)

#### **...learn about the port**

→ [PORT_SUMMARY.md](PORT_SUMMARY.md)

#### **...get a high-level overview**

→ [OVERVIEW.md](OVERVIEW.md)

## 📑 Documentation Files

| File                                         | Purpose                | Audience        |
| -------------------------------------------- | ---------------------- | --------------- |
| **[README.md](README.md)**                   | Full API documentation | Users           |
| **[OVERVIEW.md](OVERVIEW.md)**               | Complete overview      | Everyone        |
| **[QUICKSTART.md](QUICKSTART.md)**           | Quick start guide      | New users       |
| **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** | TS → Python guide      | TS users        |
| **[DEVELOPMENT.md](DEVELOPMENT.md)**         | Dev guide              | Contributors    |
| **[STRUCTURE.md](STRUCTURE.md)**             | Project structure      | Developers      |
| **[PORT_SUMMARY.md](PORT_SUMMARY.md)**       | Port details           | Technical users |
| **[examples/README.md](examples/README.md)** | Examples guide         | All users       |

## 🔍 Quick Reference

### Installation

```bash
cd python
pip install -e .
```

### Basic Usage

```python
from process_redis_events import Stream, StartFrom
stream = Stream[MyData](name="test", create_redis=...)
await stream.add({"data": "value"})
```

### Running Tests

```bash
make test
```

### Type Checking

```bash
make typecheck
```

## 📚 Learning Path

### Beginner

1. Read [QUICKSTART.md](QUICKSTART.md)
2. Try [examples/basic_usage.py](examples/basic_usage.py)
3. Read [README.md](README.md) sections as needed

### Intermediate

1. Study all [examples/](examples/)
2. Read [README.md](README.md) fully
3. Understand [STRUCTURE.md](STRUCTURE.md)

### Advanced

1. Read [DEVELOPMENT.md](DEVELOPMENT.md)
2. Study [PORT_SUMMARY.md](PORT_SUMMARY.md)
3. Review source code with types

### TypeScript Users

1. Read [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)
2. Compare examples with TS version
3. Note API differences in guide

## 🎯 Common Tasks

### Running Examples

```bash
cd examples
python basic_usage.py
python error_handling.py
python telemetry_example.py
```

### Development Workflow

```bash
make install-dev  # Setup
make test         # Test
make typecheck    # Type check
make lint         # Lint
make format       # Format
```

### Getting Help

1. Check relevant documentation above
2. Look at [examples/](examples/)
3. Review [README.md](README.md) API docs
4. Study test cases in `tests/test_stream.py`

## 📦 Package Structure

```
python/
├── 📖 Documentation (8 files)
│   ├── README.md
│   ├── OVERVIEW.md
│   ├── QUICKSTART.md
│   ├── MIGRATION_GUIDE.md
│   ├── DEVELOPMENT.md
│   ├── STRUCTURE.md
│   ├── PORT_SUMMARY.md
│   └── INDEX.md (this file)
│
├── 📦 Package (14 modules)
│   └── process_redis_events/
│
├── 🧪 Tests (9 test cases)
│   └── tests/
│
├── 💡 Examples (3 demos)
│   └── examples/
│
└── ⚙️ Config (6 files)
    ├── pyproject.toml
    ├── requirements.txt
    ├── requirements-dev.txt
    ├── Makefile
    ├── LICENSE
    └── .gitignore
```

## 🎓 Best Practices

1. **Always use type hints** - Enables IDE autocomplete
2. **Handle shutdown gracefully** - Use `asyncio.Event`
3. **Test with Redis** - Use Docker for local testing
4. **Read examples first** - Understand patterns
5. **Enable telemetry** - Monitor in production

## 💡 Tips

- Use `Stream[YourType]` for type safety
- Start with [QUICKSTART.md](QUICKSTART.md) if new
- Check [examples/](examples/) for patterns
- Run `make test` to verify setup
- Use `make typecheck` before committing

## 🏆 Quality Checklist

When using this library:

✅ Define types with `TypedDict`
✅ Use type hints in callbacks
✅ Handle errors appropriately
✅ Configure retry logic
✅ Test with real Redis
✅ Enable telemetry in production
✅ Read relevant documentation

## 📞 Quick Links

- **Start Here**: [QUICKSTART.md](QUICKSTART.md)
- **Full API**: [README.md](README.md)
- **Examples**: [examples/](examples/)
- **TypeScript Users**: [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)
- **Contributing**: [DEVELOPMENT.md](DEVELOPMENT.md)

---

**Happy coding!** 🐍✨
