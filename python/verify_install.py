#!/usr/bin/env python3
"""
Verify that process-redis-events is installed correctly.
Run this after installing the package to ensure everything works.
"""

import sys


def verify_installation():
    """Verify the package is installed and functional."""
    print("🔍 Verifying process-redis-events installation...\n")

    # Check imports
    try:
        from process_redis_events import Stream, StartFrom, QueueItem, StreamEvent

        print("✅ Core imports successful")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

    # Check version
    try:
        from process_redis_events import __version__

        print(f"✅ Version: {__version__}")
    except ImportError:
        print("⚠️  Version not found (might be running from source)")

    # Check type hints
    try:
        from typing import get_type_hints
        from process_redis_events import Stream

        hints = get_type_hints(Stream.__init__)
        print("✅ Type hints available")
    except Exception as e:
        print(f"⚠️  Type hints check failed: {e}")

    # Check dependencies
    print("\n📦 Checking dependencies:")
    dependencies = [
        ("redis", "Redis client"),
        ("pydantic", "Data validation"),
        ("opentelemetry.api", "OpenTelemetry integration"),
    ]

    all_deps_ok = True
    for module, description in dependencies:
        try:
            __import__(module)
            print(f"  ✅ {description}")
        except ImportError:
            print(f"  ❌ {description} - MISSING")
            all_deps_ok = False

    if not all_deps_ok:
        print("\n⚠️  Some dependencies are missing. Install with:")
        print("  pip install 'process-redis-events[dev]'")

    print("\n✨ Installation verification complete!")
    print("\nNext steps:")
    print("  1. Start Redis: docker run -p 6379:6379 redis")
    print("  2. Try the quick start example from the README")
    print("  3. Check out examples/ directory for more examples")

    return True


if __name__ == "__main__":
    success = verify_installation()
    sys.exit(0 if success else 1)
