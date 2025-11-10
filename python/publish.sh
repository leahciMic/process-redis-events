#!/usr/bin/env bash
# Script to help publish the package to PyPI
# Usage: ./publish.sh [test|prod]

set -e

MODE=${1:-test}

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "🐍 Activating virtual environment..."
    source venv/bin/activate
fi

# Check if required tools are available
if ! command -v pytest &> /dev/null; then
    echo "❌ pytest not found. Installing development dependencies..."
    pip install -e ".[dev]"
fi

if ! command -v python -m build &> /dev/null; then
    echo "❌ build not found. Installing build tools..."
    pip install build twine
fi

echo "🔧 Cleaning previous builds..."
rm -rf dist/ build/ *.egg-info process_redis_events.egg-info

echo "🧪 Running tests..."
pytest tests/ -v

echo "🔍 Type checking with mypy..."
mypy process_redis_events --pretty

echo "🎨 Formatting check..."
black --check process_redis_events tests

echo "📦 Building package..."
python -m build

echo "📋 Package contents:"
tar -tzf dist/*.tar.gz | head -20

if [ "$MODE" = "test" ]; then
    echo ""
    echo "📤 Uploading to TestPyPI..."
    python -m twine upload --repository testpypi dist/*
    echo ""
    echo "✅ Uploaded to TestPyPI!"
    echo ""
    echo "To test installation:"
    echo "  pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ process-redis-events"
    echo ""
    echo "To publish to production PyPI, run:"
    echo "  ./publish.sh prod"
elif [ "$MODE" = "prod" ]; then
    echo ""
    read -p "⚠️  Are you sure you want to publish to PyPI? This cannot be undone. (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
        echo "📤 Uploading to PyPI..."
        python -m twine upload dist/*
        echo ""
        echo "🎉 Successfully published to PyPI!"
        echo ""
        echo "Users can now install with:"
        echo "  pip install process-redis-events"
    else
        echo "❌ Cancelled."
        exit 1
    fi
else
    echo "❌ Invalid mode. Use 'test' or 'prod'"
    exit 1
fi
