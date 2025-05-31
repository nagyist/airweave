#!/bin/bash
set -e  # Exit on error

echo "🌿 Generating Airweave API documentation..."

# Check if we're in the fern directory
if [[ $(basename "$PWD") != "fern" ]]; then
    echo "❌ Please run this script from the fern directory"
    exit 1
fi

# Make scripts executable if needed
chmod +x scripts/update_connector_docs.py
chmod +x scripts/generate_openapi.py

# Generate connector documentation
echo "📝 Generating connector documentation..."
python scripts/update_connector_docs.py

# Generate OpenAPI spec - this now directly writes to definition/openapi.json
echo "📝 Generating filtered OpenAPI spec..."
cd ../backend
poetry run python ../fern/scripts/generate_openapi.py
cd ../fern

# Ensure directory exists
mkdir -p definition

# Note: We no longer need to copy the OpenAPI spec since generate_openapi.py
# already writes to the correct location (definition/openapi.json)

# Check if fern CLI is installed
if ! command -v fern &> /dev/null; then
    echo "🔧 Installing Fern CLI..."
    npm install -g fern-api
fi

# Generate Fern docs
echo "📚 Generating Fern documentation..."
if [ -z "$FERN_TOKEN" ]; then
    echo "⚠️  Warning: FERN_TOKEN not set. Some features might not work."
fi

echo "🚀 Running Fern generators..."
fern generate --group public --log-level debug --version v0.1.45

echo "✅ Done! Generated files:"
ls -la definition/

# Generate docs
echo "📚 Generating Fern documentation..."
fern generate --docs
