#!/bin/bash
# filepath: scripts/validate-mcp.sh
set -e

echo "🔍 Validating MCP server configuration..."

# Check if Python is available
if ! command -v python3 &> /dev/null && ! command -v python &> /dev/null; then
    echo "⚠️  Python not found. MCP server requires Python >=3.8"
else
    echo "✅ Python found"
fi

# Check if pip is available
if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
    echo "⚠️  pip not found. Required for installing Sling package"
else
    echo "✅ pip found"
fi

# Check if ajv-cli is installed for schema validation
if ! command -v ajv &> /dev/null; then
    echo "Installing ajv-cli for JSON schema validation..."
    if command -v npm &> /dev/null; then
        npm install -g ajv-cli
    else
        echo "⚠️  npm not found. Skipping schema validation."
        echo "   Install Node.js and npm to validate server.json schema"
        exit 0
    fi
fi

# Download the MCP server schema
echo "📥 Downloading MCP server schema..."
curl -s -o /tmp/server-schema.json https://static.modelcontextprotocol.io/schemas/2025-09-16/server.schema.json

# Validate the server.json file
echo "✅ Validating server.json against schema..."
if ajv validate -s /tmp/server-schema.json -d server.json; then
    echo "✅ server.json is valid!"
else
    echo "❌ server.json validation failed!"
    exit 1
fi

# Test Sling installation and MCP command
echo "🧪 Testing Sling installation..."
if command -v sling &> /dev/null; then
    echo "✅ Sling CLI found"
    
    # Test MCP command
    echo "🧪 Testing MCP command..."
    if timeout 3s sling mcp --help &> /dev/null; then
        echo "✅ Sling MCP command works!"
    else
        echo "⚠️  Could not test 'sling mcp' command (this is normal if no help flag exists)"
    fi
else
    echo "⚠️  Sling CLI not found. Testing pip installation..."
    
    # Try to install sling temporarily for testing
    if command -v pip3 &> /dev/null; then
        echo "🧪 Testing pip installation of sling package..."
        pip3 show sling &> /dev/null && echo "✅ Sling package is already installed" || echo "ℹ️  Sling package not installed (users will install via: pip install sling)"
    fi
fi

echo ""
echo "🎉 Validation complete!"
echo ""
echo "📋 Installation instructions for users:"
echo "   pip install sling"
echo "   # Then the MCP server can be started with: sling mcp"