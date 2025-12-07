#!/bin/sh
set -e

# Set CI mode for pnpm (non-interactive)
export CI=true

# Always sync dependencies on startup (handles stale volume + new packages)
# pnpm install is fast when deps are already installed
echo "Syncing dependencies..."
pnpm install --prefer-offline 2>/dev/null || pnpm install

# Execute the main command
exec "$@"
