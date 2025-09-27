#!/bin/bash

# Stop Streaming Analytics Pipeline
# This script stops all components of the streaming analytics pipeline

set -e

echo "🛑 Stopping Streaming Analytics Pipeline..."

# Stop the main pipeline process
echo "📊 Stopping main pipeline process..."
pkill -f "python -m src.main" || true

# Stop infrastructure services
echo "🐳 Stopping infrastructure services..."
docker-compose down

# Clean up temporary files
echo "🧹 Cleaning up temporary files..."
rm -rf /tmp/checkpoints/* /tmp/streaming_output/* /tmp/dask_results/* /tmp/batch_results/*

echo "✅ Pipeline stopped successfully!"
echo "📝 To start the pipeline again, run: ./scripts/start_pipeline.sh"
