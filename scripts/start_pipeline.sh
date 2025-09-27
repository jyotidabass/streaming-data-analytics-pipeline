#!/bin/bash

# Start Streaming Analytics Pipeline
# This script starts the complete streaming analytics pipeline with all components

set -e

echo "🚀 Starting Streaming Analytics Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed. Please install docker-compose first."
    exit 1
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p data logs checkpoints notebooks

# Start infrastructure services
echo "🐳 Starting infrastructure services (Kafka, PostgreSQL, Redis, Spark)..."
docker-compose up -d zookeeper kafka postgres redis spark-master spark-worker

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check if services are running
echo "🔍 Checking service health..."
docker-compose ps

# Start the main pipeline
echo "📊 Starting the main analytics pipeline..."
python -m src.main --mode streaming --duration 60 --events-per-second 10

echo "✅ Pipeline started successfully!"
echo "📊 Dashboard available at: http://localhost:8501"
echo "🔧 API available at: http://localhost:8000"
echo "📈 Spark UI available at: http://localhost:8080"
echo "🗄️  Jupyter available at: http://localhost:8888"

echo "📝 To stop the pipeline, run: ./scripts/stop_pipeline.sh"
