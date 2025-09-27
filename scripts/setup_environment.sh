#!/bin/bash

# Setup Environment for Streaming Analytics Pipeline
# This script sets up the development environment and installs dependencies

set -e

echo "🔧 Setting up Streaming Analytics Pipeline environment..."

# Check Python version
echo "🐍 Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
required_version="3.8"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Python 3.8+ is required. Current version: $python_version"
    exit 1
fi

echo "✅ Python version: $python_version"

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📚 Installing Python dependencies..."
pip install -r requirements.txt

# Install additional development dependencies
echo "🛠️  Installing development dependencies..."
pip install jupyter ipykernel black flake8 pytest

# Add Jupyter kernel
echo "📓 Adding Jupyter kernel..."
python -m ipykernel install --user --name=streaming-analytics --display-name="Streaming Analytics"

# Create necessary directories
echo "📁 Creating project directories..."
mkdir -p data logs checkpoints notebooks src/{config,spark,kafka,sql,dask,cloud,dashboard}

# Set up environment variables
echo "🔐 Setting up environment variables..."
if [ ! -f .env ]; then
    cp config.env.example .env
    echo "📝 Created .env file from template. Please update with your configuration."
fi

# Make scripts executable
echo "🔧 Making scripts executable..."
chmod +x scripts/*.sh

# Download Spark (if not already present)
echo "⚡ Setting up Spark..."
if [ ! -d "spark" ]; then
    echo "📥 Downloading Apache Spark..."
    wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
    tar -xzf spark-3.5.0-bin-hadoop3.tgz
    mv spark-3.5.0-bin-hadoop3 spark
    rm spark-3.5.0-bin-hadoop3.tgz
    echo "✅ Spark downloaded and extracted"
else
    echo "✅ Spark already present"
fi

# Set up Git hooks (if in a Git repository)
if [ -d ".git" ]; then
    echo "🔗 Setting up Git hooks..."
    cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
# Run code formatting and linting before commit
echo "🔍 Running pre-commit checks..."
source venv/bin/activate
black --check src/ || (echo "❌ Code formatting issues found. Run 'black src/' to fix." && exit 1)
flake8 src/ || (echo "❌ Linting issues found. Please fix them." && exit 1)
echo "✅ Pre-commit checks passed"
EOF
    chmod +x .git/hooks/pre-commit
    echo "✅ Git hooks configured"
fi

# Create sample data
echo "📊 Creating sample data..."
python -c "
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Create sample user events
np.random.seed(42)
n_events = 10000
events = pd.DataFrame({
    'user_id': [f'user_{i:06d}' for i in range(n_events)],
    'session_id': [f'session_{i:08d}' for i in range(n_events)],
    'event_type': np.random.choice(['page_view', 'click', 'purchase', 'video_play'], n_events),
    'timestamp': pd.date_range(start='2024-01-01', periods=n_events, freq='1min'),
    'value': np.random.exponential(10, n_events),
    'category': np.random.choice(['A', 'B', 'C', 'D'], n_events),
    'device_type': np.random.choice(['Desktop', 'Mobile', 'Tablet'], n_events),
    'country': np.random.choice(['US', 'UK', 'CA', 'DE', 'FR'], n_events)
})

# Save to data directory
os.makedirs('data', exist_ok=True)
events.to_parquet('data/sample_events.parquet', index=False)
print(f'✅ Created sample data with {len(events)} events')
"

echo "🎉 Environment setup completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Update .env file with your configuration"
echo "2. Start the pipeline: ./scripts/start_pipeline.sh"
echo "3. Access the dashboard: http://localhost:8501"
echo "4. Access the API: http://localhost:8000"
echo ""
echo "📚 Available commands:"
echo "  - Start pipeline: ./scripts/start_pipeline.sh"
echo "  - Stop pipeline: ./scripts/stop_pipeline.sh"
echo "  - Run tests: pytest"
echo "  - Format code: black src/"
echo "  - Lint code: flake8 src/"
echo ""
echo "🔧 Development tools:"
echo "  - Jupyter: jupyter notebook"
echo "  - API docs: http://localhost:8000/docs"
echo "  - Spark UI: http://localhost:8080"
