"""Dashboard module for the streaming analytics pipeline."""

from .streamlit_app import StreamingAnalyticsDashboard
from .fastapi_app import app

__all__ = ["StreamingAnalyticsDashboard", "app"]
