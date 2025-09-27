"""Streamlit dashboard for streaming analytics pipeline monitoring."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import time
from datetime import datetime, timedelta
import logging

from ..config import settings
from ..kafka import StreamingDataConsumer, UserEventProcessor, AnalyticsEventProcessor
from ..sql import SQLOptimizer
from ..dask import DaskDistributedProcessor

logger = logging.getLogger(__name__)

# Configure Streamlit page
st.set_page_config(
    page_title="Streaming Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .status-healthy {
        color: #28a745;
        font-weight: bold;
    }
    .status-warning {
        color: #ffc107;
        font-weight: bold;
    }
    .status-error {
        color: #dc3545;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

class StreamingAnalyticsDashboard:
    """Main dashboard class for streaming analytics."""
    
    def __init__(self):
        """Initialize dashboard components."""
        self.sql_optimizer = None
        self.dask_processor = None
        self.user_processor = UserEventProcessor()
        self.analytics_processor = AnalyticsEventProcessor()
        
        # Initialize session state
        if 'dashboard_data' not in st.session_state:
            st.session_state.dashboard_data = {}
        if 'last_update' not in st.session_state:
            st.session_state.last_update = datetime.now()
    
    def initialize_components(self):
        """Initialize dashboard components."""
        try:
            self.sql_optimizer = SQLOptimizer()
            self.dask_processor = DaskDistributedProcessor()
            return True
        except Exception as e:
            st.error(f"Error initializing components: {e}")
            return False
    
    def render_header(self):
        """Render dashboard header."""
        st.markdown('<h1 class="main-header">📊 Streaming Analytics Dashboard</h1>', unsafe_allow_html=True)
        
        # Status indicators
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Pipeline Status", "🟢 Healthy", delta="0%")
        
        with col2:
            st.metric("Events Processed", "1,234,567", delta="12.3%")
        
        with col3:
            st.metric("Active Users", "8,901", delta="5.7%")
        
        with col4:
            st.metric("System Load", "45%", delta="-2.1%")
    
    def render_sidebar(self):
        """Render sidebar with controls."""
        st.sidebar.title("Dashboard Controls")
        
        # Time range selector
        st.sidebar.subheader("Time Range")
        time_range = st.sidebar.selectbox(
            "Select time range",
            ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days", "Custom"]
        )
        
        if time_range == "Custom":
            start_date = st.sidebar.date_input("Start Date", datetime.now().date())
            end_date = st.sidebar.date_input("End Date", datetime.now().date())
            start_time = st.sidebar.time_input("Start Time", datetime.now().time())
            end_time = st.sidebar.time_input("End Time", datetime.now().time())
        
        # Refresh controls
        st.sidebar.subheader("Refresh Settings")
        auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)
        refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 10, 300, 60)
        
        # Data source selection
        st.sidebar.subheader("Data Sources")
        show_kafka = st.sidebar.checkbox("Kafka Streams", value=True)
        show_database = st.sidebar.checkbox("Database", value=True)
        show_cloud = st.sidebar.checkbox("Cloud Storage", value=False)
        
        # Export options
        st.sidebar.subheader("Export Options")
        if st.sidebar.button("Export Dashboard Data"):
            self.export_dashboard_data()
        
        return {
            'time_range': time_range,
            'auto_refresh': auto_refresh,
            'refresh_interval': refresh_interval,
            'show_kafka': show_kafka,
            'show_database': show_database,
            'show_cloud': show_cloud
        }
    
    def render_real_time_metrics(self):
        """Render real-time metrics section."""
        st.subheader("📈 Real-time Metrics")
        
        # Create columns for metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Events/Second",
                "1,234",
                delta="12.3%",
                help="Current event processing rate"
            )
        
        with col2:
            st.metric(
                "Active Sessions",
                "5,678",
                delta="8.9%",
                help="Number of active user sessions"
            )
        
        with col3:
            st.metric(
                "Conversion Rate",
                "3.45%",
                delta="0.23%",
                help="Current conversion rate"
            )
        
        with col4:
            st.metric(
                "Error Rate",
                "0.12%",
                delta="-0.05%",
                help="Current error rate"
            )
    
    def render_event_analytics(self):
        """Render event analytics charts."""
        st.subheader("🎯 Event Analytics")
        
        # Generate sample data for demonstration
        event_types = ['page_view', 'click', 'purchase', 'video_play', 'search']
        event_counts = [45000, 12000, 800, 5600, 3400]
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Event type distribution
            fig_pie = px.pie(
                values=event_counts,
                names=event_types,
                title="Event Type Distribution",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # Event timeline
            timeline_data = pd.DataFrame({
                'timestamp': pd.date_range(start=datetime.now() - timedelta(hours=24), 
                                         end=datetime.now(), freq='1H'),
                'events': [1000 + i * 50 + (i % 3) * 200 for i in range(25)]
            })
            
            fig_line = px.line(
                timeline_data,
                x='timestamp',
                y='events',
                title="Events Over Time",
                labels={'events': 'Number of Events', 'timestamp': 'Time'}
            )
            fig_line.update_layout(height=400)
            st.plotly_chart(fig_line, use_container_width=True)
    
    def render_user_analytics(self):
        """Render user analytics section."""
        st.subheader("👥 User Analytics")
        
        # Generate sample user data
        user_data = pd.DataFrame({
            'user_id': [f'user_{i}' for i in range(100)],
            'sessions': [1, 2, 3, 4, 5] * 20,
            'events': [10, 25, 50, 100, 200] * 20,
            'country': ['US', 'UK', 'CA', 'DE', 'FR'] * 20,
            'device': ['Desktop', 'Mobile', 'Tablet'] * 33 + ['Desktop']
        })
        
        col1, col2 = st.columns(2)
        
        with col1:
            # User engagement distribution
            fig_hist = px.histogram(
                user_data,
                x='events',
                nbins=20,
                title="User Engagement Distribution",
                labels={'events': 'Events per User', 'count': 'Number of Users'}
            )
            fig_hist.update_layout(height=400)
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            # Geographic distribution
            country_counts = user_data['country'].value_counts()
            fig_bar = px.bar(
                x=country_counts.index,
                y=country_counts.values,
                title="Users by Country",
                labels={'x': 'Country', 'y': 'Number of Users'}
            )
            fig_bar.update_layout(height=400)
            st.plotly_chart(fig_bar, use_container_width=True)
    
    def render_performance_metrics(self):
        """Render performance metrics section."""
        st.subheader("⚡ Performance Metrics")
        
        # Generate sample performance data
        performance_data = pd.DataFrame({
            'timestamp': pd.date_range(start=datetime.now() - timedelta(hours=24), 
                                     end=datetime.now(), freq='1H'),
            'cpu_usage': [45, 50, 55, 60, 58, 52, 48, 46, 49, 51, 53, 56, 54, 50, 47, 45, 48, 52, 55, 57, 54, 51, 49, 46, 44],
            'memory_usage': [60, 65, 70, 75, 72, 68, 64, 62, 66, 69, 71, 74, 72, 68, 65, 63, 67, 70, 73, 75, 72, 69, 67, 64, 62],
            'disk_usage': [30, 32, 35, 38, 36, 34, 31, 30, 33, 35, 37, 39, 37, 34, 32, 31, 34, 36, 38, 40, 37, 35, 33, 31, 30]
        })
        
        # Create subplots
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=('CPU Usage (%)', 'Memory Usage (%)', 'Disk Usage (%)'),
            vertical_spacing=0.1
        )
        
        # Add traces
        fig.add_trace(
            go.Scatter(x=performance_data['timestamp'], y=performance_data['cpu_usage'], 
                      name='CPU', line=dict(color='blue')),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=performance_data['timestamp'], y=performance_data['memory_usage'], 
                      name='Memory', line=dict(color='green')),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=performance_data['timestamp'], y=performance_data['disk_usage'], 
                      name='Disk', line=dict(color='red')),
            row=3, col=1
        )
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    def render_system_health(self):
        """Render system health section."""
        st.subheader("🏥 System Health")
        
        # Health status indicators
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h4>Kafka Cluster</h4>
                <p class="status-healthy">🟢 Healthy</p>
                <p>3/3 brokers active</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h4>Spark Cluster</h4>
                <p class="status-healthy">🟢 Healthy</p>
                <p>4/4 workers active</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-card">
                <h4>Database</h4>
                <p class="status-warning">🟡 Warning</p>
                <p>Connection pool 85%</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
            <div class="metric-card">
                <h4>Cloud Storage</h4>
                <p class="status-healthy">🟢 Healthy</p>
                <p>All services operational</p>
            </div>
            """, unsafe_allow_html=True)
    
    def render_alerts(self):
        """Render alerts and notifications section."""
        st.subheader("🚨 Alerts & Notifications")
        
        # Sample alerts
        alerts = [
            {"type": "warning", "message": "Database connection pool usage is high (85%)", "time": "2 minutes ago"},
            {"type": "info", "message": "Scheduled maintenance completed successfully", "time": "1 hour ago"},
            {"type": "success", "message": "New data pipeline deployed successfully", "time": "3 hours ago"},
            {"type": "error", "message": "Kafka consumer lag detected in analytics topic", "time": "5 hours ago"}
        ]
        
        for alert in alerts:
            if alert["type"] == "error":
                st.error(f"🔴 {alert['message']} - {alert['time']}")
            elif alert["type"] == "warning":
                st.warning(f"🟡 {alert['message']} - {alert['time']}")
            elif alert["type"] == "info":
                st.info(f"🔵 {alert['message']} - {alert['time']}")
            else:
                st.success(f"🟢 {alert['message']} - {alert['time']}")
    
    def render_data_quality(self):
        """Render data quality metrics."""
        st.subheader("🔍 Data Quality Metrics")
        
        # Generate sample data quality metrics
        quality_metrics = pd.DataFrame({
            'Metric': ['Completeness', 'Accuracy', 'Consistency', 'Timeliness', 'Validity'],
            'Score': [95.2, 98.7, 92.1, 89.5, 96.8],
            'Status': ['Good', 'Excellent', 'Good', 'Fair', 'Excellent']
        })
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Data quality scores
            fig_bar = px.bar(
                quality_metrics,
                x='Metric',
                y='Score',
                title="Data Quality Scores",
                color='Score',
                color_continuous_scale='RdYlGn'
            )
            fig_bar.update_layout(height=400)
            st.plotly_chart(fig_bar, use_container_width=True)
        
        with col2:
            # Data quality table
            st.dataframe(quality_metrics, use_container_width=True)
    
    def export_dashboard_data(self):
        """Export dashboard data to various formats."""
        try:
            # Generate sample export data
            export_data = {
                'timestamp': datetime.now().isoformat(),
                'metrics': {
                    'total_events': 1234567,
                    'unique_users': 8901,
                    'conversion_rate': 3.45,
                    'error_rate': 0.12
                },
                'system_health': {
                    'kafka': 'healthy',
                    'spark': 'healthy',
                    'database': 'warning',
                    'cloud_storage': 'healthy'
                }
            }
            
            # Convert to JSON
            json_data = json.dumps(export_data, indent=2)
            
            # Create download button
            st.download_button(
                label="Download Dashboard Data (JSON)",
                data=json_data,
                file_name=f"dashboard_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
            
            st.success("Dashboard data exported successfully!")
            
        except Exception as e:
            st.error(f"Error exporting data: {e}")
    
    def run_dashboard(self):
        """Run the main dashboard."""
        # Initialize components
        if not self.initialize_components():
            st.error("Failed to initialize dashboard components")
            return
        
        # Render header
        self.render_header()
        
        # Render sidebar and get controls
        controls = self.render_sidebar()
        
        # Main content tabs
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📊 Overview", "🎯 Events", "👥 Users", "⚡ Performance", "🏥 Health", "🔍 Quality"
        ])
        
        with tab1:
            self.render_real_time_metrics()
            self.render_system_health()
            self.render_alerts()
        
        with tab2:
            self.render_event_analytics()
        
        with tab3:
            self.render_user_analytics()
        
        with tab4:
            self.render_performance_metrics()
        
        with tab5:
            self.render_system_health()
            self.render_alerts()
        
        with tab6:
            self.render_data_quality()
        
        # Auto refresh functionality
        if controls['auto_refresh']:
            time.sleep(controls['refresh_interval'])
            st.rerun()


# Main function to run the dashboard
def main():
    """Main function to run the Streamlit dashboard."""
    dashboard = StreamingAnalyticsDashboard()
    dashboard.run_dashboard()


if __name__ == "__main__":
    main()
