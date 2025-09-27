"""SQL optimization examples for large datasets."""

import logging
from typing import Dict, Any, List, Optional
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime, Float, Index
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, func, case, and_, or_
import pandas as pd
from datetime import datetime, timedelta

from ..config import settings

logger = logging.getLogger(__name__)


class SQLOptimizer:
    """SQL optimization utilities for large dataset processing."""
    
    def __init__(self):
        """Initialize database connection and metadata."""
        self.engine = create_engine(settings.database_url, pool_size=20, max_overflow=30)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        self._define_tables()
    
    def _define_tables(self):
        """Define database table schemas."""
        self.user_events = Table(
            'user_events',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', String(50), nullable=False),
            Column('session_id', String(50), nullable=False),
            Column('event_type', String(50), nullable=False),
            Column('event_data', String(1000)),
            Column('timestamp', DateTime, nullable=False),
            Column('ip_address', String(45)),
            Column('user_agent', String(500)),
            Column('page_url', String(500)),
            Column('referrer', String(500)),
            Column('device_type', String(20)),
            Column('browser', String(50)),
            Column('country', String(10)),
            Column('created_at', DateTime, default=datetime.utcnow)
        )
        
        self.user_sessions = Table(
            'user_sessions',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('session_id', String(50), nullable=False, unique=True),
            Column('user_id', String(50), nullable=False),
            Column('start_time', DateTime, nullable=False),
            Column('end_time', DateTime),
            Column('duration_seconds', Integer),
            Column('page_views', Integer),
            Column('unique_pages', Integer),
            Column('device_type', String(20)),
            Column('country', String(10)),
            Column('created_at', DateTime, default=datetime.utcnow)
        )
        
        self.analytics_metrics = Table(
            'analytics_metrics',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('metric_name', String(100), nullable=False),
            Column('metric_value', Float, nullable=False),
            Column('dimensions', String(1000)),
            Column('timestamp', DateTime, nullable=False),
            Column('source', String(100)),
            Column('created_at', DateTime, default=datetime.utcnow)
        )
        
        # Create indexes for optimization
        self._create_indexes()
    
    def _create_indexes(self):
        """Create database indexes for query optimization."""
        indexes = [
            Index('idx_user_events_user_id', self.user_events.c.user_id),
            Index('idx_user_events_timestamp', self.user_events.c.timestamp),
            Index('idx_user_events_event_type', self.user_events.c.event_type),
            Index('idx_user_events_session_id', self.user_events.c.session_id),
            Index('idx_user_events_composite', self.user_events.c.user_id, self.user_events.c.timestamp),
            
            Index('idx_sessions_user_id', self.user_sessions.c.user_id),
            Index('idx_sessions_start_time', self.user_sessions.c.start_time),
            Index('idx_sessions_duration', self.user_sessions.c.duration_seconds),
            
            Index('idx_metrics_name_timestamp', self.analytics_metrics.c.metric_name, self.analytics_metrics.c.timestamp),
            Index('idx_metrics_timestamp', self.analytics_metrics.c.timestamp)
        ]
        
        for index in indexes:
            try:
                index.create(self.engine, checkfirst=True)
            except Exception as e:
                logger.warning(f"Could not create index {index.name}: {e}")
    
    def create_tables(self):
        """Create all tables."""
        self.metadata.create_all(self.engine)
        logger.info("Database tables created successfully")
    
    def optimize_query_1_user_activity_analysis(self) -> str:
        """Optimized query for user activity analysis with proper indexing."""
        query = """
        -- User Activity Analysis with Window Functions
        WITH user_activity AS (
            SELECT 
                user_id,
                DATE(timestamp) as activity_date,
                COUNT(*) as daily_events,
                COUNT(DISTINCT session_id) as daily_sessions,
                COUNT(DISTINCT event_type) as event_types_used,
                MIN(timestamp) as first_event,
                MAX(timestamp) as last_event
            FROM user_events 
            WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY user_id, DATE(timestamp)
        ),
        user_metrics AS (
            SELECT 
                user_id,
                AVG(daily_events) as avg_daily_events,
                AVG(daily_sessions) as avg_daily_sessions,
                SUM(daily_events) as total_events_30d,
                COUNT(DISTINCT activity_date) as active_days,
                MAX(daily_events) as peak_daily_events
            FROM user_activity
            GROUP BY user_id
        )
        SELECT 
            user_id,
            avg_daily_events,
            avg_daily_sessions,
            total_events_30d,
            active_days,
            peak_daily_events,
            CASE 
                WHEN active_days >= 20 THEN 'Highly Active'
                WHEN active_days >= 10 THEN 'Moderately Active'
                WHEN active_days >= 5 THEN 'Occasionally Active'
                ELSE 'Rarely Active'
            END as activity_level
        FROM user_metrics
        ORDER BY total_events_30d DESC
        LIMIT 1000;
        """
        return query
    
    def optimize_query_2_session_analysis(self) -> str:
        """Optimized query for session analysis with proper joins."""
        query = """
        -- Session Analysis with Optimized Joins
        SELECT 
            s.session_id,
            s.user_id,
            s.duration_seconds,
            s.page_views,
            s.unique_pages,
            s.device_type,
            s.country,
            COUNT(e.id) as actual_events,
            COUNT(DISTINCT e.event_type) as event_types,
            COUNT(DISTINCT e.page_url) as pages_visited,
            AVG(CASE WHEN e.event_type = 'page_view' THEN 1 ELSE 0 END) as page_view_ratio
        FROM user_sessions s
        LEFT JOIN user_events e ON s.session_id = e.session_id
        WHERE s.start_time >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY s.session_id, s.user_id, s.duration_seconds, s.page_views, 
                 s.unique_pages, s.device_type, s.country
        HAVING COUNT(e.id) > 0
        ORDER BY s.duration_seconds DESC
        LIMIT 5000;
        """
        return query
    
    def optimize_query_3_real_time_metrics(self) -> str:
        """Optimized query for real-time metrics calculation."""
        query = """
        -- Real-time Metrics with Time Windows
        WITH time_windows AS (
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour_window,
                DATE_TRUNC('minute', timestamp) as minute_window
            FROM user_events
            WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            GROUP BY DATE_TRUNC('hour', timestamp), DATE_TRUNC('minute', timestamp)
        ),
        hourly_metrics AS (
            SELECT 
                DATE_TRUNC('hour', e.timestamp) as hour_window,
                COUNT(*) as total_events,
                COUNT(DISTINCT e.user_id) as unique_users,
                COUNT(DISTINCT e.session_id) as unique_sessions,
                COUNT(DISTINCT e.event_type) as event_types,
                COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) as purchases,
                COUNT(CASE WHEN e.event_type = 'video_complete' THEN 1 END) as video_completions
            FROM user_events e
            WHERE e.timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            GROUP BY DATE_TRUNC('hour', e.timestamp)
        ),
        minute_metrics AS (
            SELECT 
                DATE_TRUNC('minute', e.timestamp) as minute_window,
                COUNT(*) as events_per_minute,
                COUNT(DISTINCT e.user_id) as users_per_minute
            FROM user_events e
            WHERE e.timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
            GROUP BY DATE_TRUNC('minute', e.timestamp)
        )
        SELECT 
            h.hour_window,
            h.total_events,
            h.unique_users,
            h.unique_sessions,
            h.event_types,
            h.purchases,
            h.video_completions,
            ROUND(h.purchases::DECIMAL / NULLIF(h.total_events, 0) * 100, 2) as conversion_rate,
            ROUND(h.video_completions::DECIMAL / NULLIF(h.total_events, 0) * 100, 2) as video_completion_rate
        FROM hourly_metrics h
        ORDER BY h.hour_window DESC;
        """
        return query
    
    def optimize_query_4_cohort_analysis(self) -> str:
        """Optimized query for cohort analysis."""
        query = """
        -- Cohort Analysis with Efficient Date Calculations
        WITH user_first_activity AS (
            SELECT 
                user_id,
                DATE(MIN(timestamp)) as first_activity_date,
                DATE_TRUNC('week', MIN(timestamp)) as cohort_week
            FROM user_events
            GROUP BY user_id
        ),
        user_activity_by_week AS (
            SELECT 
                ufa.user_id,
                ufa.cohort_week,
                DATE_TRUNC('week', e.timestamp) as activity_week,
                COUNT(DISTINCT e.session_id) as sessions_in_week
            FROM user_first_activity ufa
            JOIN user_events e ON ufa.user_id = e.user_id
            WHERE e.timestamp >= ufa.first_activity_date
            GROUP BY ufa.user_id, ufa.cohort_week, DATE_TRUNC('week', e.timestamp)
        ),
        cohort_sizes AS (
            SELECT 
                cohort_week,
                COUNT(DISTINCT user_id) as cohort_size
            FROM user_first_activity
            GROUP BY cohort_week
        ),
        retention_data AS (
            SELECT 
                uabw.cohort_week,
                uabw.activity_week,
                COUNT(DISTINCT uabw.user_id) as active_users,
                EXTRACT(WEEK FROM uabw.activity_week) - EXTRACT(WEEK FROM uabw.cohort_week) as week_number
            FROM user_activity_by_week uabw
            GROUP BY uabw.cohort_week, uabw.activity_week
        )
        SELECT 
            rd.cohort_week,
            cs.cohort_size,
            rd.week_number,
            rd.active_users,
            ROUND(rd.active_users::DECIMAL / cs.cohort_size * 100, 2) as retention_rate
        FROM retention_data rd
        JOIN cohort_sizes cs ON rd.cohort_week = cs.cohort_week
        WHERE rd.week_number <= 12  -- First 12 weeks
        ORDER BY rd.cohort_week DESC, rd.week_number;
        """
        return query
    
    def optimize_query_5_funnel_analysis(self) -> str:
        """Optimized query for funnel analysis."""
        query = """
        -- Funnel Analysis with Efficient Event Tracking
        WITH funnel_events AS (
            SELECT 
                user_id,
                session_id,
                timestamp,
                event_type,
                ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY timestamp) as event_sequence
            FROM user_events
            WHERE event_type IN ('page_view', 'click', 'add_to_cart', 'purchase')
            AND timestamp >= CURRENT_DATE - INTERVAL '30 days'
        ),
        funnel_steps AS (
            SELECT 
                user_id,
                session_id,
                MAX(CASE WHEN event_type = 'page_view' THEN event_sequence END) as step_1,
                MAX(CASE WHEN event_type = 'click' THEN event_sequence END) as step_2,
                MAX(CASE WHEN event_type = 'add_to_cart' THEN event_sequence END) as step_3,
                MAX(CASE WHEN event_type = 'purchase' THEN event_sequence END) as step_4
            FROM funnel_events
            GROUP BY user_id, session_id
        ),
        funnel_conversion AS (
            SELECT 
                COUNT(*) as total_sessions,
                COUNT(step_1) as step_1_completions,
                COUNT(step_2) as step_2_completions,
                COUNT(step_3) as step_3_completions,
                COUNT(step_4) as step_4_completions
            FROM funnel_steps
        )
        SELECT 
            'Page View' as step_name,
            step_1_completions as completions,
            ROUND(step_1_completions::DECIMAL / total_sessions * 100, 2) as conversion_rate
        FROM funnel_conversion
        UNION ALL
        SELECT 
            'Click' as step_name,
            step_2_completions as completions,
            ROUND(step_2_completions::DECIMAL / step_1_completions * 100, 2) as conversion_rate
        FROM funnel_conversion
        UNION ALL
        SELECT 
            'Add to Cart' as step_name,
            step_3_completions as completions,
            ROUND(step_3_completions::DECIMAL / step_2_completions * 100, 2) as conversion_rate
        FROM funnel_conversion
        UNION ALL
        SELECT 
            'Purchase' as step_name,
            step_4_completions as completions,
            ROUND(step_4_completions::DECIMAL / step_3_completions * 100, 2) as conversion_rate
        FROM funnel_conversion
        ORDER BY completions DESC;
        """
        return query
    
    def execute_optimized_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute an optimized query and return results as DataFrame."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return pd.DataFrame()
    
    def analyze_query_performance(self, query: str) -> Dict[str, Any]:
        """Analyze query performance using EXPLAIN ANALYZE."""
        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
        
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(explain_query))
                performance_data = result.fetchone()[0]
                
                return {
                    "execution_time": performance_data[0].get("Execution Time", 0),
                    "planning_time": performance_data[0].get("Planning Time", 0),
                    "total_cost": performance_data[0].get("Total Cost", 0),
                    "plan": performance_data[0].get("Plan", {})
                }
        except Exception as e:
            logger.error(f"Error analyzing query performance: {e}")
            return {}
    
    def create_materialized_view(self, view_name: str, query: str):
        """Create a materialized view for frequently accessed data."""
        create_view_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
        {query}
        """
        
        try:
            with self.engine.connect() as connection:
                connection.execute(text(create_view_query))
                connection.commit()
                logger.info(f"Materialized view {view_name} created successfully")
        except Exception as e:
            logger.error(f"Error creating materialized view {view_name}: {e}")
    
    def refresh_materialized_view(self, view_name: str):
        """Refresh a materialized view."""
        refresh_query = f"REFRESH MATERIALIZED VIEW {view_name}"
        
        try:
            with self.engine.connect() as connection:
                connection.execute(text(refresh_query))
                connection.commit()
                logger.info(f"Materialized view {view_name} refreshed successfully")
        except Exception as e:
            logger.error(f"Error refreshing materialized view {view_name}: {e}")
    
    def partition_table(self, table_name: str, partition_column: str, partition_type: str = "RANGE"):
        """Create table partitions for better query performance."""
        # This is a simplified example - actual implementation would depend on the database
        partition_query = f"""
        CREATE TABLE {table_name}_partitioned (
            LIKE {table_name} INCLUDING ALL
        ) PARTITION BY {partition_type} ({partition_column});
        """
        
        try:
            with self.engine.connect() as connection:
                connection.execute(text(partition_query))
                connection.commit()
                logger.info(f"Partitioned table {table_name} created successfully")
        except Exception as e:
            logger.error(f"Error creating partitioned table {table_name}: {e}")
    
    def get_table_statistics(self, table_name: str) -> Dict[str, Any]:
        """Get table statistics for optimization insights."""
        stats_query = f"""
        SELECT 
            schemaname,
            tablename,
            attname,
            n_distinct,
            correlation,
            most_common_vals,
            most_common_freqs
        FROM pg_stats 
        WHERE tablename = '{table_name}'
        ORDER BY attname;
        """
        
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(stats_query))
                stats = result.fetchall()
                
                return {
                    "table_name": table_name,
                    "statistics": [
                        {
                            "column": row[2],
                            "distinct_values": row[3],
                            "correlation": row[4],
                            "most_common_values": row[5],
                            "most_common_frequencies": row[6]
                        }
                        for row in stats
                    ]
                }
        except Exception as e:
            logger.error(f"Error getting table statistics for {table_name}: {e}")
            return {}


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    optimizer = SQLOptimizer()
    
    try:
        # Create tables
        optimizer.create_tables()
        
        # Execute optimized queries
        print("=== User Activity Analysis ===")
        user_activity_df = optimizer.execute_optimized_query(optimizer.optimize_query_1_user_activity_analysis())
        print(user_activity_df.head())
        
        print("\n=== Session Analysis ===")
        session_df = optimizer.execute_optimized_query(optimizer.optimize_query_2_session_analysis())
        print(session_df.head())
        
        print("\n=== Real-time Metrics ===")
        metrics_df = optimizer.execute_optimized_query(optimizer.optimize_query_3_real_time_metrics())
        print(metrics_df.head())
        
        # Analyze query performance
        print("\n=== Query Performance Analysis ===")
        performance = optimizer.analyze_query_performance(optimizer.optimize_query_1_user_activity_analysis())
        print(f"Execution Time: {performance.get('execution_time', 0)}ms")
        print(f"Planning Time: {performance.get('planning_time', 0)}ms")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
