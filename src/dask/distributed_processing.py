"""Dask distributed computing for large-scale data processing."""

import logging
import time
from typing import Dict, Any, List, Optional, Callable
import dask
import dask.dataframe as dd
import dask.array as da
import dask.bag as db
import pandas as pd
import numpy as np
from dask.distributed import Client, LocalCluster, as_completed
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler
import dask.delayed as delayed
from datetime import datetime, timedelta
import json

from ..config import settings

logger = logging.getLogger(__name__)


class DaskDistributedProcessor:
    """Dask-based distributed data processing for large datasets."""
    
    def __init__(self, cluster_type: str = "local", n_workers: int = 4, threads_per_worker: int = 2):
        """Initialize Dask client and cluster."""
        self.cluster_type = cluster_type
        self.n_workers = n_workers
        self.threads_per_worker = threads_per_worker
        self.client = None
        self.cluster = None
        self._setup_cluster()
    
    def _setup_cluster(self):
        """Setup Dask cluster and client."""
        try:
            if self.cluster_type == "local":
                self.cluster = LocalCluster(
                    n_workers=self.n_workers,
                    threads_per_worker=self.threads_per_worker,
                    memory_limit='4GB',
                    processes=True
                )
            else:
                # For cloud clusters, you would configure here
                raise ValueError(f"Cluster type {self.cluster_type} not implemented")
            
            self.client = Client(self.cluster)
            logger.info(f"Dask cluster started with {self.n_workers} workers")
            
        except Exception as e:
            logger.error(f"Error setting up Dask cluster: {e}")
            raise
    
    def create_large_dataset(self, n_rows: int = 1000000, n_partitions: int = 10) -> dd.DataFrame:
        """Create a large synthetic dataset for testing."""
        logger.info(f"Creating dataset with {n_rows} rows and {n_partitions} partitions")
        
        def generate_partition(partition_id: int, rows_per_partition: int) -> pd.DataFrame:
            """Generate a single partition of data."""
            np.random.seed(partition_id)
            
            data = {
                'user_id': [f"user_{i:06d}" for i in range(partition_id * rows_per_partition, 
                                                           (partition_id + 1) * rows_per_partition)],
                'session_id': [f"session_{i:08d}" for i in range(partition_id * rows_per_partition,
                                                               (partition_id + 1) * rows_per_partition)],
                'event_type': np.random.choice(['page_view', 'click', 'purchase', 'video_play'], rows_per_partition),
                'timestamp': pd.date_range(start='2024-01-01', periods=rows_per_partition, freq='1min'),
                'value': np.random.exponential(10, rows_per_partition),
                'category': np.random.choice(['A', 'B', 'C', 'D'], rows_per_partition),
                'device_type': np.random.choice(['Desktop', 'Mobile', 'Tablet'], rows_per_partition),
                'country': np.random.choice(['US', 'UK', 'CA', 'DE', 'FR'], rows_per_partition)
            }
            
            return pd.DataFrame(data)
        
        # Create delayed tasks for each partition
        delayed_partitions = [
            delayed(generate_partition)(i, n_rows // n_partitions)
            for i in range(n_partitions)
        ]
        
        # Convert to Dask DataFrame
        ddf = dd.from_delayed(delayed_partitions)
        
        logger.info(f"Dataset created with {len(ddf)} partitions")
        return ddf
    
    def process_user_events_distributed(self, ddf: dd.DataFrame) -> dd.DataFrame:
        """Process user events using distributed computing."""
        logger.info("Starting distributed user event processing")
        
        # Add computed columns
        processed_ddf = ddf.assign(
            hour=ddf['timestamp'].dt.hour,
            day_of_week=ddf['timestamp'].dt.dayofweek,
            is_weekend=ddf['timestamp'].dt.dayofweek.isin([5, 6]),
            value_category=ddf['value'].apply(
                lambda x: 'low' if x < 5 else 'medium' if x < 15 else 'high',
                meta=('value_category', 'object')
            )
        )
        
        # Group by user and calculate metrics
        user_metrics = processed_ddf.groupby('user_id').agg({
            'value': ['sum', 'mean', 'std', 'count'],
            'event_type': 'nunique',
            'session_id': 'nunique',
            'timestamp': ['min', 'max']
        }).compute()
        
        # Flatten column names
        user_metrics.columns = ['_'.join(col).strip() for col in user_metrics.columns]
        
        logger.info(f"Processed {len(user_metrics)} unique users")
        return user_metrics
    
    def calculate_session_analytics(self, ddf: dd.DataFrame) -> dd.DataFrame:
        """Calculate session-level analytics using Dask."""
        logger.info("Calculating session analytics")
        
        # Session-level aggregations
        session_metrics = ddf.groupby('session_id').agg({
            'user_id': 'first',
            'timestamp': ['min', 'max', 'count'],
            'value': ['sum', 'mean'],
            'event_type': 'nunique',
            'category': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'Unknown',
            'device_type': 'first',
            'country': 'first'
        }).compute()
        
        # Flatten column names
        session_metrics.columns = ['_'.join(col).strip() for col in session_metrics.columns]
        
        # Calculate session duration
        session_metrics['session_duration_minutes'] = (
            session_metrics['timestamp_max'] - session_metrics['timestamp_min']
        ).dt.total_seconds() / 60
        
        # Rename columns for clarity
        session_metrics = session_metrics.rename(columns={
            'timestamp_count': 'events_per_session',
            'event_type_nunique': 'unique_event_types',
            'value_sum': 'total_value',
            'value_mean': 'avg_value'
        })
        
        logger.info(f"Calculated metrics for {len(session_metrics)} sessions")
        return session_metrics
    
    def time_series_analysis(self, ddf: dd.DataFrame) -> dd.DataFrame:
        """Perform time series analysis on the data."""
        logger.info("Performing time series analysis")
        
        # Set timestamp as index for time series operations
        ddf_indexed = ddf.set_index('timestamp')
        
        # Resample to hourly aggregates
        hourly_metrics = ddf_indexed.resample('1H').agg({
            'value': ['sum', 'mean', 'count'],
            'user_id': 'nunique',
            'session_id': 'nunique',
            'event_type': lambda x: x.value_counts().index[0] if len(x) > 0 else 'None'
        }).compute()
        
        # Flatten column names
        hourly_metrics.columns = ['_'.join(col).strip() for col in hourly_metrics.columns]
        
        # Calculate rolling averages
        hourly_metrics['value_sum_rolling_24h'] = hourly_metrics['value_sum'].rolling(
            window=24, min_periods=1
        ).mean()
        
        hourly_metrics['user_id_nunique_rolling_24h'] = hourly_metrics['user_id_nunique'].rolling(
            window=24, min_periods=1
        ).mean()
        
        logger.info(f"Time series analysis completed for {len(hourly_metrics)} hours")
        return hourly_metrics
    
    def detect_anomalies_distributed(self, ddf: dd.DataFrame) -> dd.DataFrame:
        """Detect anomalies using distributed computing."""
        logger.info("Detecting anomalies using distributed processing")
        
        # Calculate user-level statistics
        user_stats = ddf.groupby('user_id')['value'].agg(['mean', 'std', 'count']).compute()
        
        # Calculate global statistics
        global_mean = user_stats['mean'].mean()
        global_std = user_stats['std'].std()
        
        # Identify anomalous users (Z-score > 3)
        user_stats['z_score'] = (user_stats['mean'] - global_mean) / global_std
        user_stats['is_anomaly'] = abs(user_stats['z_score']) > 3
        
        # Get anomaly details
        anomalies = user_stats[user_stats['is_anomaly']].copy()
        
        logger.info(f"Detected {len(anomalies)} anomalous users")
        return anomalies
    
    def parallel_data_transformation(self, ddf: dd.DataFrame, transformations: List[Callable]) -> List[dd.DataFrame]:
        """Apply multiple transformations in parallel."""
        logger.info(f"Applying {len(transformations)} transformations in parallel")
        
        # Create delayed tasks for each transformation
        delayed_results = [
            delayed(transform)(ddf) for transform in transformations
        ]
        
        # Execute all transformations in parallel
        results = dask.compute(*delayed_results)
        
        logger.info("All transformations completed")
        return list(results)
    
    def optimize_dataframe_operations(self, ddf: dd.DataFrame) -> dd.DataFrame:
        """Optimize DataFrame operations for better performance."""
        logger.info("Optimizing DataFrame operations")
        
        # Repartition for better load balancing
        optimal_partitions = max(1, len(ddf) // 100000)  # ~100k rows per partition
        ddf_optimized = ddf.repartition(npartitions=optimal_partitions)
        
        # Persist frequently used data in memory
        ddf_optimized = ddf_optimized.persist()
        
        # Optimize column types
        ddf_optimized = ddf_optimized.astype({
            'user_id': 'category',
            'event_type': 'category',
            'category': 'category',
            'device_type': 'category',
            'country': 'category'
        })
        
        logger.info(f"DataFrame optimized with {ddf_optimized.npartitions} partitions")
        return ddf_optimized
    
    def benchmark_performance(self, ddf: dd.DataFrame, operations: List[str]) -> Dict[str, float]:
        """Benchmark performance of different operations."""
        logger.info("Benchmarking performance")
        
        results = {}
        
        for operation in operations:
            start_time = time.time()
            
            if operation == "groupby_user":
                result = ddf.groupby('user_id')['value'].sum().compute()
            elif operation == "groupby_session":
                result = ddf.groupby('session_id')['value'].mean().compute()
            elif operation == "filter_high_value":
                result = ddf[ddf['value'] > 20].compute()
            elif operation == "time_series_resample":
                ddf_indexed = ddf.set_index('timestamp')
                result = ddf_indexed.resample('1H')['value'].sum().compute()
            elif operation == "join_operations":
                # Create a second DataFrame for join
                user_info = ddf[['user_id', 'country']].drop_duplicates()
                result = ddf.merge(user_info, on='user_id', how='left').compute()
            
            end_time = time.time()
            results[operation] = end_time - start_time
            
            logger.info(f"{operation}: {results[operation]:.2f} seconds")
        
        return results
    
    def memory_usage_analysis(self, ddf: dd.DataFrame) -> Dict[str, Any]:
        """Analyze memory usage of the DataFrame."""
        logger.info("Analyzing memory usage")
        
        # Get memory usage per partition
        memory_usage = ddf.map_partitions(lambda x: x.memory_usage(deep=True).sum()).compute()
        
        analysis = {
            'total_memory_mb': memory_usage.sum() / 1024 / 1024,
            'avg_memory_per_partition_mb': memory_usage.mean() / 1024 / 1024,
            'max_memory_per_partition_mb': memory_usage.max() / 1024 / 1024,
            'min_memory_per_partition_mb': memory_usage.min() / 1024 / 1024,
            'memory_std_mb': memory_usage.std() / 1024 / 1024,
            'partition_count': len(memory_usage)
        }
        
        logger.info(f"Total memory usage: {analysis['total_memory_mb']:.2f} MB")
        return analysis
    
    def create_dashboard_data(self, ddf: dd.DataFrame) -> Dict[str, Any]:
        """Create data for dashboard visualization."""
        logger.info("Creating dashboard data")
        
        # Real-time metrics
        recent_data = ddf[ddf['timestamp'] >= datetime.now() - timedelta(hours=24)]
        
        dashboard_data = {
            'total_events': len(ddf),
            'unique_users': ddf['user_id'].nunique(),
            'unique_sessions': ddf['session_id'].nunique(),
            'events_last_24h': len(recent_data),
            'avg_value': ddf['value'].mean(),
            'top_event_types': ddf['event_type'].value_counts().head(5).to_dict(),
            'device_distribution': ddf['device_type'].value_counts().to_dict(),
            'country_distribution': ddf['country'].value_counts().head(10).to_dict(),
            'hourly_activity': ddf.groupby(ddf['timestamp'].dt.hour).size().to_dict()
        }
        
        # Compute all metrics
        for key, value in dashboard_data.items():
            if hasattr(value, 'compute'):
                dashboard_data[key] = value.compute()
        
        logger.info("Dashboard data created successfully")
        return dashboard_data
    
    def save_results(self, results: Dict[str, Any], output_path: str):
        """Save processing results to various formats."""
        logger.info(f"Saving results to {output_path}")
        
        for name, data in results.items():
            if isinstance(data, pd.DataFrame):
                # Save as Parquet for efficient storage
                data.to_parquet(f"{output_path}/{name}.parquet", index=False)
            elif isinstance(data, dict):
                # Save as JSON
                with open(f"{output_path}/{name}.json", 'w') as f:
                    json.dump(data, f, indent=2, default=str)
        
        logger.info("Results saved successfully")
    
    def close(self):
        """Close Dask client and cluster."""
        if self.client:
            self.client.close()
        if self.cluster:
            self.cluster.close()
        logger.info("Dask cluster closed")


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Initialize Dask processor
    processor = DaskDistributedProcessor(n_workers=4, threads_per_worker=2)
    
    try:
        # Create large dataset
        print("Creating large dataset...")
        ddf = processor.create_large_dataset(n_rows=1000000, n_partitions=20)
        
        # Optimize DataFrame
        print("Optimizing DataFrame...")
        ddf_optimized = processor.optimize_dataframe_operations(ddf)
        
        # Analyze memory usage
        print("Analyzing memory usage...")
        memory_analysis = processor.memory_usage_analysis(ddf_optimized)
        print(f"Memory usage: {memory_analysis}")
        
        # Process user events
        print("Processing user events...")
        user_metrics = processor.process_user_events_distributed(ddf_optimized)
        print(f"User metrics shape: {user_metrics.shape}")
        
        # Calculate session analytics
        print("Calculating session analytics...")
        session_metrics = processor.calculate_session_analytics(ddf_optimized)
        print(f"Session metrics shape: {session_metrics.shape}")
        
        # Time series analysis
        print("Performing time series analysis...")
        time_series = processor.time_series_analysis(ddf_optimized)
        print(f"Time series shape: {time_series.shape}")
        
        # Detect anomalies
        print("Detecting anomalies...")
        anomalies = processor.detect_anomalies_distributed(ddf_optimized)
        print(f"Anomalies detected: {len(anomalies)}")
        
        # Benchmark performance
        print("Benchmarking performance...")
        operations = ["groupby_user", "groupby_session", "filter_high_value"]
        benchmark_results = processor.benchmark_performance(ddf_optimized, operations)
        print(f"Benchmark results: {benchmark_results}")
        
        # Create dashboard data
        print("Creating dashboard data...")
        dashboard_data = processor.create_dashboard_data(ddf_optimized)
        print(f"Dashboard data keys: {list(dashboard_data.keys())}")
        
        # Save results
        print("Saving results...")
        results = {
            'user_metrics': user_metrics,
            'session_metrics': session_metrics,
            'time_series': time_series,
            'anomalies': anomalies,
            'dashboard_data': dashboard_data
        }
        processor.save_results(results, "/tmp/dask_results")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        processor.close()
