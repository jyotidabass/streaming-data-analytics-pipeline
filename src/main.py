"""Main application entry point for the streaming analytics pipeline."""

import logging
import asyncio
import signal
import sys
from typing import Optional
from datetime import datetime

from .config import settings
from .kafka import StreamingDataProducer, StreamingDataConsumer
from .spark import SparkPipeline
from .dask import DaskDistributedProcessor
from .sql import SQLOptimizer
from .cloud import AWSCloudIntegration, GCPCloudIntegration

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('streaming_analytics.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class StreamingAnalyticsPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self):
        """Initialize the streaming analytics pipeline."""
        self.producer = None
        self.consumer = None
        self.spark_pipeline = None
        self.dask_processor = None
        self.sql_optimizer = None
        self.aws_integration = None
        self.gcp_integration = None
        self.running = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def initialize_components(self):
        """Initialize all pipeline components."""
        try:
            logger.info("Initializing streaming analytics pipeline components...")
            
            # Initialize Kafka components
            self.producer = StreamingDataProducer()
            self.consumer = StreamingDataConsumer()
            
            # Initialize processing components
            self.spark_pipeline = SparkPipeline()
            self.dask_processor = DaskDistributedProcessor()
            self.sql_optimizer = SQLOptimizer()
            
            # Initialize cloud integrations
            if settings.aws_access_key_id:
                self.aws_integration = AWSCloudIntegration()
                logger.info("AWS integration initialized")
            
            if settings.gcp_project_id:
                self.gcp_integration = GCPCloudIntegration()
                logger.info("GCP integration initialized")
            
            # Create database tables
            self.sql_optimizer.create_tables()
            
            logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing components: {e}")
            return False
    
    def start_streaming_simulation(self, duration_minutes: int = 60, events_per_second: int = 10):
        """Start streaming data simulation."""
        try:
            logger.info(f"Starting streaming simulation for {duration_minutes} minutes")
            
            # Start producer in background
            import threading
            producer_thread = threading.Thread(
                target=self.producer.start_streaming_simulation,
                args=(duration_minutes, events_per_second)
            )
            producer_thread.daemon = True
            producer_thread.start()
            
            # Start consumer
            topics = [settings.kafka_topic_user_events, settings.kafka_topic_analytics]
            self.consumer.start_consuming(topics)
            
        except Exception as e:
            logger.error(f"Error in streaming simulation: {e}")
    
    def run_batch_processing(self):
        """Run batch data processing."""
        try:
            logger.info("Starting batch data processing...")
            
            # Create large dataset
            ddf = self.dask_processor.create_large_dataset(n_rows=1000000, n_partitions=20)
            
            # Optimize DataFrame
            ddf_optimized = self.dask_processor.optimize_dataframe_operations(ddf)
            
            # Process user events
            user_metrics = self.dask_processor.process_user_events_distributed(ddf_optimized)
            session_metrics = self.dask_processor.calculate_session_analytics(ddf_optimized)
            time_series = self.dask_processor.time_series_analysis(ddf_optimized)
            anomalies = self.dask_processor.detect_anomalies_distributed(ddf_optimized)
            
            # Save results
            results = {
                'user_metrics': user_metrics,
                'session_metrics': session_metrics,
                'time_series': time_series,
                'anomalies': anomalies
            }
            
            # Save to local storage
            self.dask_processor.save_results(results, "/tmp/batch_results")
            
            # Upload to cloud storage if available
            if self.aws_integration:
                for name, data in results.items():
                    if hasattr(data, 'to_json'):
                        self.aws_integration.upload_to_s3(
                            data.to_json(), 
                            settings.s3_bucket, 
                            f"batch_results/{name}.json"
                        )
            
            if self.gcp_integration:
                for name, data in results.items():
                    if hasattr(data, 'to_json'):
                        self.gcp_integration.upload_to_gcs(
                            data.to_json(), 
                            settings.gcs_bucket, 
                            f"batch_results/{name}.json"
                        )
            
            logger.info("Batch processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
    
    def run_sql_optimization_demo(self):
        """Run SQL optimization demonstrations."""
        try:
            logger.info("Running SQL optimization demonstrations...")
            
            # Execute optimized queries
            queries = [
                ("User Activity Analysis", self.sql_optimizer.optimize_query_1_user_activity_analysis()),
                ("Session Analysis", self.sql_optimizer.optimize_query_2_session_analysis()),
                ("Real-time Metrics", self.sql_optimizer.optimize_query_3_real_time_metrics()),
                ("Cohort Analysis", self.sql_optimizer.optimize_query_4_cohort_analysis()),
                ("Funnel Analysis", self.sql_optimizer.optimize_query_5_funnel_analysis())
            ]
            
            for query_name, query in queries:
                logger.info(f"Executing {query_name}...")
                result_df = self.sql_optimizer.execute_optimized_query(query)
                
                # Analyze performance
                performance = self.sql_optimizer.analyze_query_performance(query)
                
                logger.info(f"{query_name} completed: {len(result_df)} rows, "
                          f"execution time: {performance.get('execution_time', 0)}ms")
            
            logger.info("SQL optimization demonstrations completed")
            
        except Exception as e:
            logger.error(f"Error in SQL optimization demo: {e}")
    
    def run_spark_processing(self):
        """Run Spark data processing."""
        try:
            logger.info("Starting Spark data processing...")
            
            # Read streaming data from Kafka
            kafka_df = self.spark_pipeline.read_kafka_stream(settings.kafka_topic_user_events)
            parsed_df = self.spark_pipeline.parse_json_stream(kafka_df, self.spark_pipeline.user_event_schema)
            
            # Process data
            processed_df = self.spark_pipeline.process_user_events(parsed_df)
            session_metrics = self.spark_pipeline.calculate_session_metrics(processed_df)
            real_time_metrics = self.spark_pipeline.calculate_real_time_metrics(session_metrics)
            anomaly_detection = self.spark_pipeline.detect_anomalies(real_time_metrics)
            optimized_df = self.spark_pipeline.optimize_sql_queries(anomaly_detection)
            
            # Write results
            self.spark_pipeline.write_to_database(optimized_df, "processed_events")
            
            if self.aws_integration:
                self.spark_pipeline.write_to_s3(optimized_df, "spark_results/processed_events")
            
            if self.gcp_integration:
                self.spark_pipeline.write_to_gcs(optimized_df, "spark_results/processed_events")
            
            logger.info("Spark processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error in Spark processing: {e}")
    
    def monitor_pipeline_health(self):
        """Monitor pipeline health and performance."""
        try:
            logger.info("Monitoring pipeline health...")
            
            health_status = {
                "timestamp": datetime.utcnow().isoformat(),
                "components": {}
            }
            
            # Check Kafka health
            if self.producer and self.consumer:
                health_status["components"]["kafka"] = "healthy"
            else:
                health_status["components"]["kafka"] = "unavailable"
            
            # Check Spark health
            if self.spark_pipeline:
                health_status["components"]["spark"] = "healthy"
            else:
                health_status["components"]["spark"] = "unavailable"
            
            # Check Dask health
            if self.dask_processor:
                health_status["components"]["dask"] = "healthy"
            else:
                health_status["components"]["dask"] = "unavailable"
            
            # Check SQL optimizer health
            if self.sql_optimizer:
                health_status["components"]["sql"] = "healthy"
            else:
                health_status["components"]["sql"] = "unavailable"
            
            # Check cloud integrations
            if self.aws_integration:
                health_status["components"]["aws"] = "healthy"
            else:
                health_status["components"]["aws"] = "unavailable"
            
            if self.gcp_integration:
                health_status["components"]["gcp"] = "healthy"
            else:
                health_status["components"]["gcp"] = "unavailable"
            
            # Determine overall health
            healthy_components = sum(1 for status in health_status["components"].values() if status == "healthy")
            total_components = len(health_status["components"])
            
            if healthy_components == total_components:
                health_status["overall_status"] = "healthy"
            elif healthy_components > total_components // 2:
                health_status["overall_status"] = "degraded"
            else:
                health_status["overall_status"] = "unhealthy"
            
            logger.info(f"Pipeline health: {health_status['overall_status']} "
                      f"({healthy_components}/{total_components} components healthy)")
            
            return health_status
            
        except Exception as e:
            logger.error(f"Error monitoring pipeline health: {e}")
            return {"overall_status": "error", "error": str(e)}
    
    def start(self, mode: str = "streaming"):
        """Start the pipeline in specified mode."""
        try:
            if not self.initialize_components():
                logger.error("Failed to initialize components")
                return False
            
            self.running = True
            logger.info(f"Starting pipeline in {mode} mode...")
            
            if mode == "streaming":
                self.start_streaming_simulation()
            elif mode == "batch":
                self.run_batch_processing()
            elif mode == "sql_demo":
                self.run_sql_optimization_demo()
            elif mode == "spark":
                self.run_spark_processing()
            elif mode == "monitor":
                while self.running:
                    health = self.monitor_pipeline_health()
                    logger.info(f"Health check: {health['overall_status']}")
                    import time
                    time.sleep(60)  # Check every minute
            else:
                logger.error(f"Unknown mode: {mode}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error starting pipeline: {e}")
            return False
    
    def stop(self):
        """Stop the pipeline and cleanup resources."""
        try:
            logger.info("Stopping pipeline...")
            self.running = False
            
            # Stop consumer
            if self.consumer:
                self.consumer.stop()
            
            # Close producer
            if self.producer:
                self.producer.close()
            
            # Stop Spark
            if self.spark_pipeline:
                self.spark_pipeline.stop()
            
            # Close Dask
            if self.dask_processor:
                self.dask_processor.close()
            
            logger.info("Pipeline stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping pipeline: {e}")


def main():
    """Main function to run the streaming analytics pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Streaming Analytics Pipeline")
    parser.add_argument(
        "--mode", 
        choices=["streaming", "batch", "sql_demo", "spark", "monitor"],
        default="streaming",
        help="Pipeline execution mode"
    )
    parser.add_argument(
        "--duration", 
        type=int, 
        default=60,
        help="Duration in minutes for streaming mode"
    )
    parser.add_argument(
        "--events-per-second", 
        type=int, 
        default=10,
        help="Events per second for streaming mode"
    )
    
    args = parser.parse_args()
    
    # Create and start pipeline
    pipeline = StreamingAnalyticsPipeline()
    
    try:
        if args.mode == "streaming":
            pipeline.start_streaming_simulation(args.duration, args.events_per_second)
        else:
            pipeline.start(args.mode)
        
        # Keep running until interrupted
        while pipeline.running:
            import time
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
