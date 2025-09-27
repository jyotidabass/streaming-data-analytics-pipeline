"""Google Cloud Platform integration for the streaming analytics pipeline."""

import logging
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import storage, bigquery, pubsub_v1, monitoring_v3
from google.cloud.exceptions import NotFound, GoogleCloudError
import google.auth

from ..config import settings

logger = logging.getLogger(__name__)


class GCPCloudIntegration:
    """Google Cloud Platform services integration for data processing and storage."""
    
    def __init__(self):
        """Initialize GCP clients."""
        try:
            # Initialize Cloud Storage client
            self.storage_client = storage.Client()
            
            # Initialize BigQuery client
            self.bigquery_client = bigquery.Client()
            
            # Initialize Pub/Sub client
            self.pubsub_publisher = pubsub_v1.PublisherClient()
            self.pubsub_subscriber = pubsub_v1.SubscriberClient()
            
            # Initialize Cloud Monitoring client
            self.monitoring_client = monitoring_v3.MetricServiceClient()
            
            logger.info("GCP clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing GCP clients: {e}")
            raise
    
    def upload_to_gcs(self, data: Any, bucket_name: str, blob_name: str, 
                     format_type: str = "json") -> bool:
        """Upload data to Google Cloud Storage."""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            if format_type == "json":
                if isinstance(data, dict):
                    data_str = json.dumps(data, default=str)
                else:
                    data_str = str(data)
                blob.upload_from_string(data_str, content_type='application/json')
                
            elif format_type == "csv":
                if isinstance(data, pd.DataFrame):
                    csv_data = data.to_csv(index=False)
                    blob.upload_from_string(csv_data, content_type='text/csv')
                else:
                    blob.upload_from_string(str(data), content_type='text/csv')
                    
            elif format_type == "parquet":
                if isinstance(data, pd.DataFrame):
                    parquet_data = data.to_parquet(index=False)
                    blob.upload_from_string(parquet_data, content_type='application/octet-stream')
            
            logger.info(f"Data uploaded to gs://{bucket_name}/{blob_name}")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"Error uploading to GCS: {e}")
            return False
    
    def download_from_gcs(self, bucket_name: str, blob_name: str, 
                         format_type: str = "json") -> Optional[Any]:
        """Download data from Google Cloud Storage."""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            if format_type == "json":
                data = blob.download_as_text()
                return json.loads(data)
            elif format_type == "csv":
                data = blob.download_as_text()
                return pd.read_csv(pd.StringIO(data))
            elif format_type == "parquet":
                data = blob.download_as_bytes()
                return pd.read_parquet(pd.BytesIO(data))
            else:
                return blob.download_as_text()
                
        except NotFound:
            logger.error(f"Blob gs://{bucket_name}/{blob_name} not found")
            return None
        except GoogleCloudError as e:
            logger.error(f"Error downloading from GCS: {e}")
            return None
    
    def list_gcs_objects(self, bucket_name: str, prefix: str = "") -> List[Dict[str, Any]]:
        """List objects in GCS bucket with optional prefix."""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            
            objects = []
            for blob in blobs:
                objects.append({
                    'name': blob.name,
                    'size': blob.size,
                    'created': blob.time_created,
                    'updated': blob.updated,
                    'content_type': blob.content_type
                })
            
            return objects
            
        except GoogleCloudError as e:
            logger.error(f"Error listing GCS objects: {e}")
            return []
    
    def create_gcs_bucket(self, bucket_name: str, location: str = "US") -> bool:
        """Create GCS bucket."""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            bucket.location = location
            bucket.create()
            
            logger.info(f"GCS bucket {bucket_name} created successfully")
            return True
            
        except GoogleCloudError as e:
            if "already exists" in str(e):
                logger.info(f"GCS bucket {bucket_name} already exists")
                return True
            else:
                logger.error(f"Error creating GCS bucket: {e}")
                return False
    
    def delete_gcs_object(self, bucket_name: str, blob_name: str) -> bool:
        """Delete object from GCS bucket."""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
            
            logger.info(f"Object gs://{bucket_name}/{blob_name} deleted successfully")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"Error deleting GCS object: {e}")
            return False
    
    def create_bigquery_dataset(self, dataset_id: str, location: str = "US") -> bool:
        """Create BigQuery dataset."""
        try:
            dataset = bigquery.Dataset(f"{settings.gcp_project_id}.{dataset_id}")
            dataset.location = location
            dataset = self.bigquery_client.create_dataset(dataset, timeout=30)
            
            logger.info(f"BigQuery dataset {dataset_id} created successfully")
            return True
            
        except GoogleCloudError as e:
            if "already exists" in str(e):
                logger.info(f"BigQuery dataset {dataset_id} already exists")
                return True
            else:
                logger.error(f"Error creating BigQuery dataset: {e}")
                return False
    
    def create_bigquery_table(self, dataset_id: str, table_id: str, 
                             schema: List[Dict[str, str]]) -> bool:
        """Create BigQuery table with schema."""
        try:
            table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
            
            # Convert schema to BigQuery format
            bigquery_schema = []
            for field in schema:
                bigquery_schema.append(
                    bigquery.SchemaField(field['name'], field['type'], field.get('mode', 'NULLABLE'))
                )
            
            table = bigquery.Table(table_ref, schema=bigquery_schema)
            table = self.bigquery_client.create_table(table)
            
            logger.info(f"BigQuery table {dataset_id}.{table_id} created successfully")
            return True
            
        except GoogleCloudError as e:
            if "already exists" in str(e):
                logger.info(f"BigQuery table {dataset_id}.{table_id} already exists")
                return True
            else:
                logger.error(f"Error creating BigQuery table: {e}")
                return False
    
    def load_data_to_bigquery(self, dataset_id: str, table_id: str, 
                             data: pd.DataFrame) -> bool:
        """Load data to BigQuery table."""
        try:
            table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True
            )
            
            job = self.bigquery_client.load_table_from_dataframe(
                data, table_ref, job_config=job_config
            )
            job.result()  # Wait for job to complete
            
            logger.info(f"Data loaded to BigQuery table {dataset_id}.{table_id}")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"Error loading data to BigQuery: {e}")
            return False
    
    def query_bigquery(self, query: str) -> Optional[pd.DataFrame]:
        """Execute query on BigQuery and return results as DataFrame."""
        try:
            query_job = self.bigquery_client.query(query)
            results = query_job.result()
            
            df = results.to_dataframe()
            logger.info(f"BigQuery query executed successfully, returned {len(df)} rows")
            return df
            
        except GoogleCloudError as e:
            logger.error(f"Error executing BigQuery query: {e}")
            return None
    
    def create_pubsub_topic(self, topic_name: str) -> bool:
        """Create Pub/Sub topic."""
        try:
            topic_path = self.pubsub_publisher.topic_path(settings.gcp_project_id, topic_name)
            topic = self.pubsub_publisher.create_topic(request={"name": topic_path})
            
            logger.info(f"Pub/Sub topic {topic_name} created successfully")
            return True
            
        except GoogleCloudError as e:
            if "already exists" in str(e):
                logger.info(f"Pub/Sub topic {topic_name} already exists")
                return True
            else:
                logger.error(f"Error creating Pub/Sub topic: {e}")
                return False
    
    def create_pubsub_subscription(self, topic_name: str, subscription_name: str) -> bool:
        """Create Pub/Sub subscription."""
        try:
            topic_path = self.pubsub_publisher.topic_path(settings.gcp_project_id, topic_name)
            subscription_path = self.pubsub_subscriber.subscription_path(
                settings.gcp_project_id, subscription_name
            )
            
            subscription = self.pubsub_subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
            
            logger.info(f"Pub/Sub subscription {subscription_name} created successfully")
            return True
            
        except GoogleCloudError as e:
            if "already exists" in str(e):
                logger.info(f"Pub/Sub subscription {subscription_name} already exists")
                return True
            else:
                logger.error(f"Error creating Pub/Sub subscription: {e}")
                return False
    
    def publish_message(self, topic_name: str, message_data: Dict[str, Any]) -> bool:
        """Publish message to Pub/Sub topic."""
        try:
            topic_path = self.pubsub_publisher.topic_path(settings.gcp_project_id, topic_name)
            
            message_json = json.dumps(message_data, default=str)
            message_bytes = message_json.encode('utf-8')
            
            future = self.pubsub_publisher.publish(topic_path, message_bytes)
            message_id = future.result()
            
            logger.info(f"Message published to topic {topic_name}: {message_id}")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"Error publishing message: {e}")
            return False
    
    def pull_messages(self, subscription_name: str, max_messages: int = 10) -> List[Dict[str, Any]]:
        """Pull messages from Pub/Sub subscription."""
        try:
            subscription_path = self.pubsub_subscriber.subscription_path(
                settings.gcp_project_id, subscription_name
            )
            
            response = self.pubsub_subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": max_messages,
                }
            )
            
            messages = []
            for received_message in response.received_messages:
                message_data = json.loads(received_message.message.data.decode('utf-8'))
                messages.append({
                    'message_id': received_message.message.message_id,
                    'data': message_data,
                    'ack_id': received_message.ack_id
                })
            
            return messages
            
        except GoogleCloudError as e:
            logger.error(f"Error pulling messages: {e}")
            return []
    
    def acknowledge_message(self, subscription_name: str, ack_id: str) -> bool:
        """Acknowledge message in Pub/Sub subscription."""
        try:
            subscription_path = self.pubsub_subscriber.subscription_path(
                settings.gcp_project_id, subscription_name
            )
            
            self.pubsub_subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": [ack_id]}
            )
            
            logger.info("Message acknowledged successfully")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"Error acknowledging message: {e}")
            return False
    
    def create_monitoring_metric(self, metric_type: str, metric_name: str, 
                               value: float, labels: Dict[str, str] = None) -> bool:
        """Create custom metric in Cloud Monitoring."""
        try:
            project_name = f"projects/{settings.gcp_project_id}"
            
            series = monitoring_v3.TimeSeries()
            series.metric.type = f"custom.googleapis.com/{metric_type}/{metric_name}"
            series.resource.type = "global"
            
            if labels:
                for key, value in labels.items():
                    series.metric.labels[key] = value
            
            now = datetime.utcnow()
            seconds = int(now.timestamp())
            nanos = int((now.timestamp() - seconds) * 10**9)
            
            interval = monitoring_v3.TimeInterval({
                "end_time": {"seconds": seconds, "nanos": nanos}
            })
            
            point = monitoring_v3.Point({
                "interval": interval,
                "value": {"double_value": value}
            })
            
            series.points = [point]
            
            self.monitoring_client.create_time_series(
                name=project_name,
                time_series=[series]
            )
            
            logger.info(f"Metric {metric_name} created in Cloud Monitoring")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"Error creating monitoring metric: {e}")
            return False
    
    def get_monitoring_metrics(self, metric_type: str, metric_name: str,
                              start_time: datetime, end_time: datetime,
                              labels: Dict[str, str] = None) -> List[Dict[str, Any]]:
        """Get metrics from Cloud Monitoring."""
        try:
            project_name = f"projects/{settings.gcp_project_id}"
            filter_str = f'metric.type="custom.googleapis.com/{metric_type}/{metric_name}"'
            
            if labels:
                for key, value in labels.items():
                    filter_str += f' AND metric.labels.{key}="{value}"'
            
            interval = monitoring_v3.TimeInterval({
                "end_time": {
                    "seconds": int(end_time.timestamp()),
                    "nanos": int((end_time.timestamp() - int(end_time.timestamp())) * 10**9)
                },
                "start_time": {
                    "seconds": int(start_time.timestamp()),
                    "nanos": int((start_time.timestamp() - int(start_time.timestamp())) * 10**9)
                }
            })
            
            aggregation = monitoring_v3.Aggregation({
                "alignment_period": {"seconds": 300},  # 5 minutes
                "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
                "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_MEAN
            })
            
            request = monitoring_v3.ListTimeSeriesRequest({
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "aggregation": aggregation
            })
            
            response = self.monitoring_client.list_time_series(request=request)
            
            metrics = []
            for series in response:
                for point in series.points:
                    metrics.append({
                        'timestamp': datetime.fromtimestamp(
                            point.interval.end_time.seconds + point.interval.end_time.nanos / 10**9
                        ),
                        'value': point.value.double_value,
                        'labels': dict(series.metric.labels)
                    })
            
            return metrics
            
        except GoogleCloudError as e:
            logger.error(f"Error getting monitoring metrics: {e}")
            return []
    
    def create_data_pipeline(self, pipeline_name: str, config: Dict[str, Any]) -> bool:
        """Create data processing pipeline in GCP."""
        try:
            pipeline_config = {
                'name': pipeline_name,
                'config': config,
                'created_at': datetime.utcnow().isoformat(),
                'status': 'active'
            }
            
            # Store pipeline configuration in GCS
            bucket_name = settings.gcs_bucket or "streaming-analytics-pipelines"
            blob_name = f"pipelines/{pipeline_name}/config.json"
            
            success = self.upload_to_gcs(pipeline_config, bucket_name, blob_name)
            
            if success:
                logger.info(f"Data pipeline {pipeline_name} created successfully")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error creating data pipeline: {e}")
            return False
    
    def monitor_pipeline_health(self, pipeline_name: str) -> Dict[str, Any]:
        """Monitor pipeline health and performance."""
        try:
            health_metrics = {
                'pipeline_name': pipeline_name,
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'healthy',
                'metrics': {}
            }
            
            # Check GCS bucket health
            if settings.gcs_bucket:
                objects = self.list_gcs_objects(settings.gcs_bucket, f"pipelines/{pipeline_name}/")
                health_metrics['metrics']['gcs_objects_count'] = len(objects)
            
            # Get Cloud Monitoring metrics
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)
            
            monitoring_metrics = self.get_monitoring_metrics(
                'pipeline',
                'health',
                start_time,
                end_time,
                {'pipeline_name': pipeline_name}
            )
            
            if monitoring_metrics:
                latest_metric = max(monitoring_metrics, key=lambda x: x['timestamp'])
                health_metrics['metrics']['monitoring_health'] = latest_metric['value']
            
            return health_metrics
            
        except Exception as e:
            logger.error(f"Error monitoring pipeline health: {e}")
            return {
                'pipeline_name': pipeline_name,
                'status': 'error',
                'error': str(e)
            }


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    gcp_integration = GCPCloudIntegration()
    
    try:
        # Test GCS operations
        test_data = {"test": "data", "timestamp": datetime.utcnow().isoformat()}
        success = gcp_integration.upload_to_gcs(test_data, "test-bucket", "test-key.json")
        print(f"GCS upload success: {success}")
        
        # Test BigQuery operations
        success = gcp_integration.create_bigquery_dataset("test_dataset")
        print(f"BigQuery dataset creation success: {success}")
        
        # Test Pub/Sub operations
        success = gcp_integration.create_pubsub_topic("test-topic")
        print(f"Pub/Sub topic creation success: {success}")
        
        # Test monitoring
        success = gcp_integration.create_monitoring_metric(
            "pipeline",
            "test_metric",
            100.0,
            {"environment": "test"}
        )
        print(f"Monitoring metric creation success: {success}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
