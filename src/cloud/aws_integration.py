"""AWS cloud integration for the streaming analytics pipeline."""

import logging
import boto3
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd
from io import StringIO, BytesIO

from ..config import settings

logger = logging.getLogger(__name__)


class AWSCloudIntegration:
    """AWS cloud services integration for data processing and storage."""
    
    def __init__(self):
        """Initialize AWS clients."""
        try:
            # Initialize S3 client
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name=settings.aws_region
            )
            
            # Initialize other AWS services
            self.s3_resource = boto3.resource(
                's3',
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name=settings.aws_region
            )
            
            self.cloudwatch = boto3.client(
                'cloudwatch',
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name=settings.aws_region
            )
            
            self.sqs = boto3.client(
                'sqs',
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name=settings.aws_region
            )
            
            self.lambda_client = boto3.client(
                'lambda',
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name=settings.aws_region
            )
            
            logger.info("AWS clients initialized successfully")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except Exception as e:
            logger.error(f"Error initializing AWS clients: {e}")
            raise
    
    def upload_to_s3(self, data: Any, bucket: str, key: str, format_type: str = "json") -> bool:
        """Upload data to S3 bucket."""
        try:
            if format_type == "json":
                if isinstance(data, dict):
                    data_str = json.dumps(data, default=str)
                else:
                    data_str = str(data)
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=data_str,
                    ContentType='application/json'
                )
            elif format_type == "csv":
                if isinstance(data, pd.DataFrame):
                    csv_buffer = StringIO()
                    data.to_csv(csv_buffer, index=False)
                    self.s3_client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=csv_buffer.getvalue(),
                        ContentType='text/csv'
                    )
                else:
                    self.s3_client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=str(data),
                        ContentType='text/csv'
                    )
            elif format_type == "parquet":
                if isinstance(data, pd.DataFrame):
                    parquet_buffer = BytesIO()
                    data.to_parquet(parquet_buffer, index=False)
                    self.s3_client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=parquet_buffer.getvalue(),
                        ContentType='application/octet-stream'
                    )
            
            logger.info(f"Data uploaded to s3://{bucket}/{key}")
            return True
            
        except ClientError as e:
            logger.error(f"Error uploading to S3: {e}")
            return False
    
    def download_from_s3(self, bucket: str, key: str, format_type: str = "json") -> Optional[Any]:
        """Download data from S3 bucket."""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read()
            
            if format_type == "json":
                return json.loads(data.decode('utf-8'))
            elif format_type == "csv":
                return pd.read_csv(StringIO(data.decode('utf-8')))
            elif format_type == "parquet":
                return pd.read_parquet(BytesIO(data))
            else:
                return data.decode('utf-8')
                
        except ClientError as e:
            logger.error(f"Error downloading from S3: {e}")
            return None
    
    def list_s3_objects(self, bucket: str, prefix: str = "") -> List[Dict[str, Any]]:
        """List objects in S3 bucket with optional prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            
            objects = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    objects.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag']
                    })
            
            return objects
            
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            return []
    
    def create_s3_bucket(self, bucket_name: str, region: str = None) -> bool:
        """Create S3 bucket."""
        try:
            if region:
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            else:
                self.s3_client.create_bucket(Bucket=bucket_name)
            
            logger.info(f"S3 bucket {bucket_name} created successfully")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyExists':
                logger.info(f"S3 bucket {bucket_name} already exists")
                return True
            else:
                logger.error(f"Error creating S3 bucket: {e}")
                return False
    
    def delete_s3_object(self, bucket: str, key: str) -> bool:
        """Delete object from S3 bucket."""
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key)
            logger.info(f"Object s3://{bucket}/{key} deleted successfully")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting S3 object: {e}")
            return False
    
    def send_cloudwatch_metric(self, namespace: str, metric_name: str, value: float, 
                              dimensions: Dict[str, str] = None, unit: str = "Count") -> bool:
        """Send custom metric to CloudWatch."""
        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }
            
            if dimensions:
                metric_data['Dimensions'] = [
                    {'Name': k, 'Value': v} for k, v in dimensions.items()
                ]
            
            self.cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=[metric_data]
            )
            
            logger.info(f"Metric {metric_name} sent to CloudWatch")
            return True
            
        except ClientError as e:
            logger.error(f"Error sending CloudWatch metric: {e}")
            return False
    
    def get_cloudwatch_metrics(self, namespace: str, metric_name: str, 
                              start_time: datetime, end_time: datetime,
                              dimensions: Dict[str, str] = None) -> List[Dict[str, Any]]:
        """Get metrics from CloudWatch."""
        try:
            params = {
                'Namespace': namespace,
                'MetricName': metric_name,
                'StartTime': start_time,
                'EndTime': end_time,
                'Period': 300,  # 5 minutes
                'Statistics': ['Average', 'Sum', 'Maximum', 'Minimum']
            }
            
            if dimensions:
                params['Dimensions'] = [
                    {'Name': k, 'Value': v} for k, v in dimensions.items()
                ]
            
            response = self.cloudwatch.get_metric_statistics(**params)
            
            return response.get('Datapoints', [])
            
        except ClientError as e:
            logger.error(f"Error getting CloudWatch metrics: {e}")
            return []
    
    def send_sqs_message(self, queue_url: str, message_body: str, 
                        message_attributes: Dict[str, str] = None) -> bool:
        """Send message to SQS queue."""
        try:
            params = {
                'QueueUrl': queue_url,
                'MessageBody': message_body
            }
            
            if message_attributes:
                params['MessageAttributes'] = {
                    k: {'StringValue': v, 'DataType': 'String'}
                    for k, v in message_attributes.items()
                }
            
            response = self.sqs.send_message(**params)
            logger.info(f"Message sent to SQS queue: {response['MessageId']}")
            return True
            
        except ClientError as e:
            logger.error(f"Error sending SQS message: {e}")
            return False
    
    def receive_sqs_messages(self, queue_url: str, max_messages: int = 10) -> List[Dict[str, Any]]:
        """Receive messages from SQS queue."""
        try:
            response = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=20
            )
            
            messages = []
            if 'Messages' in response:
                for message in response['Messages']:
                    messages.append({
                        'message_id': message['MessageId'],
                        'receipt_handle': message['ReceiptHandle'],
                        'body': message['Body'],
                        'attributes': message.get('MessageAttributes', {})
                    })
            
            return messages
            
        except ClientError as e:
            logger.error(f"Error receiving SQS messages: {e}")
            return []
    
    def delete_sqs_message(self, queue_url: str, receipt_handle: str) -> bool:
        """Delete message from SQS queue."""
        try:
            self.sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info("SQS message deleted successfully")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting SQS message: {e}")
            return False
    
    def invoke_lambda_function(self, function_name: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Invoke AWS Lambda function."""
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            
            result = json.loads(response['Payload'].read())
            logger.info(f"Lambda function {function_name} invoked successfully")
            return result
            
        except ClientError as e:
            logger.error(f"Error invoking Lambda function: {e}")
            return None
    
    def create_data_pipeline(self, pipeline_name: str, config: Dict[str, Any]) -> bool:
        """Create data processing pipeline in AWS."""
        try:
            # This is a simplified example - actual implementation would use AWS Data Pipeline or Step Functions
            pipeline_config = {
                'name': pipeline_name,
                'config': config,
                'created_at': datetime.utcnow().isoformat(),
                'status': 'active'
            }
            
            # Store pipeline configuration in S3
            bucket = settings.s3_bucket or "streaming-analytics-pipelines"
            key = f"pipelines/{pipeline_name}/config.json"
            
            success = self.upload_to_s3(pipeline_config, bucket, key)
            
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
            
            # Check S3 bucket health
            if settings.s3_bucket:
                objects = self.list_s3_objects(settings.s3_bucket, f"pipelines/{pipeline_name}/")
                health_metrics['metrics']['s3_objects_count'] = len(objects)
            
            # Get CloudWatch metrics
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)
            
            cloudwatch_metrics = self.get_cloudwatch_metrics(
                'StreamingAnalytics',
                'PipelineHealth',
                start_time,
                end_time,
                {'PipelineName': pipeline_name}
            )
            
            if cloudwatch_metrics:
                latest_metric = max(cloudwatch_metrics, key=lambda x: x['Timestamp'])
                health_metrics['metrics']['cloudwatch_health'] = latest_metric['Average']
            
            return health_metrics
            
        except Exception as e:
            logger.error(f"Error monitoring pipeline health: {e}")
            return {
                'pipeline_name': pipeline_name,
                'status': 'error',
                'error': str(e)
            }
    
    def backup_data(self, source_bucket: str, destination_bucket: str, 
                   prefix: str = "", retention_days: int = 30) -> bool:
        """Backup data from one S3 bucket to another."""
        try:
            objects = self.list_s3_objects(source_bucket, prefix)
            backed_up_count = 0
            
            for obj in objects:
                # Copy object to destination bucket
                copy_source = {'Bucket': source_bucket, 'Key': obj['key']}
                destination_key = f"backup/{datetime.utcnow().strftime('%Y/%m/%d')}/{obj['key']}"
                
                self.s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=destination_bucket,
                    Key=destination_key
                )
                
                backed_up_count += 1
            
            logger.info(f"Backed up {backed_up_count} objects to {destination_bucket}")
            return True
            
        except Exception as e:
            logger.error(f"Error backing up data: {e}")
            return False
    
    def cleanup_old_data(self, bucket: str, prefix: str, days_old: int = 30) -> int:
        """Clean up old data from S3 bucket."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            objects = self.list_s3_objects(bucket, prefix)
            
            deleted_count = 0
            for obj in objects:
                if obj['last_modified'].replace(tzinfo=None) < cutoff_date:
                    if self.delete_s3_object(bucket, obj['key']):
                        deleted_count += 1
            
            logger.info(f"Cleaned up {deleted_count} old objects from {bucket}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            return 0


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    aws_integration = AWSCloudIntegration()
    
    try:
        # Test S3 operations
        test_data = {"test": "data", "timestamp": datetime.utcnow().isoformat()}
        success = aws_integration.upload_to_s3(test_data, "test-bucket", "test-key.json")
        print(f"S3 upload success: {success}")
        
        # Test CloudWatch metrics
        success = aws_integration.send_cloudwatch_metric(
            "StreamingAnalytics",
            "TestMetric",
            100.0,
            {"Environment": "test"}
        )
        print(f"CloudWatch metric success: {success}")
        
        # Test pipeline creation
        pipeline_config = {
            "source": "kafka",
            "destination": "s3",
            "processing": "spark"
        }
        success = aws_integration.create_data_pipeline("test-pipeline", pipeline_config)
        print(f"Pipeline creation success: {success}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
