"""Configuration settings for the streaming analytics pipeline."""

import os
from typing import Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Application settings."""
    
    # Database Configuration
    database_url: str = Field(
        default="postgresql://admin:password@localhost:5432/streaming_analytics",
        env="DATABASE_URL"
    )
    redis_url: str = Field(
        default="redis://localhost:6379",
        env="REDIS_URL"
    )
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        env="KAFKA_BOOTSTRAP_SERVERS"
    )
    kafka_topic_user_events: str = Field(
        default="user_events",
        env="KAFKA_TOPIC_USER_EVENTS"
    )
    kafka_topic_analytics: str = Field(
        default="analytics_events",
        env="KAFKA_TOPIC_ANALYTICS"
    )
    kafka_group_id: str = Field(
        default="analytics_processor",
        env="KAFKA_GROUP_ID"
    )
    
    # Spark Configuration
    spark_master_url: str = Field(
        default="spark://localhost:7077",
        env="SPARK_MASTER_URL"
    )
    spark_app_name: str = Field(
        default="StreamingAnalyticsPipeline",
        env="SPARK_APP_NAME"
    )
    spark_executor_memory: str = Field(
        default="2g",
        env="SPARK_EXECUTOR_MEMORY"
    )
    spark_driver_memory: str = Field(
        default="1g",
        env="SPARK_DRIVER_MEMORY"
    )
    
    # AWS Configuration
    aws_access_key_id: Optional[str] = Field(
        default=None,
        env="AWS_ACCESS_KEY_ID"
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None,
        env="AWS_SECRET_ACCESS_KEY"
    )
    aws_region: str = Field(
        default="us-east-1",
        env="AWS_REGION"
    )
    s3_bucket: Optional[str] = Field(
        default=None,
        env="S3_BUCKET"
    )
    
    # GCP Configuration
    google_application_credentials: Optional[str] = Field(
        default=None,
        env="GOOGLE_APPLICATION_CREDENTIALS"
    )
    gcp_project_id: Optional[str] = Field(
        default=None,
        env="GCP_PROJECT_ID"
    )
    gcs_bucket: Optional[str] = Field(
        default=None,
        env="GCS_BUCKET"
    )
    
    # Application Configuration
    log_level: str = Field(
        default="INFO",
        env="LOG_LEVEL"
    )
    batch_size: int = Field(
        default=1000,
        env="BATCH_SIZE"
    )
    checkpoint_interval: int = Field(
        default=60,
        env="CHECKPOINT_INTERVAL"
    )
    window_duration: int = Field(
        default=300,
        env="WINDOW_DURATION"
    )
    slide_duration: int = Field(
        default=60,
        env="SLIDE_DURATION"
    )
    
    # API Configuration
    api_host: str = Field(
        default="0.0.0.0",
        env="API_HOST"
    )
    api_port: int = Field(
        default=8000,
        env="API_PORT"
    )
    api_workers: int = Field(
        default=4,
        env="API_WORKERS"
    )
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()
