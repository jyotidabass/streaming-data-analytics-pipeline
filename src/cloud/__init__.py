"""Cloud integration module for the streaming analytics pipeline."""

from .aws_integration import AWSCloudIntegration
from .gcp_integration import GCPCloudIntegration

__all__ = ["AWSCloudIntegration", "GCPCloudIntegration"]
