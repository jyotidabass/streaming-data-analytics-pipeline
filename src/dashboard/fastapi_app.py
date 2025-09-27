"""FastAPI application for streaming analytics pipeline API."""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import uvicorn
import json
import asyncio
from pydantic import BaseModel

from ..config import settings
from ..kafka import StreamingDataConsumer, UserEventProcessor, AnalyticsEventProcessor
from ..sql import SQLOptimizer
from ..dask import DaskDistributedProcessor
from ..cloud import AWSCloudIntegration, GCPCloudIntegration

logger = logging.getLogger(__name__)

# Pydantic models for API requests/responses
class EventData(BaseModel):
    user_id: str
    session_id: str
    event_type: str
    event_data: Dict[str, Any]
    timestamp: datetime
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    page_url: Optional[str] = None

class AnalyticsQuery(BaseModel):
    query: str
    parameters: Optional[Dict[str, Any]] = None

class PipelineConfig(BaseModel):
    name: str
    config: Dict[str, Any]
    enabled: bool = True

class HealthCheck(BaseModel):
    status: str
    timestamp: datetime
    components: Dict[str, str]

# Initialize FastAPI app
app = FastAPI(
    title="Streaming Analytics Pipeline API",
    description="API for managing and monitoring the streaming analytics pipeline",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
sql_optimizer = None
dask_processor = None
user_processor = UserEventProcessor()
analytics_processor = AnalyticsEventProcessor()
aws_integration = None
gcp_integration = None

@app.on_event("startup")
async def startup_event():
    """Initialize components on startup."""
    global sql_optimizer, dask_processor, aws_integration, gcp_integration
    
    try:
        sql_optimizer = SQLOptimizer()
        dask_processor = DaskDistributedProcessor()
        
        if settings.aws_access_key_id:
            aws_integration = AWSCloudIntegration()
        
        if settings.gcp_project_id:
            gcp_integration = GCPCloudIntegration()
        
        logger.info("API components initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing API components: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global dask_processor
    
    if dask_processor:
        dask_processor.close()
    
    logger.info("API shutdown completed")

# Health check endpoint
@app.get("/health", response_model=HealthCheck)
async def health_check():
    """Check the health of the API and its components."""
    components = {}
    
    # Check SQL optimizer
    try:
        if sql_optimizer:
            components["sql_optimizer"] = "healthy"
        else:
            components["sql_optimizer"] = "unavailable"
    except Exception:
        components["sql_optimizer"] = "error"
    
    # Check Dask processor
    try:
        if dask_processor:
            components["dask_processor"] = "healthy"
        else:
            components["dask_processor"] = "unavailable"
    except Exception:
        components["dask_processor"] = "error"
    
    # Check cloud integrations
    try:
        if aws_integration:
            components["aws_integration"] = "healthy"
        else:
            components["aws_integration"] = "unavailable"
    except Exception:
        components["aws_integration"] = "error"
    
    try:
        if gcp_integration:
            components["gcp_integration"] = "healthy"
        else:
            components["gcp_integration"] = "unavailable"
    except Exception:
        components["gcp_integration"] = "error"
    
    # Determine overall status
    if all(status == "healthy" for status in components.values()):
        overall_status = "healthy"
    elif any(status == "error" for status in components.values()):
        overall_status = "error"
    else:
        overall_status = "degraded"
    
    return HealthCheck(
        status=overall_status,
        timestamp=datetime.utcnow(),
        components=components
    )

# Event endpoints
@app.post("/events")
async def create_event(event: EventData):
    """Create a new event."""
    try:
        # Process the event
        user_processor.process_user_event(event.dict())
        
        return JSONResponse(
            status_code=201,
            content={"message": "Event created successfully", "event_id": f"event_{datetime.now().timestamp()}"}
        )
        
    except Exception as e:
        logger.error(f"Error creating event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events")
async def get_events(limit: int = 100, offset: int = 0):
    """Get recent events."""
    try:
        # This would typically query the database
        # For now, return sample data
        events = [
            {
                "user_id": f"user_{i}",
                "session_id": f"session_{i}",
                "event_type": "page_view",
                "timestamp": datetime.now() - timedelta(minutes=i),
                "page_url": f"/page_{i}"
            }
            for i in range(offset, min(offset + limit, 1000))
        ]
        
        return JSONResponse(content={"events": events, "total": len(events)})
        
    except Exception as e:
        logger.error(f"Error getting events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Analytics endpoints
@app.post("/analytics/query")
async def execute_analytics_query(query: AnalyticsQuery):
    """Execute an analytics query."""
    try:
        if not sql_optimizer:
            raise HTTPException(status_code=503, detail="SQL optimizer not available")
        
        # Execute the query
        result_df = sql_optimizer.execute_optimized_query(query.query, query.parameters)
        
        # Convert DataFrame to JSON
        result = result_df.to_dict('records')
        
        return JSONResponse(content={"results": result, "count": len(result)})
        
    except Exception as e:
        logger.error(f"Error executing analytics query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/metrics")
async def get_analytics_metrics():
    """Get current analytics metrics."""
    try:
        # Get metrics from processors
        user_stats = user_processor.get_statistics()
        analytics_stats = analytics_processor.get_all_metrics_summary()
        
        metrics = {
            "user_metrics": user_stats,
            "analytics_metrics": analytics_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return JSONResponse(content=metrics)
        
    except Exception as e:
        logger.error(f"Error getting analytics metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Pipeline management endpoints
@app.post("/pipelines")
async def create_pipeline(pipeline: PipelineConfig):
    """Create a new data pipeline."""
    try:
        # Create pipeline configuration
        pipeline_config = {
            "name": pipeline.name,
            "config": pipeline.config,
            "enabled": pipeline.enabled,
            "created_at": datetime.utcnow().isoformat()
        }
        
        # Store pipeline configuration
        if aws_integration:
            success = aws_integration.create_data_pipeline(pipeline.name, pipeline_config)
        elif gcp_integration:
            success = gcp_integration.create_data_pipeline(pipeline.name, pipeline_config)
        else:
            # Store locally (in-memory for demo)
            success = True
        
        if success:
            return JSONResponse(
                status_code=201,
                content={"message": "Pipeline created successfully", "pipeline": pipeline_config}
            )
        else:
            raise HTTPException(status_code=500, detail="Failed to create pipeline")
        
    except Exception as e:
        logger.error(f"Error creating pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipelines")
async def get_pipelines():
    """Get all pipelines."""
    try:
        # This would typically query the database or cloud storage
        # For now, return sample data
        pipelines = [
            {
                "name": "user_events_pipeline",
                "config": {"source": "kafka", "destination": "database"},
                "enabled": True,
                "created_at": datetime.utcnow().isoformat()
            },
            {
                "name": "analytics_pipeline",
                "config": {"source": "database", "destination": "cloud_storage"},
                "enabled": True,
                "created_at": datetime.utcnow().isoformat()
            }
        ]
        
        return JSONResponse(content={"pipelines": pipelines})
        
    except Exception as e:
        logger.error(f"Error getting pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipelines/{pipeline_name}/health")
async def get_pipeline_health(pipeline_name: str):
    """Get pipeline health status."""
    try:
        if aws_integration:
            health = aws_integration.monitor_pipeline_health(pipeline_name)
        elif gcp_integration:
            health = gcp_integration.monitor_pipeline_health(pipeline_name)
        else:
            # Return mock health data
            health = {
                "pipeline_name": pipeline_name,
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "metrics": {
                    "events_processed": 1234567,
                    "error_rate": 0.12,
                    "latency_ms": 45
                }
            }
        
        return JSONResponse(content=health)
        
    except Exception as e:
        logger.error(f"Error getting pipeline health: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Data processing endpoints
@app.post("/process/batch")
async def process_batch_data(background_tasks: BackgroundTasks):
    """Start batch data processing."""
    try:
        if not dask_processor:
            raise HTTPException(status_code=503, detail="Dask processor not available")
        
        # Start batch processing in background
        background_tasks.add_task(run_batch_processing)
        
        return JSONResponse(
            content={"message": "Batch processing started", "status": "running"}
        )
        
    except Exception as e:
        logger.error(f"Error starting batch processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/process/status")
async def get_processing_status():
    """Get current processing status."""
    try:
        # This would typically check the actual processing status
        # For now, return mock data
        status = {
            "batch_processing": {
                "status": "idle",
                "last_run": datetime.utcnow().isoformat(),
                "records_processed": 0
            },
            "stream_processing": {
                "status": "running",
                "events_per_second": 1234,
                "total_events": 1234567
            }
        }
        
        return JSONResponse(content=status)
        
    except Exception as e:
        logger.error(f"Error getting processing status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Cloud storage endpoints
@app.get("/storage/s3/objects")
async def list_s3_objects(bucket: str, prefix: str = ""):
    """List objects in S3 bucket."""
    try:
        if not aws_integration:
            raise HTTPException(status_code=503, detail="AWS integration not available")
        
        objects = aws_integration.list_s3_objects(bucket, prefix)
        
        return JSONResponse(content={"objects": objects, "count": len(objects)})
        
    except Exception as e:
        logger.error(f"Error listing S3 objects: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/storage/gcs/objects")
async def list_gcs_objects(bucket: str, prefix: str = ""):
    """List objects in GCS bucket."""
    try:
        if not gcp_integration:
            raise HTTPException(status_code=503, detail="GCP integration not available")
        
        objects = gcp_integration.list_gcs_objects(bucket, prefix)
        
        return JSONResponse(content={"objects": objects, "count": len(objects)})
        
    except Exception as e:
        logger.error(f"Error listing GCS objects: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Real-time streaming endpoint
@app.get("/stream/events")
async def stream_events():
    """Stream real-time events."""
    async def event_generator():
        """Generate real-time events."""
        while True:
            # Generate sample event
            event = {
                "user_id": f"user_{datetime.now().timestamp()}",
                "session_id": f"session_{datetime.now().timestamp()}",
                "event_type": "page_view",
                "timestamp": datetime.utcnow().isoformat(),
                "page_url": "/dashboard"
            }
            
            yield f"data: {json.dumps(event)}\n\n"
            await asyncio.sleep(1)  # Send event every second
    
    return StreamingResponse(
        event_generator(),
        media_type="text/plain",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )

# Utility functions
async def run_batch_processing():
    """Run batch data processing."""
    try:
        logger.info("Starting batch processing")
        
        # Create sample dataset
        ddf = dask_processor.create_large_dataset(n_rows=100000, n_partitions=10)
        
        # Process data
        user_metrics = dask_processor.process_user_events_distributed(ddf)
        session_metrics = dask_processor.calculate_session_analytics(ddf)
        
        # Save results
        results = {
            'user_metrics': user_metrics,
            'session_metrics': session_metrics
        }
        dask_processor.save_results(results, "/tmp/batch_results")
        
        logger.info("Batch processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")

# Main function to run the API
def main():
    """Main function to run the FastAPI application."""
    uvicorn.run(
        "src.dashboard.fastapi_app:app",
        host=settings.api_host,
        port=settings.api_port,
        workers=settings.api_workers,
        reload=True
    )

if __name__ == "__main__":
    main()
