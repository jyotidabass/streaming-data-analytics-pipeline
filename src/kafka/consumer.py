"""Kafka consumer for processing streaming data."""

import json
import logging
from typing import Dict, Any, Callable, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ..config import settings

logger = logging.getLogger(__name__)


class StreamingDataConsumer:
    """Consumer for processing streaming analytics data."""
    
    def __init__(self, group_id: Optional[str] = None):
        """Initialize Kafka consumer."""
        self.group_id = group_id or settings.kafka_group_id
        self.consumer = KafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            max_poll_records=500,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        self.message_handlers: Dict[str, Callable] = {}
        self.running = False
    
    def register_handler(self, topic: str, handler: Callable[[Dict[str, Any]], None]):
        """Register a message handler for a specific topic."""
        self.message_handlers[topic] = handler
        logger.info(f"Registered handler for topic: {topic}")
    
    def subscribe(self, topics: list):
        """Subscribe to Kafka topics."""
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
    
    def process_message(self, message) -> bool:
        """Process a single message."""
        try:
            topic = message.topic
            key = message.key
            value = message.value
            offset = message.offset
            partition = message.partition
            
            logger.debug(f"Processing message from topic {topic}, partition {partition}, offset {offset}")
            
            # Call registered handler if available
            if topic in self.message_handlers:
                self.message_handlers[topic](value)
                return True
            else:
                logger.warning(f"No handler registered for topic: {topic}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def start_consuming(self, topics: list, timeout_ms: int = 1000):
        """Start consuming messages from specified topics."""
        self.subscribe(topics)
        self.running = True
        
        logger.info(f"Starting to consume from topics: {topics}")
        
        try:
            while self.running:
                message_batch = self.consumer.poll(timeout_ms=timeout_ms)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self.process_message(message)
                        
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the consumer."""
        self.running = False
        self.consumer.close()
        logger.info("Consumer stopped")
    
    def get_topic_metadata(self, topics: list) -> Dict[str, Any]:
        """Get metadata for specified topics."""
        try:
            metadata = self.consumer.list_consumer_group_offsets()
            return {
                "group_id": self.group_id,
                "topics": topics,
                "metadata": metadata
            }
        except Exception as e:
            logger.error(f"Error getting topic metadata: {e}")
            return {}


class UserEventProcessor:
    """Processor for user events."""
    
    def __init__(self):
        self.event_counts = {}
        self.user_sessions = {}
    
    def process_user_event(self, event: Dict[str, Any]):
        """Process a user event."""
        try:
            user_id = event.get("user_id")
            session_id = event.get("session_id")
            event_type = event.get("event_type")
            timestamp = event.get("timestamp")
            
            # Update event counts
            if event_type not in self.event_counts:
                self.event_counts[event_type] = 0
            self.event_counts[event_type] += 1
            
            # Track user sessions
            if user_id not in self.user_sessions:
                self.user_sessions[user_id] = set()
            self.user_sessions[user_id].add(session_id)
            
            # Log important events
            if event_type in ["purchase", "signup", "login"]:
                logger.info(f"Important event: {event_type} by user {user_id} at {timestamp}")
            
            # Process event-specific logic
            if event_type == "purchase":
                self._process_purchase_event(event)
            elif event_type == "video_complete":
                self._process_video_complete_event(event)
            elif event_type == "search":
                self._process_search_event(event)
                
        except Exception as e:
            logger.error(f"Error processing user event: {e}")
    
    def _process_purchase_event(self, event: Dict[str, Any]):
        """Process purchase event."""
        try:
            event_data = json.loads(event.get("event_data", "{}"))
            total_amount = event_data.get("total_amount", 0)
            items_count = event_data.get("items_count", 0)
            
            logger.info(f"Purchase completed: ${total_amount} for {items_count} items")
            
        except Exception as e:
            logger.error(f"Error processing purchase event: {e}")
    
    def _process_video_complete_event(self, event: Dict[str, Any]):
        """Process video completion event."""
        try:
            event_data = json.loads(event.get("event_data", "{}"))
            video_id = event_data.get("video_id")
            video_duration = event_data.get("video_duration", 0)
            
            logger.info(f"Video completed: {video_id} (duration: {video_duration}s)")
            
        except Exception as e:
            logger.error(f"Error processing video complete event: {e}")
    
    def _process_search_event(self, event: Dict[str, Any]):
        """Process search event."""
        try:
            event_data = json.loads(event.get("event_data", "{}"))
            search_query = event_data.get("search_query", "")
            results_count = event_data.get("search_results_count", 0)
            
            logger.info(f"Search performed: '{search_query}' ({results_count} results)")
            
        except Exception as e:
            logger.error(f"Error processing search event: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        total_events = sum(self.event_counts.values())
        unique_users = len(self.user_sessions)
        total_sessions = sum(len(sessions) for sessions in self.user_sessions.values())
        
        return {
            "total_events_processed": total_events,
            "unique_users": unique_users,
            "total_sessions": total_sessions,
            "event_type_counts": self.event_counts,
            "average_events_per_user": total_events / unique_users if unique_users > 0 else 0
        }


class AnalyticsEventProcessor:
    """Processor for analytics events."""
    
    def __init__(self):
        self.metrics = {}
        self.metric_history = []
    
    def process_analytics_event(self, event: Dict[str, Any]):
        """Process an analytics event."""
        try:
            metric_name = event.get("metric_name")
            metric_value = event.get("metric_value")
            dimensions = json.loads(event.get("dimensions", "{}"))
            timestamp = event.get("timestamp")
            
            # Store metric
            if metric_name not in self.metrics:
                self.metrics[metric_name] = []
            
            self.metrics[metric_name].append({
                "value": metric_value,
                "dimensions": dimensions,
                "timestamp": timestamp
            })
            
            # Keep only recent metrics (last 1000 entries per metric)
            if len(self.metrics[metric_name]) > 1000:
                self.metrics[metric_name] = self.metrics[metric_name][-1000:]
            
            # Log significant metric changes
            if len(self.metrics[metric_name]) > 1:
                previous_value = self.metrics[metric_name][-2]["value"]
                change_percent = ((metric_value - previous_value) / previous_value) * 100
                
                if abs(change_percent) > 20:  # Significant change
                    logger.info(f"Significant change in {metric_name}: {change_percent:.1f}%")
            
        except Exception as e:
            logger.error(f"Error processing analytics event: {e}")
    
    def get_metric_summary(self, metric_name: str) -> Dict[str, Any]:
        """Get summary for a specific metric."""
        if metric_name not in self.metrics:
            return {}
        
        values = [entry["value"] for entry in self.metrics[metric_name]]
        
        return {
            "metric_name": metric_name,
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "latest_value": values[-1] if values else None
        }
    
    def get_all_metrics_summary(self) -> Dict[str, Any]:
        """Get summary for all metrics."""
        return {
            metric_name: self.get_metric_summary(metric_name)
            for metric_name in self.metrics.keys()
        }


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create processors
    user_processor = UserEventProcessor()
    analytics_processor = AnalyticsEventProcessor()
    
    # Create consumer
    consumer = StreamingDataConsumer()
    
    # Register handlers
    consumer.register_handler(settings.kafka_topic_user_events, user_processor.process_user_event)
    consumer.register_handler(settings.kafka_topic_analytics, analytics_processor.process_analytics_event)
    
    try:
        # Start consuming
        topics = [settings.kafka_topic_user_events, settings.kafka_topic_analytics]
        consumer.start_consuming(topics)
        
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        # Print statistics
        print("\n=== User Event Statistics ===")
        print(json.dumps(user_processor.get_statistics(), indent=2))
        
        print("\n=== Analytics Metrics Summary ===")
        print(json.dumps(analytics_processor.get_all_metrics_summary(), indent=2))
