"""Kafka producer for simulating streaming data."""

import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from kafka import KafkaProducer
from kafka.errors import KafkaError

from ..config import settings

logger = logging.getLogger(__name__)


class StreamingDataProducer:
    """Producer for simulating streaming platform analytics data."""
    
    def __init__(self):
        """Initialize Kafka producer."""
        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        
        # Sample data for simulation
        self.user_ids = [f"user_{i:06d}" for i in range(1, 10001)]
        self.session_ids = [f"session_{i:08d}" for i in range(1, 50001)]
        self.event_types = [
            "page_view", "click", "scroll", "video_play", "video_pause",
            "video_complete", "search", "purchase", "add_to_cart", "remove_from_cart",
            "login", "logout", "signup", "share", "like", "comment", "follow"
        ]
        self.page_urls = [
            "/home", "/products", "/product/123", "/cart", "/checkout",
            "/profile", "/settings", "/search", "/category/electronics",
            "/category/clothing", "/category/books", "/about", "/contact",
            "/help", "/privacy", "/terms", "/blog", "/news"
        ]
        self.devices = ["Desktop", "Mobile", "Tablet"]
        self.browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
        self.operating_systems = ["Windows", "macOS", "Linux", "iOS", "Android"]
        self.countries = ["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "CN"]
        
    def generate_user_event(self) -> Dict[str, Any]:
        """Generate a realistic user event."""
        user_id = random.choice(self.user_ids)
        session_id = random.choice(self.session_ids)
        event_type = random.choice(self.event_types)
        timestamp = datetime.now() - timedelta(seconds=random.randint(0, 3600))
        
        # Generate event-specific data
        event_data = self._generate_event_data(event_type)
        
        # Generate realistic IP address
        ip_address = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
        
        # Generate realistic user agent
        browser = random.choice(self.browsers)
        os = random.choice(self.operating_systems)
        user_agent = f"Mozilla/5.0 ({os}) AppleWebKit/537.36 (KHTML, like Gecko) {browser}/91.0.4472.124 Safari/537.36"
        
        return {
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "event_data": json.dumps(event_data),
            "timestamp": timestamp.isoformat(),
            "ip_address": ip_address,
            "user_agent": user_agent,
            "page_url": random.choice(self.page_urls),
            "referrer": random.choice(self.page_urls) if random.random() > 0.3 else None
        }
    
    def _generate_event_data(self, event_type: str) -> Dict[str, Any]:
        """Generate event-specific data based on event type."""
        base_data = {
            "page_load_time": random.randint(100, 5000),
            "viewport_width": random.choice([1920, 1366, 1440, 1536, 1024]),
            "viewport_height": random.choice([1080, 768, 900, 864, 576])
        }
        
        if event_type == "page_view":
            base_data.update({
                "time_on_page": random.randint(5, 300),
                "scroll_depth": random.randint(0, 100)
            })
        elif event_type == "click":
            base_data.update({
                "element_id": f"btn_{random.randint(1, 100)}",
                "element_class": random.choice(["button", "link", "icon", "menu-item"]),
                "click_position": {
                    "x": random.randint(0, 1920),
                    "y": random.randint(0, 1080)
                }
            })
        elif event_type == "video_play":
            base_data.update({
                "video_id": f"video_{random.randint(1, 1000)}",
                "video_duration": random.randint(30, 3600),
                "video_quality": random.choice(["720p", "1080p", "4K"])
            })
        elif event_type == "search":
            base_data.update({
                "search_query": random.choice([
                    "laptop", "smartphone", "headphones", "camera", "book",
                    "clothing", "shoes", "watch", "tablet", "gaming"
                ]),
                "search_results_count": random.randint(0, 1000)
            })
        elif event_type == "purchase":
            base_data.update({
                "order_id": f"order_{random.randint(100000, 999999)}",
                "total_amount": round(random.uniform(10.0, 1000.0), 2),
                "currency": "USD",
                "items_count": random.randint(1, 10)
            })
        
        return base_data
    
    def generate_analytics_event(self) -> Dict[str, Any]:
        """Generate analytics/metrics event."""
        metric_names = [
            "page_views", "unique_visitors", "bounce_rate", "session_duration",
            "conversion_rate", "revenue", "cart_abandonment_rate", "search_queries",
            "video_completion_rate", "social_shares", "user_engagement_score"
        ]
        
        return {
            "metric_name": random.choice(metric_names),
            "metric_value": round(random.uniform(0.0, 100.0), 2),
            "dimensions": json.dumps({
                "device_type": random.choice(self.devices),
                "country": random.choice(self.countries),
                "hour": datetime.now().hour,
                "day_of_week": datetime.now().weekday()
            }),
            "timestamp": datetime.now().isoformat(),
            "source": "streaming_platform"
        }
    
    def send_user_event(self, event: Dict[str, Any]) -> bool:
        """Send user event to Kafka."""
        try:
            future = self.producer.send(
                settings.kafka_topic_user_events,
                key=event["user_id"],
                value=event
            )
            future.get(timeout=10)
            return True
        except KafkaError as e:
            logger.error(f"Failed to send user event: {e}")
            return False
    
    def send_analytics_event(self, event: Dict[str, Any]) -> bool:
        """Send analytics event to Kafka."""
        try:
            future = self.producer.send(
                settings.kafka_topic_analytics,
                key=event["metric_name"],
                value=event
            )
            future.get(timeout=10)
            return True
        except KafkaError as e:
            logger.error(f"Failed to send analytics event: {e}")
            return False
    
    def start_streaming_simulation(self, duration_minutes: int = 60, events_per_second: int = 10):
        """Start streaming data simulation."""
        logger.info(f"Starting streaming simulation for {duration_minutes} minutes at {events_per_second} events/second")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        event_count = 0
        
        try:
            while time.time() < end_time:
                batch_start = time.time()
                
                # Send batch of events
                for _ in range(events_per_second):
                    # 80% user events, 20% analytics events
                    if random.random() < 0.8:
                        event = self.generate_user_event()
                        success = self.send_user_event(event)
                    else:
                        event = self.generate_analytics_event()
                        success = self.send_analytics_event(event)
                    
                    if success:
                        event_count += 1
                
                # Wait for next second
                elapsed = time.time() - batch_start
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                
                # Log progress every 60 seconds
                if event_count % (events_per_second * 60) == 0:
                    logger.info(f"Sent {event_count} events so far")
        
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        finally:
            self.producer.flush()
            logger.info(f"Simulation completed. Total events sent: {event_count}")
    
    def send_burst_events(self, count: int, burst_duration_seconds: int = 10):
        """Send a burst of events for testing."""
        logger.info(f"Sending {count} events in {burst_duration_seconds} seconds")
        
        events_per_second = count // burst_duration_seconds
        start_time = time.time()
        end_time = start_time + burst_duration_seconds
        sent_count = 0
        
        while time.time() < end_time and sent_count < count:
            batch_start = time.time()
            
            for _ in range(min(events_per_second, count - sent_count)):
                event = self.generate_user_event()
                if self.send_user_event(event):
                    sent_count += 1
            
            elapsed = time.time() - batch_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
        
        self.producer.flush()
        logger.info(f"Burst completed. Sent {sent_count} events")
    
    def close(self):
        """Close the producer."""
        self.producer.close()


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    producer = StreamingDataProducer()
    
    try:
        # Start streaming simulation
        producer.start_streaming_simulation(duration_minutes=5, events_per_second=5)
        
        # Or send a burst of events
        # producer.send_burst_events(count=100, burst_duration_seconds=10)
        
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        producer.close()
