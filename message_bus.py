import threading
import time
import json
from datetime import datetime
from collections import deque, defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MessageBus:
    """
    Pure Python message bus that simulates Kafka functionality
    No external dependencies required
    """
    
    def __init__(self):
        self.topics = defaultdict(deque)
        self.consumers = defaultdict(list)
        self.max_messages_per_topic = 1000
        self.lock = threading.RLock()
        
        # Statistics
        self.metrics = {
            'messages_sent': 0,
            'messages_consumed': 0,
            'topics_created': 0
        }
        
        logger.info("✅ Pure Python Message Bus initialized")

    def create_topic(self, topic_name):
        """Create a new topic"""
        with self.lock:
            if topic_name not in self.topics:
                self.topics[topic_name] = deque(maxlen=self.max_messages_per_topic)
                self.metrics['topics_created'] += 1
                logger.info(f"✅ Topic created: {topic_name}")

    def send_message(self, topic_name, message):
        """Send message to a topic"""
        with self.lock:
            if topic_name not in self.topics:
                self.create_topic(topic_name)
            
            # Add timestamp and unique ID
            message_data = {
                'message_id': int(datetime.now().timestamp() * 1000),
                'timestamp': datetime.now().isoformat(),
                'data': message
            }
            
            self.topics[topic_name].append(message_data)
            self.metrics['messages_sent'] += 1
            
            # Notify all consumers
            self._notify_consumers(topic_name, message_data)
            
            logger.info(f"✅ Message sent to {topic_name}: {str(message)[:50]}...")
            return True

    def subscribe(self, topic_name, callback, group_id=None):
        """Subscribe to a topic with a callback function"""
        with self.lock:
            consumer_id = f"{group_id or 'default'}_{len(self.consumers[topic_name])}"
            self.consumers[topic_name].append({
                'id': consumer_id,
                'callback': callback,
                'group_id': group_id,
                'last_offset': -1
            })
            
            logger.info(f"✅ Consumer {consumer_id} subscribed to {topic_name}")
            return consumer_id

    def _notify_consumers(self, topic_name, message_data):
        """Notify all consumers of a new message"""
        with self.lock:
            for consumer in self.consumers[topic_name]:
                try:
                    # Simulate async processing
                    threading.Thread(
                        target=consumer['callback'],
                        args=(message_data,),
                        daemon=True
                    ).start()
                    self.metrics['messages_consumed'] += 1
                except Exception as e:
                    logger.error(f"Error in consumer callback: {e}")

    def get_topic_messages(self, topic_name, limit=20):
        """Get recent messages from a topic"""
        with self.lock:
            if topic_name in self.topics:
                messages = list(self.topics[topic_name])
                return messages[-limit:]
            return []

    def get_metrics(self):
        """Get bus metrics"""
        return self.metrics.copy()

    def get_topic_info(self):
        """Get information about all topics"""
        with self.lock:
            info = {}
            for topic_name, messages in self.topics.items():
                info[topic_name] = {
                    'message_count': len(messages),
                    'consumers_count': len(self.consumers[topic_name]),
                    'latest_message': messages[-1] if messages else None
                }
            return info


# Global message bus instance
message_bus = MessageBus()