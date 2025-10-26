import json
import threading
import time
from datetime import datetime
from collections import deque
import logging
from message_bus import message_bus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaManager:
    """
    Kafka-compatible interface using pure Python message bus
    """
    def __init__(self):
        self.bootstrap_servers = 'localhost:9092'  # For compatibility
        self.chat_topic = 'mental-health-chat'
        self.results_topic = 'sentiment-results'
        
        self.setup_topics()
        
        # Store recent messages for dashboard with message_id tracking
        self.recent_messages = []
        self.max_messages = 100
        self.processed_message_ids = set()  # Track processed messages to avoid duplicates
        self.consumer_started = False
        
        logger.info("✅ Pure Python Kafka Manager initialized")

    def setup_topics(self):
        """Create topics in the message bus"""
        message_bus.create_topic(self.chat_topic)
        message_bus.create_topic(self.results_topic)
        logger.info("✅ Topics created in message bus")

    def send_chat_message(self, username, message):
        """Send chat message to message bus"""
        chat_data = {
            'message_id': int(datetime.now().timestamp() * 1000),
            'username': username,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'platform': 'web_chat'
        }
        
        success = message_bus.send_message(self.chat_topic, chat_data)
        
        if success:
            logger.info(f"✅ Sent message to bus: {message[:50]}...")
            return True
        else:
            logger.error("❌ Failed to send message to bus")
            return False

    def send_sentiment_result(self, sentiment_data):
        """Send sentiment analysis result to message bus"""
        # Add unique ID to prevent duplicates
        sentiment_data['processed_id'] = f"{sentiment_data.get('message_id', '')}_{int(datetime.now().timestamp() * 1000)}"
        success = message_bus.send_message(self.results_topic, sentiment_data)
        return success

    def start_consumer(self, message_callback):
        """Start consuming sentiment results from message bus"""
        if self.consumer_started:
            logger.info("✅ Consumer already running")
            return
            
        def internal_callback(message_data):
            """Internal callback that processes messages"""
            try:
                data = message_data['data']
                message_id = data.get('processed_id') or data.get('message_id')
                
                # Check if we've already processed this message
                if message_id in self.processed_message_ids:
                    return
                
                # Mark as processed
                self.processed_message_ids.add(message_id)
                if len(self.processed_message_ids) > 1000:  # Prevent memory leak
                    self.processed_message_ids.clear()
                
                # Store recent message
                self.recent_messages.append(data)
                if len(self.recent_messages) > self.max_messages:
                    self.recent_messages.pop(0)
                
                # Call the external callback
                if message_callback:
                    message_callback(data)
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")

        # Subscribe to the results topic
        message_bus.subscribe(
            topic_name=self.results_topic,
            callback=internal_callback,
            group_id='dashboard-consumer'
        )
        
        self.consumer_started = True
        logger.info("🚀 Started message bus consumer for sentiment results...")

    def get_recent_messages(self, limit=20):
        """Get recent messages for dashboard"""
        # Return only unique messages from internal storage
        unique_messages = {}
        for msg in self.recent_messages:
            msg_id = msg.get('processed_id') or msg.get('message_id')
            if msg_id not in unique_messages:
                unique_messages[msg_id] = msg
        
        # Return most recent
        recent = sorted(
            unique_messages.values(),
            key=lambda x: x.get('timestamp', ''),
            reverse=True
        )[:limit]
        
        return recent

    def get_bus_metrics(self):
        """Get message bus metrics"""
        return message_bus.get_metrics()

    def get_bus_info(self):
        """Get message bus information"""
        return message_bus.get_topic_info()