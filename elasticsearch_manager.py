import json
from datetime import datetime

class ElasticsearchManager:
    def __init__(self):
        # Simulate Elasticsearch with in-memory storage
        self.index_name = 'mental-health-sentiment'
        self.documents = []
        self.setup_index()

    def setup_index(self):
        """Simulate index creation"""
        print(f"✅ Created Elasticsearch index: {self.index_name}")

    def index_sentiment_data(self, data):
        """Store sentiment analysis data in memory"""
        try:
            document = {
                'timestamp': datetime.now(),
                'username': data.get('username', 'anonymous'),
                'message': data.get('text', ''),
                'sentiment_label': data.get('sentiment_label', 'NEUTRAL'),
                'sentiment_score': data.get('sentiment_score', 0),
                'severity': data.get('severity', 'LOW'),
                'mental_health_context': data.get('mental_health_context', {}).get('has_any_context', False),
                'needs_attention': data.get('needs_attention', False),
                'risk_level': data.get('risk_level', 'LOW')
            }
            
            self.documents.append(document)
            print(f"✅ Stored data in memory: {data['sentiment_label']}")
            return True
        except Exception as e:
            print(f"❌ Failed to store data: {e}")
            return False

    def get_sentiment_stats(self):
        """Get sentiment statistics from stored data"""
        try:
            total_messages = len(self.documents)
            
            # Calculate sentiment distribution
            sentiment_counts = {}
            total_score = 0
            mental_health_count = 0
            
            for doc in self.documents:
                sentiment = doc.get('sentiment_label', 'NEUTRAL')
                sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
                total_score += doc.get('sentiment_score', 0)
                
                if doc.get('mental_health_context', False):
                    mental_health_count += 1
            
            average_sentiment = total_score / total_messages if total_messages > 0 else 0
            
            stats = {
                'total_messages': total_messages,
                'sentiment_distribution': sentiment_counts,
                'average_sentiment': average_sentiment,
                'mental_health_messages': mental_health_count
            }
            
            return stats
            
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {
                'total_messages': 0,
                'sentiment_distribution': {},
                'average_sentiment': 0,
                'mental_health_messages': 0
            }