from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import json
import time
from datetime import datetime
from collections import deque
import threading
import atexit
import sys

# Import our pure Python implementations
from kafka_manager import KafkaManager
from sentiment_analyzer import MentalHealthSentimentAnalyzer
from elasticsearch_manager import ElasticsearchManager
from message_bus import message_bus

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'mental-health-big-data-2024'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Initialize components
analyzer = MentalHealthSentimentAnalyzer()
kafka_manager = KafkaManager()
es_manager = ElasticsearchManager()

# Statistics
stats = {
    'total_messages': 0,
    'sentiment_counts': {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0, 'CRISIS': 0},
    'mental_health_messages': 0,
    'crisis_alerts': 0,
    'start_time': datetime.now().isoformat(),
    'kafka_available': True,
    'elasticsearch_available': True,
    'message_bus': True
}

# Duplicate prevention
message_history = set()
sentiment_history = set()
MAX_HISTORY_SIZE = 1000

def generate_message_id(sentiment_data):
    """Generate unique ID for message to prevent duplicates"""
    username = sentiment_data.get('username', 'Anonymous')
    text = sentiment_data.get('text', '')
    timestamp = sentiment_data.get('timestamp', '')
    return f"{username}_{text}_{timestamp}"

def cleanup_history():
    """Clean up message history to prevent memory issues"""
    if len(message_history) > MAX_HISTORY_SIZE:
        # Convert to list and keep only recent half
        history_list = list(message_history)
        message_history.clear()
        message_history.update(history_list[-MAX_HISTORY_SIZE//2:])
    
    if len(sentiment_history) > MAX_HISTORY_SIZE:
        sentiment_list = list(sentiment_history)
        sentiment_history.clear()
        sentiment_history.update(sentiment_list[-MAX_HISTORY_SIZE//2:])

def sentiment_callback(sentiment_data):
    """Callback for when new sentiment data is received"""
    try:
        # Check for duplicates
        message_id = generate_message_id(sentiment_data)
        if message_id in sentiment_history:
            print(f"Duplicate sentiment data detected, skipping: {message_id}")
            return
        
        sentiment_history.add(message_id)
        
        # Update statistics
        stats['total_messages'] += 1
        label = sentiment_data.get('sentiment_label', 'NEUTRAL')
        
        if label in stats['sentiment_counts']:
            stats['sentiment_counts'][label] += 1
        
        if sentiment_data.get('mental_health_context', {}).get('has_any_context', False):
            stats['mental_health_messages'] += 1
        
        if label == 'CRISIS':
            stats['crisis_alerts'] += 1
            # Emit crisis alert to all connected clients
            socketio.emit('crisis_alert', {
                'username': sentiment_data.get('username', 'User'),
                'message': sentiment_data.get('text', ''),
                'timestamp': sentiment_data.get('timestamp', ''),
                'severity': 'HIGH'
            })
        
        # Index in Elasticsearch
        es_manager.index_sentiment_data(sentiment_data)
        
        # Send update to all connected clients
        socketio.emit('sentiment_update', {
            'sentiment_data': sentiment_data,
            'stats': get_current_stats()
        })
        
        # Clean up history periodically
        if stats['total_messages'] % 50 == 0:
            cleanup_history()
            
    except Exception as e:
        print(f"Error in sentiment callback: {e}")

# Start message bus consumer
kafka_manager.start_consumer(sentiment_callback)

def get_current_stats():
    """Get current statistics with percentages"""
    try:
        total = stats['total_messages']
        current_stats = stats.copy()
        
        if total > 0:
            # Calculate percentages
            current_stats['positive_percent'] = (stats['sentiment_counts']['POSITIVE'] / total) * 100
            current_stats['negative_percent'] = (stats['sentiment_counts']['NEGATIVE'] / total) * 100
            current_stats['neutral_percent'] = (stats['sentiment_counts']['NEUTRAL'] / total) * 100
            current_stats['crisis_percent'] = (stats['sentiment_counts']['CRISIS'] / total) * 100
            current_stats['mental_health_percent'] = (stats['mental_health_messages'] / total) * 100
            
            # Calculate average sentiment score
            sentiment_scores = [0.7, 0.3, 0.5]  # Placeholder - you might want to track actual scores
            current_stats['average_sentiment'] = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0
        else:
            current_stats.update({
                'positive_percent': 0,
                'negative_percent': 0,
                'neutral_percent': 0,
                'crisis_percent': 0,
                'mental_health_percent': 0,
                'average_sentiment': 0
            })
        
        # Get message bus metrics
        bus_metrics = kafka_manager.get_bus_metrics()
        current_stats['bus_metrics'] = bus_metrics
        
        # Get Elasticsearch stats
        es_stats = es_manager.get_sentiment_stats()
        current_stats.update(es_stats)
        
        return current_stats
    except Exception as e:
        print(f"Error getting current stats: {e}")
        return stats.copy()

@app.route('/')
def index():
    """Main page - clear history on load to prevent stale data"""
    message_history.clear()
    sentiment_history.clear()
    return render_template('index.html')

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    try:
        return jsonify(get_current_stats())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/recent-messages')
def api_recent_messages():
    """API endpoint for recent messages"""
    try:
        messages = kafka_manager.get_recent_messages(20)
        # Clear history when loading recent messages to prevent duplicates
        message_history.clear()
        return jsonify(messages)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/services-status')
def api_services_status():
    """Check status of all services"""
    try:
        status = {
            'kafka': '✅ Pure Python Message Bus',
            'elasticsearch': '✅ In-Memory Storage',
            'message_bus': '✅ Running',
            'sentiment_ai': '✅ Active'
        }
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bus-info')
def api_bus_info():
    """Get message bus information"""
    try:
        bus_info = kafka_manager.get_bus_info()
        return jsonify(bus_info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/send-message', methods=['POST'])
def api_send_message():
    """API endpoint to send messages with duplicate prevention"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
            
        username = data.get('username', 'Anonymous')
        message = data.get('message', '')
        
        if not message.strip():
            return jsonify({'error': 'Message cannot be empty'}), 400
        
        # Check for duplicate message
        message_id = f"{username}_{message}_{datetime.now().timestamp()}"
        if message_id in message_history:
            return jsonify({'error': 'Duplicate message detected'}), 400
            
        message_history.add(message_id)
        
        # Send to message bus
        success = kafka_manager.send_chat_message(username, message)
        
        if success:
            # Analyze sentiment immediately
            sentiment_data = analyzer.analyze_sentiment(message)
            sentiment_data['username'] = username
            
            # Send to results topic
            kafka_manager.send_sentiment_result(sentiment_data)
            
            return jsonify({
                'success': True,
                'sentiment': sentiment_data,
                'message': 'Message sent and analyzed successfully'
            })
        else:
            # Remove from history if failed
            message_history.discard(message_id)
            return jsonify({'error': 'Failed to send message'}), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(400)
def bad_request(error):
    """Handle 400 errors"""
    return jsonify({'error': 'Bad request'}), 400

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({'error': 'Internal server error'}), 500

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('Client connected')
    try:
        emit('connection_established', {
            'status': 'connected',
            'kafka_available': True,
            'elasticsearch_available': True,
            'message_bus': True
        })
    except Exception as e:
        print(f"Error in connect handler: {e}")

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

@socketio.on('send_chat_message')
def handle_chat_message(data):
    """Handle chat messages from socket with duplicate prevention"""
    try:
        username = data.get('username', 'Anonymous')
        message = data.get('message', '')
        
        if message:
            # Check for duplicate
            message_id = f"{username}_{message}_{datetime.now().timestamp()}"
            if message_id in message_history:
                return
                
            message_history.add(message_id)
            
            # Send to message bus
            kafka_manager.send_chat_message(username, message)
            
            # Analyze and broadcast
            sentiment_data = analyzer.analyze_sentiment(message)
            sentiment_data['username'] = username
            kafka_manager.send_sentiment_result(sentiment_data)
            
    except Exception as e:
        print(f"Error in handle_chat_message: {e}")

@socketio.on_error()
def handle_error(e):
    """Handle socket errors"""
    print(f"Socket error: {e}")

def cleanup():
    """Cleanup function for graceful shutdown"""
    print("Cleaning up resources...")
    kafka_manager.stop_consumer()
    message_history.clear()
    sentiment_history.clear()

# Register cleanup function
atexit.register(cleanup)

if __name__ == '__main__':
    print("🚀 Starting Mental Health Sentiment Detection...")
    print("📊 Dashboard: http://localhost:5000")
    print("💬 Using Pure Python Message Bus ")
    print("📈 Real-time sentiment analysis with crisis detection")
    print("🎯 Big Data concepts demonstrated without complex setup")
    print("🛡️  Duplicate prevention and error handling enabled")
    
    print("\n💡 Try these test messages:")
    print("   - 'Feeling anxious about my exam tomorrow'")
    print("   - 'Had a great therapy session today!'") 
    print("   - 'I feel hopeless and overwhelmed'")
    print("   - 'Meditation is helping me stay calm'")
    print("   - 'This depression is really hard to handle'")
    
    try:
        socketio.run(app, debug=True, host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        cleanup()
    except Exception as e:
        print(f"Error starting server: {e}")
        sys.exit(1)