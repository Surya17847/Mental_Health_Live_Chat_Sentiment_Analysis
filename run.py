#!/usr/bin/env python3
"""
Mental Health Sentiment Detection - Docker Edition
Big Data Analytics Project
"""

import os
import sys
import webbrowser
import threading
import time
from docker_manager import DockerManager

def main():
    print("🧠 Mental Health Sentiment Detection - Big Data Analytics")
    print("=" * 60)
    
    # Check if Docker is running
    docker_manager = DockerManager()
    
    print("\n🚀 Starting the application...")
    
    # Start Docker services
    print("\n📦 Starting Big Data services (Kafka, Elasticsearch, Kibana)...")
    if not docker_manager.start_services():
        print("❌ Failed to start Docker services. Please make sure Docker is running.")
        return
    
    # Wait a bit for services to initialize
    print("\n⏳ Waiting for services to be ready...")
    time.sleep(10)
    
    # Open browser automatically
    def open_browser():
        time.sleep(3)
        webbrowser.open('http://localhost:5000')
    
    browser_thread = threading.Thread(target=open_browser)
    browser_thread.daemon = True
    browser_thread.start()
    
    # Start Flask application
    print("\n🌐 Starting web application...")
    print("📊 Dashboard will open automatically at: http://localhost:5000")
    print("🔍 Kibana: http://localhost:5601")
    print("💬 Kafka: localhost:9092")
    print("📈 Elasticsearch: http://localhost:9200")
    print("\n🛑 Press Ctrl+C to stop all services")
    
    try:
        from app import app, socketio
        socketio.run(app, debug=False, host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        print("🧹 Cleaning up Docker services...")
        docker_manager.stop_services()

if __name__ == '__main__':
    main()