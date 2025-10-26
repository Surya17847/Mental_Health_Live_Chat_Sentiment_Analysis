import subprocess
import time
import requests
import os
from dotenv import load_dotenv

load_dotenv()

class DockerManager:
    def __init__(self):
        self.compose_file = "docker-compose.yml"
        
    def start_services(self):
        """Start all Docker services"""
        print("🚀 Starting Big Data services with Docker...")
        
        try:
            # Start services in detached mode
            result = subprocess.run([
                "docker-compose", "-f", self.compose_file, "up", "-d"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Docker services started successfully!")
                self.wait_for_services()
                return True
            else:
                print(f"❌ Failed to start services: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Error starting Docker services: {e}")
            return False

    def stop_services(self):
        """Stop all Docker services"""
        print("🛑 Stopping Docker services...")
        
        try:
            subprocess.run([
                "docker-compose", "-f", self.compose_file, "down"
            ], capture_output=True)
            print("✅ Docker services stopped!")
        except Exception as e:
            print(f"❌ Error stopping services: {e}")

    def wait_for_services(self):
        """Wait for services to be ready"""
        print("⏳ Waiting for services to be ready...")
        
        # Wait for Kafka
        kafka_ready = False
        for i in range(30):
            try:
                from kafka import KafkaProducer
                producer = KafkaProducer(bootstrap_servers='localhost:9092')
                producer.close()
                kafka_ready = True
                print("✅ Kafka is ready!")
                break
            except:
                print(f"⏳ Waiting for Kafka... ({i+1}/30)")
                time.sleep(2)
        
        # Wait for Elasticsearch
        es_ready = False
        for i in range(30):
            try:
                response = requests.get('http://localhost:9200')
                if response.status_code == 200:
                    es_ready = True
                    print("✅ Elasticsearch is ready!")
                    break
            except:
                print(f"⏳ Waiting for Elasticsearch... ({i+1}/30)")
                time.sleep(2)
        
        # Wait for Kibana
        kibana_ready = False
        for i in range(30):
            try:
                response = requests.get('http://localhost:5601/api/status')
                if response.status_code == 200:
                    kibana_ready = True
                    print("✅ Kibana is ready!")
                    break
            except:
                print(f"⏳ Waiting for Kibana... ({i+1}/30)")
                time.sleep(2)
        
        if kafka_ready and es_ready:
            print("🎉 All Big Data services are ready!")
        else:
            print("⚠️ Some services may not be fully ready")

    def check_services_status(self):
        """Check if all services are running"""
        services = {
            'zookeeper': 2181,
            'kafka': 9092,
            'elasticsearch': 9200,
            'kibana': 5601
        }
        
        status = {}
        for service, port in services.items():
            try:
                if service == 'kafka':
                    from kafka import KafkaProducer
                    producer = KafkaProducer(bootstrap_servers=f'localhost:{port}')
                    producer.close()
                    status[service] = '✅ Running'
                else:
                    response = requests.get(f'http://localhost:{port}', timeout=5)
                    status[service] = '✅ Running'
            except:
                status[service] = '❌ Not running'
        
        return status