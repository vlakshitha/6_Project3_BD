from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
import io
import json
import time
import threading
import socket
import base64

# Configuration - CHANGE Node2-IP to your Kafka broker's actual IP
KAFKA_BROKER = "172.30.234.114:9092"
WORKER_ID = "worker-1"
GROUP_ID = "image-workers"

class ImageWorker:
    def __init__(self, worker_id, kafka_broker):
        self.worker_id = worker_id
        self.kafka_broker = kafka_broker
        
        # Consumer for receiving tasks
        self.consumer = KafkaConsumer(
            'tasks',
            bootstrap_servers=kafka_broker,
            group_id=GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        # Producer for sending results
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.running = True
        print(f"{self.worker_id}: Worker initialized")
    
    def process_tile(self, tile_data):
        """Convert tile image to grayscale"""
        try:
            # Decode base64 image data
            image_bytes = base64.b64decode(tile_data)
            image = Image.open(io.BytesIO(image_bytes))
            
            # Convert to grayscale
            grayscale_image = image.convert('L')
            
            # Re-encode to base64
            buffer = io.BytesIO()
            grayscale_image.save(buffer, format='PNG')
            processed_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
            return processed_data
        except Exception as e:
            print(f"{self.worker_id}: Error processing tile: {e}")
            return None
    
    def send_heartbeat(self):
        """Send periodic heartbeat messages every 5 seconds"""
        while self.running:
            try:
                heartbeat = {
                    'worker_id': self.worker_id,  # ✅ FIXED: Changed to 'worker_id' with underscore
                    'timestamp': time.time(),
                    'status': 'active',
                    'hostname': socket.gethostname()
                }
                self.producer.send('heartbeats', value=heartbeat)
                print(f"{self.worker_id}: Heartbeat sent")
                time.sleep(5)
            except Exception as e:
                print(f"{self.worker_id}: Heartbeat error: {e}")
    
    def consume_tasks(self):
        """Main loop - consume and process tiles"""
        print(f"{self.worker_id}: Starting to consume tasks...")
        for message in self.consumer:
            if not self.running:
                break
            try:
                task = message.value
                job_id = task['jobid']
                tile_id = task['tileid']
                tile_data = task['tiledata']
                
                print(f"{self.worker_id}: Processing job={job_id}, tile={tile_id}")
                
                # Process the tile
                start_time = time.time()
                processed_data = self.process_tile(tile_data)
                processing_time = time.time() - start_time
                
                if processed_data:
                    # Send result back
                    result = {
                        'jobid': job_id,
                        'tileid': tile_id,
                        'workerid': self.worker_id,
                        'tiledata': processed_data,
                        'processingtime': processing_time,
                        'timestamp': time.time()
                    }
                    self.producer.send('results', value=result)
                    print(f"{self.worker_id}: Completed tile {tile_id} in {processing_time:.2f}s")
            except Exception as e:
                print(f"{self.worker_id}: Error processing message: {e}")
    
    def start(self):
        """Start worker threads"""
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()
        
        # Start consuming tasks in main thread
        try:
            self.consume_tasks()
        except KeyboardInterrupt:
            print(f"{self.worker_id}: Shutting down...")
            self.running = False
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    worker = ImageWorker(WORKER_ID, KAFKA_BROKER)
    worker.start()
