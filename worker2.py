from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
import io
import json
import time
import threading
import socket
import base64

# Configuration
KAFKA_BROKER = "172.30.234.114:9092"
WORKER_ID = "worker-2" # This is set for Node 4
GROUP_ID = "image-workers"

class ImageWorker:
    def __init__(self, worker_id, kafka_broker):
        self.worker_id = worker_id
        self.kafka_broker = kafka_broker
        # Consumer for tasks
        self.consumer = KafkaConsumer(
            'tasks',
            bootstrap_servers=[kafka_broker],
            group_id=GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        # Producer for results
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.running = True
        print(f"[{self.worker_id}] Worker initialized")

    def process_tile(self, tiledata):
        """Convert tile to grayscale"""
        try:
            # Decode image from base64
            image_bytes = base64.b64decode(tiledata)
            image = Image.open(io.BytesIO(image_bytes))
            
            # Convert to grayscale
            grayscale_image = image.convert('L')
            
            # Encode back to base64
            buffer = io.BytesIO()
            grayscale_image.save(buffer, format='PNG')
            processed_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
            return processed_data
        except Exception as e:
            print(f"[{self.worker_id}] Error processing tile: {e}")
            return None

    def send_heartbeat(self):
        """Send periodic heartbeat messages"""
        while self.running:
            try:
                heartbeat = {
                    'workerid': self.worker_id,        # corrected field name
                    'timestamp': time.time(),
                    'status': 'active',
                    'hostname': socket.gethostname()
                }
                self.producer.send('heartbeats', value=heartbeat)
                print(f"[{self.worker_id}] Heartbeat sent")
                time.sleep(5) # Send every 5 seconds
            except Exception as e:
                print(f"[{self.worker_id}] Heartbeat error: {e}")

    def consume_tasks(self):
        """Main consumer loop"""
        print(f"[{self.worker_id}] Starting to consume tasks...")
        for message in self.consumer:
            if not self.running:
                break
            
            try:
                task = message.value
                jobid = task['jobid']         # <-- corrected field name
                tileid = task['tileid']       # <-- corrected field name
                tiledata = task['tiledata']   # <-- corrected field name
                
                print(f"[{self.worker_id}] Processing job={jobid}, tile={tileid}")
                
                # Process the tile
                start_time = time.time()
                processed_data = self.process_tile(tiledata)
                processing_time = time.time() - start_time
                
                if processed_data:
                    # Send result back
                    result = {
                        'jobid': jobid,                       # <-- corrected
                        'tileid': tileid,                     # <-- corrected
                        'workerid': self.worker_id,           # <-- corrected
                        'tiledata': processed_data,           # <-- corrected
                        'processingtime': processing_time,    # <-- corrected
                        'timestamp': time.time()
                    }
                    self.producer.send('results', value=result)
                    print(f"[{self.worker_id}] Completed tile {tileid} in {processing_time:.2f}s")
            
            except Exception as e:
                print(f"[{self.worker_id}] Error processing message: {e}")

    def start(self):
        """Start worker threads"""
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()
        
        # Start consuming tasks (main thread)
        try:
            self.consume_tasks()
        except KeyboardInterrupt:
            print(f"\n[{self.worker_id}] Shutting down...")
        finally:
            self.running = False
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    worker = ImageWorker(WORKER_ID, KAFKA_BROKER)
    worker.start()