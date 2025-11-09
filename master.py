from kafka import KafkaProducer, KafkaConsumer
from PIL import Image
import io
import json
import base64
import time
import uuid
import threading
import logging
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('master.log')
    ]
)

KAFKA_BROKER = '172.30.234.114:9092'

class ImageMaster:
    def __init__(self, kafkabroker):
        self.kafkabroker = kafkabroker
        self.producer = KafkaProducer(
            bootstrap_servers=kafkabroker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3
        )
        self.consumer = KafkaConsumer(
            'results',
            bootstrap_servers=kafkabroker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=None,
            enable_auto_commit=False
        )
        self.heartbeat_consumer = KafkaConsumer(
            'heartbeats',
            bootstrap_servers=kafkabroker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id=None,
            enable_auto_commit=False
        )
        self.jobs = {}
        self.workers = {}  # Track worker heartbeats
        self.lock = threading.Lock()
        self.running = True
        self.unknown_jobids = set()
        
        threading.Thread(target=self.consume_results, daemon=True).start()
        threading.Thread(target=self.consume_heartbeats, daemon=True).start()
        logging.info("ImageMaster initialized")

    def split_image(self, image_path, tile_size=512):
        """Split image into tiles"""
        image = Image.open(image_path)
        width, height = image.size
        tiles = []
        positions = []
        
        for y in range(0, height, tile_size):
            for x in range(0, width, tile_size):
                box = (x, y, min(x + tile_size, width), min(y + tile_size, height))
                tile = image.crop(box)
                buffer = io.BytesIO()
                tile.save(buffer, format='PNG')
                tiledata = base64.b64encode(buffer.getvalue()).decode('utf-8')
                tiles.append(tiledata)
                positions.append((x, y))
        
        logging.info(f"Split image {image_path} into {len(tiles)} tiles")
        return tiles, positions, (width, height)

    def submit_job(self, image_path):
        """Submit job and send tiles to Kafka"""
        jobid = str(uuid.uuid4())
        tiles, positions, original_size = self.split_image(image_path)
        
        with self.lock:
            self.jobs[jobid] = {
                'status': 'processing',
                'total_tiles': len(tiles),
                'completed_tiles': 0,
                'tile_positions': positions,
                'original_size': original_size,
                'results': {},
                'start_time': time.time()
            }
        
        for tileid, tiledata in enumerate(tiles):
            task = {
                'jobid': jobid,
                'tileid': tileid,
                'tiledata': tiledata
            }
            self.producer.send('tasks', value=task)
        
        self.producer.flush()
        time.sleep(0.2)
        
        logging.info(f"Job {jobid} submitted with {len(tiles)} tiles")
        return jobid

    def consume_results(self):
        """Consume processed tiles from workers"""
        logging.info("Starting result consumer thread")
        
        while self.running:
            try:
                for message in self.consumer:
                    try:
                        result = message.value
                        jobid = result.get('jobid')
                        tileid = result.get('tileid')
                        tiledata = result.get('tiledata')
                        
                        print(f"[MASTER] Received result: jobid={jobid}, tileid={tileid}")
                        logging.info(f"Received result: jobid={jobid}, tileid={tileid}")
                        
                        with self.lock:
                            if jobid in self.jobs:
                                job = self.jobs[jobid]
                                job['results'][tileid] = tiledata
                                job['completed_tiles'] += 1
                                print(f"[MASTER] Updated: jobid={jobid}, completed_tiles={job['completed_tiles']}/{job['total_tiles']}")
                                logging.info(f"Updated: jobid={jobid}, completed_tiles={job['completed_tiles']}/{job['total_tiles']}")
                                
                                if job['completed_tiles'] == job['total_tiles']:
                                    job['status'] = 'completed'
                                    elapsed = time.time() - job['start_time']
                                    print(f"[MASTER] JOB COMPLETED: jobid={jobid} in {elapsed:.2f}s")
                                    logging.info(f"JOB COMPLETED: jobid={jobid} in {elapsed:.2f}s")
                            else:
                                if jobid not in self.unknown_jobids:
                                    self.unknown_jobids.add(jobid)
                                    print(f"[MASTER] WARNING: jobid {jobid} not found (likely stale)")
                                    logging.warning(f"Received result for unknown jobid: {jobid}")
                    except Exception as e:
                        logging.error(f"Error processing result: {e}")
                        continue
            except Exception as e:
                logging.error(f"Consumer error: {e}. Reconnecting...")
                time.sleep(5)

    def consume_heartbeats(self):
        """Consume worker heartbeats"""
        logging.info("Starting heartbeat consumer thread")
        
        while self.running:
            try:
                for message in self.heartbeat_consumer:
                    try:
                        heartbeat = message.value
                        workerid = heartbeat.get('workerid')
                        hostname = heartbeat.get('hostname', 'unknown')
                        timestamp = heartbeat.get('timestamp', time.time())
                        
                        with self.lock:
                            self.workers[workerid] = {
                                'hostname': hostname,
                                'lastseen': timestamp,
                                'status': 'active'
                            }
                        logging.info(f"Heartbeat from worker {workerid} on {hostname}")
                    except Exception as e:
                        logging.error(f"Error processing heartbeat: {e}")
                        continue
            except Exception as e:
                logging.error(f"Heartbeat consumer error: {e}. Reconnecting...")
                time.sleep(5)

    def get_workers_status(self):
        """Get active workers (seen in last 30 seconds)"""
        current_time = time.time()
        with self.lock:
            active_workers = []
            for workerid, info in self.workers.items():
                if current_time - info['lastseen'] < 30:  # Active if seen in last 30 seconds
                    active_workers.append({
                        'workerid': workerid,
                        'hostname': info['hostname'],
                        'lastseen': info['lastseen'],
                        'status': 'active'
                    })
            return active_workers

    def reconstruct_image(self, jobid):
        """Reconstruct image from processed tiles"""
        with self.lock:
            job = self.jobs.get(jobid)
            if not job or job['status'] != 'completed':
                return None
            
            width, height = job['original_size']
            
            try:
                first_tile_data = next(iter(job['results'].values()))
                tile_bytes = base64.b64decode(first_tile_data)
                first_tile = Image.open(io.BytesIO(tile_bytes))
                mode = first_tile.mode
                logging.info(f"Detected image mode: {mode}")
            except Exception as e:
                logging.warning(f"Could not detect image mode, defaulting to L: {e}")
                mode = 'L'
            
            result_image = Image.new(mode, (width, height))
            
            for tileid, tiledata in job['results'].items():
                try:
                    tile_bytes = base64.b64decode(tiledata)
                    tile = Image.open(io.BytesIO(tile_bytes))
                    x, y = job['tile_positions'][tileid]
                    result_image.paste(tile, (x, y))
                except Exception as e:
                    logging.error(f"Error reconstructing tile {tileid}: {e}")
            
            logging.info(f"Image reconstructed for jobid={jobid}")
            return result_image

    def get_job_status(self, jobid):
        """Get current job status"""
        with self.lock:
            job = self.jobs.get(jobid)
            if not job:
                return None
            
            elapsed = time.time() - job['start_time']
            
            if elapsed > 60 and job['completed_tiles'] < job['total_tiles']:
                logging.warning(f"Job {jobid} stuck at {job['completed_tiles']}/{job['total_tiles']} for {elapsed:.1f}s")
            
            return {
                'tiles_done': job['completed_tiles'],
                'tiles_total': job['total_tiles'],
                'completed': job['status'] == 'completed',
                'elapsed_seconds': elapsed
            }
