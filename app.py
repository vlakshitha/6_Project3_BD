from flask import Flask, render_template, request, send_file, jsonify
from master import ImageMaster
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
RESULT_FOLDER = 'results'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)

KAFKA_BROKER = '172.30.234.114:9092'
master = ImageMaster(KAFKA_BROKER)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_image():
    if 'image' not in request.files:
        logging.warning("No file in upload request")
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['image']
    if not file or file.filename == '':
        logging.warning("Empty file in upload request")
        return jsonify({'error': 'No file selected'}), 400
    path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(path)
    jobid = master.submit_job(path)
    logging.info(f"Job {jobid} submitted for {file.filename}")
    return jsonify({'job_id': jobid, 'message': 'Job submitted successfully'})

@app.route('/status/<jobid>')
def job_status(jobid):
    status = master.get_job_status(jobid)
    print(f"Debug status [{jobid}]: {status}")
    if not status:
        logging.warning(f"Status request for unknown jobid: {jobid}")
        return jsonify({'error': 'Job not found'}), 404
    n = status['tiles_done']
    total = status['tiles_total']
    completed = status['completed']
    elapsed = status.get('elapsed_seconds', 0)
    status_out = {
        'progress': f"{n}/{total}",
        'status': "completed" if completed else "processing",
        'elapsed': f"{elapsed:.1f}s"
    }
    return jsonify(status_out)

@app.route('/result/<jobid>')
def get_result(jobid):
    status = master.get_job_status(jobid)
    if not status or not status['completed']:
        logging.warning(f"Result request for incomplete job: {jobid}")
        return jsonify({'error': 'Job not completed yet'}), 400
    result_image = master.reconstruct_image(jobid)
    if result_image:
        result_path = os.path.join(RESULT_FOLDER, f'{jobid}.png')
        result_image.save(result_path)
        logging.info(f"Result image saved: {result_path}")
        return send_file(result_path, mimetype='image/png')
    logging.error(f"Failed to reconstruct image for job: {jobid}")
    return jsonify({'error': 'Failed to reconstruct image'}), 500

@app.route('/workers')
def workers():
    """Get active workers from heartbeat monitor"""
    active_workers = master.get_workers_status()
    return jsonify({
        'workers': active_workers,
        'count': len(active_workers),
        'timestamp': __import__('time').time()
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
