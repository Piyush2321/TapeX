from flask import Flask, render_template, jsonify, request
import json
import os
from datetime import datetime

app = Flask(__name__)

# Configuration
TRANSFERS_FILE = 'transfers.json'

def load_transfers():
    if os.path.exists(TRANSFERS_FILE):
        with open(TRANSFERS_FILE, 'r') as f:
            return json.load(f)
    return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/transfers')
def get_transfers():
    transfers = load_transfers()
    return jsonify(transfers)

@app.route('/receive_csv', methods=['POST'])
def receive_csv():
    data = request.get_json()
    
    # Add timestamp if not provided
    if 'timestamp' not in data:
        data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Load existing transfers
    transfers = load_transfers()
    
    # Add new transfer
    transfers.append(data)
    
    # Save updated transfers
    with open(TRANSFERS_FILE, 'w') as f:
        json.dump(transfers, f, indent=2)
    
    return jsonify({'status': 'success'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True) 