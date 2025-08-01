#!/usr/bin/env python3
"""
Unified Salvium Pool Service
- Collects data from all pool nodes every 30 seconds
- Stores historical data in Redis
- Serves API endpoints for the web interface
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import redis
import json
import time
import threading
from datetime import datetime, timedelta
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
REDIS_HOST = 'core.supportsal.com'
REDIS_PORT = 6379
REDIS_DB = 0
API_PORT = 5000
TTL = 259200  # 3 days in seconds

# Pool nodes configuration
POOL_NODES = [
    'http://core.supportsal.com:4243',    # Upstream - has aggregated data
    'http://node1.supportsal.com:4243',   # Downstream - has live miner data
]

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize Redis connection
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    r.ping()
    print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"Failed to connect to Redis: {e}")
    exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for data collection
collection_thread = None
stop_collection = threading.Event()

def fetch_node_stats(node_url, address=None, timeout=5):
    """Fetch stats from a single pool node"""
    try:
        headers = {}
        if address:
            headers['Cookie'] = f'wa={address}'
        
        response = requests.get(f'{node_url}/stats', headers=headers, timeout=timeout)
        if response.status_code == 200:
            data = response.json()
            data['node_url'] = node_url
            return data
        else:
            logger.warning(f"Node {node_url} returned status {response.status_code}")
            return None
    except Exception as e:
        logger.warning(f"Failed to fetch from {node_url}: {e}")
        return None

def fetch_node_workers(node_url, address, timeout=5):
    """Fetch workers from a single pool node"""
    try:
        headers = {'Cookie': f'wa={address}'}
        response = requests.get(f'{node_url}/workers', headers=headers, timeout=timeout)
        if response.status_code == 200:
            workers = response.json()
            return workers
        else:
            return []
    except Exception as e:
        logger.warning(f"Failed to fetch workers from {node_url}: {e}")
        return []

def store_timeseries_data(key, value):
    """Store data point in Redis sorted set with TTL"""
    timestamp = int(time.time())
    
    # Store the data point
    r.zadd(key, {f"{timestamp}:{value}": timestamp})
    
    # Set TTL on the sorted set
    r.expire(key, TTL)
    
    # Clean old entries (keep last 3 days)
    cutoff = timestamp - TTL
    r.zremrangebyscore(key, "-inf", cutoff)

def store_miner_hashrate(address, hashrate):
    """Store miner hashrate data for charting"""
    timestamp = int(time.time())
    key = f'miner:{address}:hashrate'
    
    # Store the data point
    r.zadd(key, {f"{timestamp}:{hashrate}": timestamp})
    
    # Set TTL on the sorted set
    r.expire(key, TTL)
    
    # Clean old entries (keep last 3 days)
    cutoff = timestamp - TTL
    r.zremrangebyscore(key, "-inf", cutoff)

def store_worker_hashrate(address, worker_name, hashrate):
    """Store individual worker hashrate data for charting"""
    timestamp = int(time.time())
    # Clean worker name for use as Redis key
    safe_worker_name = worker_name.replace(':', '_').replace(' ', '_')
    key = f'worker:{address}:{safe_worker_name}:hashrate'
    
    # Store the data point
    r.zadd(key, {f"{timestamp}:{hashrate}": timestamp})
    
    # Set TTL on the sorted set
    r.expire(key, TTL)
    
    # Clean old entries (keep last 3 days)
    cutoff = timestamp - TTL
    r.zremrangebyscore(key, "-inf", cutoff)

def collect_pool_data():
    """Collect and store pool data from all nodes"""
    logger.info("Collecting pool data from all nodes...")
    
    # Collect from each node
    node_stats = []
    upstream_node = None
    
    with ThreadPoolExecutor(max_workers=len(POOL_NODES)) as executor:
        future_to_node = {
            executor.submit(fetch_node_stats, node): node 
            for node in POOL_NODES
        }
        
        for future in as_completed(future_to_node):
            node_url = future_to_node[future]
            try:
                stats = future.result()
                if stats:
                    node_stats.append(stats)
                    
                    # Identify upstream node
                    if 'core.supportsal.com' in node_url:
                        upstream_node = stats
                        
            except Exception as e:
                logger.error(f"Error collecting from {node_url}: {e}")
    
    if not node_stats:
        logger.error("No node data collected")
        return
    
    # Use upstream node data as authoritative (it has aggregated totals)
    if upstream_node:
        pool_data = upstream_node
        logger.info(f"Using upstream data: hr={pool_data.get('pool_hashrate', 0)}, miners={pool_data.get('connected_miners', 0)}")
    else:
        # Fallback: aggregate if no upstream
        pool_data = aggregate_node_stats(node_stats)
        logger.info("No upstream node, aggregating manually")
    
    # Store pool stats in Redis
    store_timeseries_data('pool:hashrate', pool_data.get('pool_hashrate', 0))
    store_timeseries_data('network:hashrate', pool_data.get('network_hashrate', 0))
    store_timeseries_data('network:difficulty', pool_data.get('network_difficulty', 0))
    store_timeseries_data('network:height', pool_data.get('network_height', 0))
    store_timeseries_data('pool:miners', pool_data.get('connected_miners', 0))
    store_timeseries_data('pool:blocks', pool_data.get('pool_blocks_found', 0))
    store_timeseries_data('pool:round_hashes', pool_data.get('round_hashes', 0))
    
    # Calculate and store effort
    if pool_data.get('round_hashes') and pool_data.get('network_difficulty'):
        effort = (pool_data['round_hashes'] / pool_data['network_difficulty']) * 100
        store_timeseries_data('pool:effort', effort)
    
    # Store latest stats JSON for fast API access
    enhanced_stats = pool_data.copy()
    enhanced_stats.update({
        'timestamp': int(time.time()),
        'active_nodes': len(node_stats),
        'total_nodes': len(POOL_NODES),
        'nodes_info': [
            {
                'url': node.get('node_url', ''),
                'hashrate': node.get('pool_hashrate', 0),
                'miners': node.get('connected_miners', 0)
            }
            for node in node_stats
        ]
    })
    
    r.setex('pool:latest_stats', 300, json.dumps(enhanced_stats))
    
    logger.info(f"Stored pool data: hr={pool_data.get('pool_hashrate', 0)}, miners={pool_data.get('connected_miners', 0)}")

def aggregate_node_stats(node_stats):
    """Aggregate stats from multiple nodes (fallback if no upstream)"""
    if not node_stats:
        return {}
    
    base = node_stats[0]
    return {
        'pool_hashrate': sum(n.get('pool_hashrate', 0) for n in node_stats),
        'network_hashrate': base.get('network_hashrate', 0),
        'network_difficulty': base.get('network_difficulty', 0),
        'network_height': base.get('network_height', 0),
        'connected_miners': sum(n.get('connected_miners', 0) for n in node_stats),
        'pool_blocks_found': sum(n.get('pool_blocks_found', 0) for n in node_stats),
        'round_hashes': sum(n.get('round_hashes', 0) for n in node_stats),
        'last_template_fetched': max(n.get('last_template_fetched', 0) for n in node_stats),
        'last_block_found': max(n.get('last_block_found', 0) for n in node_stats),
        'payment_threshold': base.get('payment_threshold', 0.1),
        'pool_fee': base.get('pool_fee', 0.007),
        'pool_port': base.get('pool_port', 4242),
        'pool_ssl_port': base.get('pool_ssl_port', 4343),
        'allow_self_select': base.get('allow_self_select', 1)
    }

def data_collection_worker():
    """Background thread that collects data every 30 seconds"""
    logger.info("Starting data collection worker...")
    
    while not stop_collection.is_set():
        try:
            collect_pool_data()
        except Exception as e:
            logger.error(f"Error in data collection: {e}")
        
        # Wait 30 seconds or until stop signal
        if stop_collection.wait(30):
            break
    
    logger.info("Data collection worker stopped")

def start_data_collection():
    """Start the background data collection thread"""
    global collection_thread
    
    if collection_thread and collection_thread.is_alive():
        return
    
    stop_collection.clear()
    collection_thread = threading.Thread(target=data_collection_worker, daemon=True)
    collection_thread.start()
    logger.info("Data collection thread started")

def stop_data_collection():
    """Stop the background data collection thread"""
    global collection_thread
    
    stop_collection.set()
    if collection_thread and collection_thread.is_alive():
        collection_thread.join(timeout=5)
    logger.info("Data collection thread stopped")

# Utility functions
def parse_redis_timeseries(data):
    """Parse Redis sorted set data into timestamps and values"""
    result = []
    for item in data:
        try:
            timestamp_str, value_str = item.split(':', 1)
            timestamp = int(timestamp_str)
            value = float(value_str)
            result.append([timestamp, value])
        except (ValueError, IndexError):
            continue
    return result

def get_time_range(hours=8):
    """Get timestamp range for the last N hours"""
    now = int(time.time())
    start = now - (hours * 3600)
    return start, now

# API Endpoints
@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        r.ping()
        
        # Check pool nodes
        responding_nodes = 0
        for node in POOL_NODES:
            try:
                response = requests.get(f'{node}/stats', timeout=2)
                if response.status_code == 200:
                    responding_nodes += 1
            except:
                pass
        
        return jsonify({
            "status": "healthy", 
            "redis": "connected",
            "data_collection": "active" if collection_thread and collection_thread.is_alive() else "inactive",
            "pool_nodes": {
                "total": len(POOL_NODES),
                "responding": responding_nodes,
                "nodes": POOL_NODES
            }
        })
    except:
        return jsonify({"status": "unhealthy", "redis": "disconnected"}), 500

@app.route('/api/stats')
def get_current_stats():
    """Get current pool statistics or miner-specific stats"""
    try:
        address = request.args.get('address') or request.cookies.get('wa')
        
        if address:
            # Miner-specific request - query nodes in real-time
            return get_miner_stats(address)
        else:
            # Pool stats - use cached Redis data
            latest_stats = r.get('pool:latest_stats')
            if latest_stats:
                stats_data = json.loads(latest_stats)
                stats_data['source'] = 'redis_cache'
                return jsonify(stats_data)
            else:
                return jsonify({"error": "No pool stats available"}), 503
                
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return jsonify({"error": "Failed to fetch stats"}), 500

def get_miner_stats(address):
    """Get miner-specific stats by querying nodes in real-time"""
    try:
        # Get pool stats from cache
        pool_data = {}
        try:
            latest_stats = r.get('pool:latest_stats')
            if latest_stats:
                pool_data = json.loads(latest_stats)
        except:
            pass
        
        # Query nodes for miner data
        total_miner_hashrate = 0
        total_miner_balance = 0
        total_paid_balance = 0
        downstream_worker_count = 0
        miner_hr_stats = [0, 0, 0, 0, 0, 0]
        found_on_nodes = []
        
        logger.info(f"Querying nodes for miner {address[:16]}...")
        
        with ThreadPoolExecutor(max_workers=len(POOL_NODES)) as executor:
            future_to_node = {
                executor.submit(fetch_node_stats, node, address): node 
                for node in POOL_NODES
            }
            
            for future in as_completed(future_to_node):
                node_url = future_to_node[future]
                try:
                    stats = future.result()
                    if stats:
                        logger.info(f"Node {node_url}: hr={stats.get('miner_hashrate', 0)}, workers={stats.get('worker_count', 0)}")
                        
                        if (stats.get('miner_hashrate', 0) > 0 or 
                            stats.get('miner_balance', 0) > 0 or 
                            stats.get('worker_count', 0) > 0):
                            
                            total_miner_hashrate += stats.get('miner_hashrate', 0)
                            total_miner_balance += stats.get('miner_balance', 0)
                            total_paid_balance += stats.get('miner_paid', 0)  # Add paid balance
                            
                            # Only count workers from downstream nodes (not core)
                            if 'core.supportsal.com' not in node_url:
                                downstream_worker_count += stats.get('worker_count', 0)
                                logger.info(f"Using worker count from downstream {node_url}: {stats.get('worker_count', 0)}")
                            else:
                                logger.info(f"Ignoring worker count from upstream {node_url}: {stats.get('worker_count', 0)}")
                            
                            # Aggregate hashrate stats
                            node_hr_stats = stats.get('miner_hashrate_stats', [0, 0, 0, 0, 0, 0])
                            for i in range(min(len(miner_hr_stats), len(node_hr_stats))):
                                miner_hr_stats[i] += node_hr_stats[i]
                            
                            found_on_nodes.append(node_url)
                        
                except Exception as e:
                    logger.error(f"Error fetching miner stats from {node_url}: {e}")
        
        # Store miner hashrate for charting
        if total_miner_hashrate > 0:
            store_miner_hashrate(address, total_miner_hashrate)
        
        logger.info(f"Miner results: hr={total_miner_hashrate}, workers={downstream_worker_count}")
        
        if total_miner_hashrate == 0 and total_miner_balance == 0 and downstream_worker_count == 0:
            return jsonify({"error": "Miner not found"}), 404
        
        # Combine pool and miner data
        result = pool_data.copy()
        result.update({
            "miner_hashrate": total_miner_hashrate,
            "miner_balance": total_miner_balance,
            "miner_paid": total_paid_balance,  # Add paid balance to response
            "worker_count": downstream_worker_count,  # Use only downstream count
            "miner_hashrate_stats": miner_hr_stats,
            "found_on_nodes": found_on_nodes,
            "source": "live_miner_query"
        })
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error fetching miner stats: {e}")
        return jsonify({"error": "Failed to fetch miner stats"}), 500

@app.route('/api/workers')
def get_workers():
    """Get workers for an address from downstream nodes"""
    try:
        address = request.args.get('address') or request.cookies.get('wa')
        
        if not address:
            return jsonify({"error": "No address provided"}), 400
        
        logger.info(f"Fetching workers for address {address[:16]}...")
        
        all_workers = []
        downstream_nodes = [node for node in POOL_NODES if 'core.supportsal.com' not in node]
        
        logger.info(f"Checking downstream nodes: {downstream_nodes}")
        
        with ThreadPoolExecutor(max_workers=len(downstream_nodes)) as executor:
            future_to_node = {
                executor.submit(fetch_node_workers, node, address): node 
                for node in downstream_nodes
            }
            
            for future in as_completed(future_to_node):
                node_url = future_to_node[future]
                try:
                    workers = future.result()
                    logger.info(f"Workers from {node_url}: {workers}")
                    
                    if workers and isinstance(workers, list):
                        # The C API returns a flat list of name-value pairs:
                        # ['name1', value1, 'name2', value2, 'name3', value3, ...]
                        
                        # Process pairs
                        for i in range(0, len(workers), 2):
                            if i + 1 < len(workers):
                                worker_name = workers[i] if workers[i] else f"Worker-{i//2 + 1}"
                                worker_value = workers[i + 1] if isinstance(workers[i + 1], (int, float)) else 0
                                
                                # Store worker hashrate for charting
                                if worker_value > 0:
                                    store_worker_hashrate(address, worker_name, worker_value)
                                
                                # The value could be hashrate or shares, let's assume it's hashrate for now
                                worker_obj = {
                                    "identifier": worker_name,
                                    "name": worker_name,
                                    "hashrate": worker_value,
                                    "shares": 0,  # We don't have shares data in this format
                                    "timestamp": int(time.time()),
                                    "node": node_url
                                }
                                all_workers.append(worker_obj)
                                logger.info(f"Added worker: {worker_name} with value: {worker_value}")
                                
                except Exception as e:
                    logger.error(f"Error fetching workers from {node_url}: {e}")
        
        logger.info(f"Total workers found: {len(all_workers)}")
        return jsonify(all_workers)
        
    except Exception as e:
        logger.error(f"Error fetching workers: {e}")
        return jsonify({"error": "Failed to fetch workers"}), 500

@app.route('/api/charts')
def get_chart_data():
    """Get chart data from Redis"""
    try:
        hours = int(request.args.get('hours', 8))
        start_time, end_time = get_time_range(hours)
        chart_type = request.args.get('type', 'pool')
        address = request.args.get('address', '')
        worker = request.args.get('worker', '')
        
        chart_data = {}
        
        if chart_type in ['all', 'network']:
            network_data = r.zrangebyscore('network:hashrate', start_time, end_time)
            chart_data['network_hashrate'] = parse_redis_timeseries(network_data)
            
        if chart_type in ['all', 'pool']:
            pool_data = r.zrangebyscore('pool:hashrate', start_time, end_time)
            chart_data['pool_hashrate'] = parse_redis_timeseries(pool_data)
            
        if chart_type in ['all', 'miner'] and address:
            # Get miner hashrate data from Redis
            miner_key = f'miner:{address}:hashrate'
            miner_data = r.zrangebyscore(miner_key, start_time, end_time)
            chart_data['miner_hashrate'] = parse_redis_timeseries(miner_data)
            
        if chart_type == 'worker' and address and worker:
            # Get individual worker hashrate data
            safe_worker_name = worker.replace(':', '_').replace(' ', '_')
            worker_key = f'worker:{address}:{safe_worker_name}:hashrate'
            worker_data = r.zrangebyscore(worker_key, start_time, end_time)
            chart_data['worker_hashrate'] = parse_redis_timeseries(worker_data)
        
        return jsonify(chart_data)
        
    except Exception as e:
        logger.error(f"Error fetching chart data: {e}")
        return jsonify({"error": "Failed to fetch chart data"}), 500

@app.route('/api/blocks')
def get_blocks_history():
    """Get blocks history"""
    try:
        # For now, return empty array since we haven't found blocks yet
        return jsonify([])
        
    except Exception as e:
        logger.error(f"Error fetching blocks data: {e}")
        return jsonify([])

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    print(f"Starting Unified Salvium Pool Service on port {API_PORT}")
    print(f"Pool nodes: {POOL_NODES}")
    print(f"Data collection: Every 30 seconds")
    print(f"Redis storage: {REDIS_HOST}:{REDIS_PORT}")
    
    # Start data collection
    start_data_collection()
    
    # Collect initial data
    collect_pool_data()
    
    print(f"Endpoints available:")
    print(f"  GET /api/health - Health check")
    print(f"  GET /api/stats - Pool stats (cached) or miner stats (live)")
    print(f"  GET /api/workers - Workers from downstream nodes")
    print(f"  GET /api/charts - Chart data from Redis")
    print(f"  GET /api/blocks - Blocks history")
    
    try:
        app.run(host='127.0.0.1', port=API_PORT, debug=False, threaded=True)
    finally:
        stop_data_collection()

