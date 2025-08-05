#!/usr/bin/env python3
"""
Unified Salvium Pool Service - Fixed for new Go API format
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
    'http://node2.supportsal.com:4243',   # Downstream - has live miner data
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
        if address:
            # For miner stats, append address to URL
            response = requests.get(f'{node_url}/stats/{address}', timeout=timeout)
        else:
            # For pool stats
            response = requests.get(f'{node_url}/stats', timeout=timeout)
            
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
        response = requests.get(f'{node_url}/workers/{address}/', timeout=timeout)
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
        logger.info(f"Using upstream data: hr={pool_data.get('pool_hr', 0)}, miners={pool_data.get('connected_addresses', 0)}")
    else:
        # Fallback: use first node
        pool_data = node_stats[0]
        logger.info("No upstream node, using first node data")
    
    # Map the new API field names to our expected names
    mapped_data = {
        'pool_hashrate': pool_data.get('pool_hr', 0),
        'network_hashrate': pool_data.get('net_hr', 0),
        'network_difficulty': pool_data.get('network_difficulty', pool_data.get('net_hr', 0) * 120),  # Use actual if available
        'network_height': pool_data.get('height', 0),
        'connected_miners': pool_data.get('connected_addresses', 0),
        'connected_workers': pool_data.get('connected_workers', 0),
        'pool_blocks_found': pool_data.get('num_blocks_found', 0),
        'round_hashes': pool_data.get('round_hashes', 0),  # Now available from fixed API
        'last_block_found': pool_data.get('last_block', {}).get('timestamp', 0),
        'last_block_height': pool_data.get('last_block', {}).get('height', 0),
        'last_block_hash': pool_data.get('last_block', {}).get('hash', ''),
        'payment_threshold': pool_data.get('payment_threshold', 0.1),
        'pool_fee': pool_data.get('pool_fee_percent', 0.7) / 100,  # Convert percentage to decimal
        'pool_port': 4242,  # Hardcoded as not in API
        'pool_ssl_port': 4343,  # Hardcoded as not in API
        'allow_self_select': 1,  # Hardcoded as not in API
        'recent_blocks': pool_data.get('recent_blocks_found', []),
        'chart': pool_data.get('chart', {}),
        'pplns_window_seconds': pool_data.get('pplns_window_seconds', 21600),
        'block_reward': pool_data.get('reward', 0),
        'effort': pool_data.get('effort', 0)  # Now available from fixed API
    }
    
    # Store pool stats in Redis
    store_timeseries_data('pool:hashrate', mapped_data.get('pool_hashrate', 0))
    store_timeseries_data('network:hashrate', mapped_data.get('network_hashrate', 0))
    store_timeseries_data('network:difficulty', mapped_data.get('network_difficulty', 0))
    store_timeseries_data('network:height', mapped_data.get('network_height', 0))
    store_timeseries_data('pool:miners', mapped_data.get('connected_miners', 0))
    store_timeseries_data('pool:workers', mapped_data.get('connected_workers', 0))
    store_timeseries_data('pool:blocks', mapped_data.get('pool_blocks_found', 0))
    store_timeseries_data('pool:effort', mapped_data.get('effort', 0))
    
    # Store latest stats JSON for fast API access
    enhanced_stats = mapped_data.copy()
    enhanced_stats.update({
        'timestamp': int(time.time()),
        'active_nodes': len(node_stats),
        'total_nodes': len(POOL_NODES),
        'nodes_info': [
            {
                'url': node.get('node_url', ''),
                'hashrate': node.get('pool_hr', 0),
                'miners': node.get('connected_addresses', 0),
                'workers': node.get('connected_workers', 0)
            }
            for node in node_stats
        ]
    })
    
    r.setex('pool:latest_stats', 300, json.dumps(enhanced_stats))
    
    logger.info(f"Stored pool data: hr={mapped_data.get('pool_hashrate', 0)}, miners={mapped_data.get('connected_miners', 0)}, workers={mapped_data.get('connected_workers', 0)}, effort={mapped_data.get('effort', 0)}%")

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
        miner_data = None
        
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
                    if stats and stats.get('hashrate_5m', 0) > 0:
                        miner_data = stats
                        logger.info(f"Found miner on {node_url}")
                        break
                except Exception as e:
                    logger.error(f"Error fetching miner stats from {node_url}: {e}")
        
        if not miner_data:
            return jsonify({"error": "Miner not found"}), 404
        
        # Store miner hashrate for charting
        hashrate = miner_data.get('hashrate_5m', 0)
        if hashrate > 0:
            store_miner_hashrate(address, hashrate)
        
        # Map miner data to expected format
        result = pool_data.copy()
        result.update({
            "miner_hashrate": miner_data.get('hashrate_5m', 0),
            "miner_balance": miner_data.get('balance', 0),
            "miner_paid": miner_data.get('paid', 0),
            "worker_count": miner_data.get('worker_count', 0),
            "miner_hashrate_stats": [
                miner_data.get('hashrate_5m', 0),
                miner_data.get('hashrate_10m', 0),
                miner_data.get('hashrate_15m', 0),
                0, 0, 0  # Padding for compatibility
            ],
            "est_pending": miner_data.get('est_pending', 0),
            "balance_pending": miner_data.get('balance_pending', 0),
            "withdrawals": miner_data.get('withdrawals', []),
            "hr_chart": miner_data.get('hr_chart', []),
            "source": "live_miner_query"
        })
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error fetching miner stats: {e}")
        return jsonify({"error": "Failed to fetch miner stats"}), 500

@app.route('/api/workers')
def get_workers():
    """Get workers for an address"""
    try:
        address = request.args.get('address') or request.cookies.get('wa')
        
        if not address:
            return jsonify({"error": "No address provided"}), 400
        
        logger.info(f"Fetching workers for address {address[:16]}...")
        
        all_workers = []
        
        with ThreadPoolExecutor(max_workers=len(POOL_NODES)) as executor:
            future_to_node = {
                executor.submit(fetch_node_workers, node, address): node 
                for node in POOL_NODES
            }
            
            for future in as_completed(future_to_node):
                node_url = future_to_node[future]
                try:
                    workers = future.result()
                    if workers and isinstance(workers, list):
                        # Add node info to each worker
                        for worker in workers:
                            worker['node'] = node_url
                            # Store worker hashrate for charting
                            if worker.get('hashrate', 0) > 0:
                                store_worker_hashrate(address, worker.get('worker_id', 'unknown'), worker['hashrate'])
                        all_workers.extend(workers)
                        
                except Exception as e:
                    logger.error(f"Error fetching workers from {node_url}: {e}")
        
        # Deduplicate workers by worker_id (keep the one with highest hashrate)
        worker_dict = {}
        for worker in all_workers:
            worker_id = worker.get('worker_id', 'unknown')
            if worker_id not in worker_dict or worker.get('hashrate', 0) > worker_dict[worker_id].get('hashrate', 0):
                worker_dict[worker_id] = worker
        
        unique_workers = list(worker_dict.values())
        
        logger.info(f"Total workers found: {len(unique_workers)}")
        return jsonify(unique_workers)
        
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
            
            # Also get miners and workers data
            miners_data = r.zrangebyscore('pool:miners', start_time, end_time)
            chart_data['pool_miners'] = parse_redis_timeseries(miners_data)
            
            workers_data = r.zrangebyscore('pool:workers', start_time, end_time)
            chart_data['pool_workers'] = parse_redis_timeseries(workers_data)
            
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
        # Get from cached pool stats
        latest_stats = r.get('pool:latest_stats')
        if latest_stats:
            stats_data = json.loads(latest_stats)
            blocks = stats_data.get('recent_blocks', [])
            return jsonify(blocks)
        else:
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
    print(f"  GET /api/workers - Workers for a miner")
    print(f"  GET /api/charts - Chart data from Redis")
    print(f"  GET /api/blocks - Blocks history")
    
    try:
        app.run(host='127.0.0.1', port=API_PORT, debug=False, threaded=True)
    finally:
        stop_data_collection()

