#!/usr/bin/env python3
"""
Salvium Pool Data API Server
Serves historical data from Redis for the pool web interface
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import redis
import json
import time
from datetime import datetime, timedelta
import logging

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
API_PORT = 5000

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

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

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        r.ping()
        return jsonify({"status": "healthy", "redis": "connected"})
    except:
        return jsonify({"status": "unhealthy", "redis": "disconnected"}), 500

@app.route('/api/stats')
def get_current_stats():
    """Get current pool statistics (mimics the original pool API)"""
    try:
        # Get latest raw stats from Redis
        latest_stats = r.get('pool:latest_stats')
        
        if latest_stats:
            stats_data = json.loads(latest_stats)
            
            # Add any additional computed fields
            stats_data['timestamp'] = int(time.time())
            
            return jsonify(stats_data)
        else:
            return jsonify({"error": "No current stats available"}), 404
            
    except Exception as e:
        logger.error(f"Error fetching current stats: {e}")
        return jsonify({"error": "Failed to fetch stats"}), 500

@app.route('/api/charts')
def get_chart_data():
    """Get chart data for the specified time range"""
    try:
        # Get time range (default 8 hours)
        hours = int(request.args.get('hours', 8))
        start_time, end_time = get_time_range(hours)
        
        # Get chart type
        chart_type = request.args.get('type', 'all')  # all, pool, network, miner
        address = request.args.get('address', '')
        
        chart_data = {}
        
        if chart_type in ['all', 'network']:
            # Network hashrate
            network_data = r.zrangebyscore('network:hashrate', start_time, end_time)
            chart_data['network_hashrate'] = parse_redis_timeseries(network_data)
            
            # Network difficulty
            difficulty_data = r.zrangebyscore('network:difficulty', start_time, end_time)
            chart_data['network_difficulty'] = parse_redis_timeseries(difficulty_data)
        
        if chart_type in ['all', 'pool']:
            # Pool hashrate
            pool_data = r.zrangebyscore('pool:hashrate', start_time, end_time)
            chart_data['pool_hashrate'] = parse_redis_timeseries(pool_data)
            
            # Pool effort
            effort_data = r.zrangebyscore('pool:effort', start_time, end_time)
            chart_data['pool_effort'] = parse_redis_timeseries(effort_data)
            
            # Pool miners
            miners_data = r.zrangebyscore('pool:miners', start_time, end_time)
            chart_data['pool_miners'] = parse_redis_timeseries(miners_data)
        
        if chart_type in ['all', 'miner'] and address:
            # Miner hashrate
            miner_data = r.zrangebyscore(f'miner:{address}:hashrate', start_time, end_time)
            chart_data['miner_hashrate'] = parse_redis_timeseries(miner_data)
            
            # Miner balance
            balance_data = r.zrangebyscore(f'miner:{address}:balance', start_time, end_time)
            chart_data['miner_balance'] = parse_redis_timeseries(balance_data)
        
        return jsonify(chart_data)
        
    except Exception as e:
        logger.error(f"Error fetching chart data: {e}")
        return jsonify({"error": "Failed to fetch chart data"}), 500

@app.route('/api/blocks')
def get_blocks_history():
    """Get simplified blocks history"""
    try:
        # Get the blocks found count over time
        hours = int(request.args.get('hours', 24))
        start_time, end_time = get_time_range(hours)
        
        blocks_data = r.zrangebyscore('pool:blocks', start_time, end_time)
        blocks_timeline = parse_redis_timeseries(blocks_data)
        
        # Calculate blocks found in this period
        current_blocks = blocks_timeline[-1][1] if blocks_timeline else 0
        start_blocks = blocks_timeline[0][1] if blocks_timeline else 0
        blocks_found = int(current_blocks - start_blocks)
        
        return jsonify({
            "blocks_found": blocks_found,
            "timeline": blocks_timeline,
            "period_hours": hours
        })
        
    except Exception as e:
        logger.error(f"Error fetching blocks data: {e}")
        return jsonify({"error": "Failed to fetch blocks data"}), 500

@app.route('/api/miner/<address>')
def get_miner_data(address):
    """Get historical data for a specific miner"""
    try:
        hours = int(request.args.get('hours', 8))
        start_time, end_time = get_time_range(hours)
        
        # Check if this address is being tracked
        if not r.sismember('tracked:addresses', address):
            # Add to tracking set
            r.sadd('tracked:addresses', address)
            logger.info(f"Added new address to tracking: {address[:12]}...")
        
        # Get miner historical data
        hashrate_data = r.zrangebyscore(f'miner:{address}:hashrate', start_time, end_time)
        balance_data = r.zrangebyscore(f'miner:{address}:balance', start_time, end_time)
        workers_data = r.zrangebyscore(f'miner:{address}:workers', start_time, end_time)
        
        # Get latest workers details
        workers_details = r.get(f'miner:{address}:workers_data')
        
        miner_data = {
            "address": address,
            "hashrate_history": parse_redis_timeseries(hashrate_data),
            "balance_history": parse_redis_timeseries(balance_data),
            "workers_history": parse_redis_timeseries(workers_data),
            "workers_details": json.loads(workers_details) if workers_details else [],
            "last_seen": r.get(f'miner:{address}:last_seen'),
            "period_hours": hours
        }
        
        return jsonify(miner_data)
        
    except Exception as e:
        logger.error(f"Error fetching miner data: {e}")
        return jsonify({"error": "Failed to fetch miner data"}), 500

@app.route('/api/tracked_addresses')
def get_tracked_addresses():
    """Get list of currently tracked addresses"""
    try:
        addresses = list(r.smembers('tracked:addresses'))
        
        # Get last seen time for each address
        address_info = []
        for addr in addresses:
            last_seen = r.get(f'miner:{addr}:last_seen')
            address_info.append({
                "address": addr,
                "last_seen": int(last_seen) if last_seen else None,
                "short_address": f"{addr[:12]}...{addr[-8:]}" if len(addr) > 20 else addr
            })
        
        # Sort by last seen time (most recent first)
        address_info.sort(key=lambda x: x['last_seen'] or 0, reverse=True)
        
        return jsonify(address_info)
        
    except Exception as e:
        logger.error(f"Error fetching tracked addresses: {e}")
        return jsonify({"error": "Failed to fetch tracked addresses"}), 500

@app.route('/api/redis_stats')
def get_redis_stats():
    """Get Redis database statistics"""
    try:
        info = r.info()
        
        # Get key counts by pattern
        key_patterns = {
            'pool_keys': len(r.keys('pool:*')),
            'network_keys': len(r.keys('network:*')),
            'miner_keys': len(r.keys('miner:*')),
            'tracked_addresses': r.scard('tracked:addresses')
        }
        
        redis_stats = {
            "connected_clients": info.get('connected_clients', 0),
            "used_memory_human": info.get('used_memory_human', '0B'),
            "total_commands_processed": info.get('total_commands_processed', 0),
            "uptime_in_seconds": info.get('uptime_in_seconds', 0),
            "key_counts": key_patterns
        }
        
        return jsonify(redis_stats)
        
    except Exception as e:
        logger.error(f"Error fetching Redis stats: {e}")
        return jsonify({"error": "Failed to fetch Redis stats"}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    print(f"Starting Salvium Pool Data API Server on port {API_PORT}")
    print(f"Endpoints available:")
    print(f"  GET /api/health - Health check")
    print(f"  GET /api/stats - Current pool stats")
    print(f"  GET /api/charts?hours=8&type=all&address= - Chart data")
    print(f"  GET /api/blocks?hours=24 - Blocks history")
    print(f"  GET /api/miner/<address>?hours=8 - Miner data")
    print(f"  GET /api/tracked_addresses - List tracked addresses")
    print(f"  GET /api/redis_stats - Redis statistics")
    
    app.run(host='0.0.0.0', port=API_PORT, debug=False)

