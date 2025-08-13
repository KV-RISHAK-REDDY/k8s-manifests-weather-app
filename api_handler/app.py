from flask import Flask, request, jsonify
import requests
import os
import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import List, Dict, Optional, Tuple
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import deque
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
try:
    WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY')
    WEATHER_API_BASE = os.environ.get('WEATHER_API_BASE')
    MAX_CONCURRENT_REQUESTS = int(os.environ.get('MAX_CONCURRENT_REQUESTS', '5'))
    REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '10'))

    # Database configuration
    DB_CONFIG = {
        'host': os.environ.get('DB_HOST'),
        'port': int(os.environ.get('DB_PORT')),
        'database': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USR'),
        'password': os.environ.get('DB_PASSWORD')
    }

except KeyError as e:
    logger.error(f"Missing environment variable: {e}")
    raise RuntimeError(f"Configuration error: {e}")

class DatabaseManager:
    """Handle all database operations"""
    
    def __init__(self):
        self.connection_pool = []
        self._init_database()
    
    # dbname: the database name
    # database: the database name (only as keyword argument)
    # user: user name used to authenticate
    # password: password used to authenticate
    # host: database host address (defaults to UNIX socket if not provided)
    # port: connection port number (defaults to 5432 if not provided)

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _init_database(self):
        """Initialize database tables"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create locations table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS locations (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        region VARCHAR(255),
                        country VARCHAR(255) NOT NULL,
                        lat DECIMAL(10, 8),
                        lon DECIMAL(11, 8),
                        tz_id VARCHAR(255),
                        localtime_epoch BIGINT,
                        localtime_string VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(name, country, region)
                    );
                """)
                
                # Create weather table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS weather (
                        id SERIAL PRIMARY KEY,
                        location_id INTEGER REFERENCES locations(id) ON DELETE CASCADE,
                        last_updated_epoch BIGINT NOT NULL,
                        last_updated VARCHAR(50) NOT NULL,
                        temp_c DECIMAL(5, 2),
                        temp_f DECIMAL(5, 2),
                        is_day INTEGER,
                        condition_text VARCHAR(255),
                        condition_icon VARCHAR(255),
                        condition_code INTEGER,
                        wind_mph DECIMAL(5, 2),
                        wind_kph DECIMAL(5, 2),
                        wind_degree INTEGER,
                        wind_dir VARCHAR(10),
                        pressure_mb DECIMAL(7, 2),
                        pressure_in DECIMAL(5, 2),
                        precip_mm DECIMAL(5, 2),
                        precip_in DECIMAL(5, 2),
                        humidity INTEGER,
                        cloud INTEGER,
                        feelslike_c DECIMAL(5, 2),
                        feelslike_f DECIMAL(5, 2),
                        vis_km DECIMAL(5, 2),
                        vis_miles DECIMAL(5, 2),
                        uv DECIMAL(3, 1),
                        gust_mph DECIMAL(5, 2),
                        gust_kph DECIMAL(5, 2),
                        raw_data JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Create indexes for better performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_weather_location_id 
                    ON weather(location_id);
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_weather_last_updated_epoch 
                    ON weather(last_updated_epoch DESC);
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_locations_name_country 
                    ON locations(name, country);
                """)
                
                conn.commit()
                logger.info("Database tables initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise
    
    def insert_or_get_location(self, location_data: Dict) -> int:
        """Insert location or get existing location ID"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Try to get existing location
                cursor.execute("""
                    SELECT id FROM locations 
                    WHERE name = %s AND country = %s AND region = %s
                """, (
                    location_data.get('name'),
                    location_data.get('country'),
                    location_data.get('region')
                ))
                
                existing = cursor.fetchone()
                if existing:
                    # Update the existing location with latest data
                    cursor.execute("""
                        UPDATE locations SET
                            lat = %s, lon = %s, tz_id = %s, 
                            localtime_epoch = %s, localtime_string = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (
                        location_data.get('lat'),
                        location_data.get('lon'),
                        location_data.get('tz_id'),
                        location_data.get('localtime_epoch'),
                        location_data.get('localtime_string'),
                        existing[0]
                    ))
                    conn.commit()
                    return existing[0]
                else:
                    # Insert new location
                    cursor.execute("""
                        INSERT INTO locations 
                        (name, region, country, lat, lon, tz_id, localtime_epoch, localtime_string)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        location_data.get('name'),
                        location_data.get('region'),
                        location_data.get('country'),
                        location_data.get('lat'),
                        location_data.get('lon'),
                        location_data.get('tz_id'),
                        location_data.get('localtime_epoch'),
                        location_data.get('localtime_string')
                    ))
                    
                    location_id = cursor.fetchone()[0]
                    conn.commit()
                    return location_id
                    
        except Exception as e:
            logger.error(f"Error inserting location: {str(e)}")
            raise
    
    def insert_weather_data(self, location_id: int, weather_data: Dict) -> int:
        """Insert weather data"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                current = weather_data.get('current', {})
                condition = current.get('condition', {})
                
                cursor.execute("""
                    INSERT INTO weather (
                        location_id, last_updated_epoch, last_updated, temp_c, temp_f,
                        is_day, condition_text, condition_icon, condition_code,
                        wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in,
                        precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f,
                        vis_km, vis_miles, uv, gust_mph, gust_kph, raw_data
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) RETURNING id
                """, (
                    location_id,
                    current.get('last_updated_epoch'),
                    current.get('last_updated'),
                    current.get('temp_c'),
                    current.get('temp_f'),
                    current.get('is_day'),
                    condition.get('text'),
                    condition.get('icon'),
                    condition.get('code'),
                    current.get('wind_mph'),
                    current.get('wind_kph'),
                    current.get('wind_degree'),
                    current.get('wind_dir'),
                    current.get('pressure_mb'),
                    current.get('pressure_in'),
                    current.get('precip_mm'),
                    current.get('precip_in'),
                    current.get('humidity'),
                    current.get('cloud'),
                    current.get('feelslike_c'),
                    current.get('feelslike_f'),
                    current.get('vis_km'),
                    current.get('vis_miles'),
                    current.get('uv'),
                    current.get('gust_mph'),
                    current.get('gust_kph'),
                    json.dumps(weather_data)
                ))
                
                weather_id = cursor.fetchone()[0]
                conn.commit()
                return weather_id
                
        except Exception as e:
            logger.error(f"Error inserting weather data: {str(e)}")
            raise
    
    def get_recent_weather_data(self, city_names: List[str], limit: int = 10) -> List[Dict]:
        """Get recent weather data for specified cities"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Create placeholders for city names
                # placeholders = ','.join(['%s'] * len(city_names))

                cursor.execute("""
                    SELECT 
                        l.name, l.region, l.country, l.lat, l.lon, l.tz_id, 
                        l.localtime_epoch, l.localtime_string,
                        w.last_updated_epoch, w.last_updated, w.temp_c, w.temp_f,
                        w.is_day, w.condition_text, w.condition_icon, w.condition_code,
                        w.wind_mph, w.wind_kph, w.wind_degree, w.wind_dir,
                        w.pressure_mb, w.pressure_in, w.precip_mm, w.precip_in,
                        w.humidity, w.cloud, w.feelslike_c, w.feelslike_f,
                        w.vis_km, w.vis_miles, w.uv, w.gust_mph, w.gust_kph,
                        w.created_at
                    FROM locations l
                    JOIN weather w ON l.id = w.location_id
                    WHERE l.name = ANY(%s)
                    ORDER BY w.last_updated_epoch DESC, w.created_at DESC
                    LIMIT %s
                """, (city_names, limit * len(city_names)))
                
                results = cursor.fetchall()
                
                # Group by city and get the most recent for each
                city_data = {}
                null_filler = lambda x: float(x) if x else None

                for row in results:
                    city_name = row['name']
                if city_name not in city_data:
                        # Reconstruct the weather API format
                        weather_item = {
                            'location': {
                                'name': row['name'],
                                'region': row['region'],
                                'country': row['country'],
                                'lat': null_filler(row['lat']),
                                'lon': null_filler(row['lon']),
                                'tz_id': row['tz_id'],
                                'localtime_epoch': row['localtime_epoch'],
                                'localtime_string': row['localtime_string']
                            },
                            'current': {
                                'last_updated_epoch': row['last_updated_epoch'],
                                'last_updated': row['last_updated'],
                                'temp_c': null_filler(row['temp_c']),
                                'temp_f': null_filler(row['temp_f']),
                                'is_day': row['is_day'],
                                'condition': {
                                    'text': row['condition_text'],
                                    'icon': row['condition_icon'],
                                    'code': row['condition_code']
                                },
                                'wind_mph': null_filler(row['wind_mph']),
                                'wind_kph': null_filler(row['wind_kph']),
                                'wind_degree': row['wind_degree'],
                                'wind_dir': row['wind_dir'],
                                'pressure_mb': null_filler(row['pressure_mb']),
                                'pressure_in': null_filler(row['pressure_in']),
                                'precip_mm': null_filler(row['precip_mm']),
                                'precip_in': null_filler(row['precip_in']),
                                'humidity': row['humidity'],
                                'cloud': row['cloud'],
                                'feelslike_c': null_filler(row['feelslike_c']),
                                'feelslike_f': null_filler(row['feelslike_f']),
                                'vis_km': null_filler(row['vis_km']),
                                'vis_miles': null_filler(row['vis_miles']),
                                'uv': null_filler(row['uv']),
                                'gust_mph': null_filler(row['gust_mph']),
                                'gust_kph': null_filler(row['gust_kph'])
                            }
                        }
                        city_data[city_name] = weather_item
                
                return list(city_data.values())
                
        except Exception as e:
            logger.error(f"Error retrieving weather data: {str(e)}")
            raise


# Rate limiting
request_lock = threading.Lock()
last_request_time = 0
MIN_REQUEST_INTERVAL = 0.1  # 100ms between requests

# Recent requests tracking - using deque for efficient operations
recent_requests = deque(maxlen=100)  # Keep last 100 request batches
recent_requests_lock = threading.Lock()

# Initialize database manager
db_manager = DatabaseManager()

def rate_limited_request(url: str, params: Dict) -> requests.Response:
    """Make a rate-limited request to avoid API limits"""
    global last_request_time
    
    with request_lock:
        current_time = time.time()
        time_since_last = current_time - last_request_time
        
        if time_since_last < MIN_REQUEST_INTERVAL:
            sleep_time = MIN_REQUEST_INTERVAL - time_since_last
            time.sleep(sleep_time)
        
        last_request_time = time.time()
    
    return requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

def get_weather_for_city(city: str) -> Tuple[str, Optional[Dict]]:
    """Get weather data for a single city and store in database"""
    try:
        logger.info(f"Fetching weather for: {city}")
        
        url = f"{WEATHER_API_BASE}/current.json"
        params = {
            'key': WEATHER_API_KEY,
            'q': city,
            'lang': 'en',
        }
        
        response = rate_limited_request(url, params)
        response.raise_for_status()
        
        data = response.json()
        
        # Validate response structure
        if 'location' not in data or 'current' not in data:
            logger.error(f"Invalid response structure for {city}: {data}")
            return city, None
        
        # Store in database
        try:
            location_id = db_manager.insert_or_get_location(data['location'])
            weather_id = db_manager.insert_weather_data(location_id, data)
            logger.info(f"Stored weather data for {city}: location_id={location_id}, weather_id={weather_id}")
        except Exception as db_error:
            logger.error(f"Database error for {city}: {str(db_error)}")
            # Don't fail the request if database fails, just log it
            
        logger.info(f"Successfully fetched weather for: {city}")
        return city, data
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout for city: {city}")
        return city, {"error": "timeout", "message": f"Request timeout for {city}"}
    
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        if status_code == 400:
            logger.error(f"City not found: {city}")
            return city, {"error": "not_found", "message": f"City '{city}' not found"}
        elif status_code == 401:
            logger.error("Invalid API key")
            return city, {"error": "auth", "message": "Invalid API key"}
        elif status_code == 403:
            logger.error("API key limit exceeded")
            return city, {"error": "limit", "message": "API request limit exceeded"}
        else:
            logger.error(f"HTTP error {status_code} for city: {city}")
            return city, {"error": "http", "message": f"HTTP {status_code} error for {city}"}
    
    except Exception as e:
        logger.error(f"Unexpected error for city {city}: {str(e)}")
        return city, {"error": "unexpected", "message": f"Unexpected error: {str(e)}"}

def process_weather_request(cities: List[str]) -> Dict:
    """Process weather request and store data"""
    logger.info(f"Processing weather request for {len(cities)} cities: {cities}")
    
    # Generate request ID for tracking
    request_id = str(uuid.uuid4())
    
    results = []
    errors = []
    successful_cities = []
    
    # Use ThreadPoolExecutor for concurrent requests
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        # Submit all tasks
        future_to_city = {
            executor.submit(get_weather_for_city, city): city 
            for city in cities
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_city):
            city = future_to_city[future]
            try:
                city_name, weather_data = future.result()
                
                if weather_data is None:
                    errors.append(f"No data received for {city_name}")
                elif "error" in weather_data:
                    errors.append(f"{city_name}: {weather_data['message']}")
                else:
                    results.append(weather_data)
                    successful_cities.append(city_name)
                    
            except Exception as e:
                logger.error(f"Error processing future for {city}: {str(e)}")
                errors.append(f"{city}: Unexpected processing error")
    
    # Store recent request for tracking
    with recent_requests_lock:
        recent_requests.append({
            'request_id': request_id,
            'cities': successful_cities,
            'timestamp': time.time(),
            'requested_at': datetime.now(timezone.utc).isoformat()
        })
    
    # Prepare response
    response = {
        "success": len(results) > 0,
        "request_id": request_id,
        "requested_cities": cities,
        "successful_cities": len(results),
        "failed_cities": len(errors),
        "errors": errors,
        "timestamp": time.time()
    }
    
    logger.info(f"Completed weather request {request_id}: {len(results)} successful, {len(errors)} failed")
    return response

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Kubernetes"""
    try:
        # Check database connection
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
        
        db_status = "healthy"
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        db_status = "unhealthy"
    
    return jsonify({
        "status": "healthy" if db_status == "healthy" else "degraded",
        "service": "weather-api-handler",
        "database": db_status,
        "timestamp": time.time()
    }), 200 if db_status == "healthy" else 503

@app.route('/process-weather', methods=['POST'])
def process_weather():
    """Main endpoint to process weather data requests"""
    try:
        # Validate request
        if not request.is_json:
            return jsonify({
                "success": False,
                "error": "Content-Type must be application/json"
            }), 400
        
        data = request.get_json()
        cities = data.get('cities', [])
        
        # Validate cities
        if not cities:
            return jsonify({
                "success": False,
                "error": "No cities provided"
            }), 400
        
        if not isinstance(cities, list):
            return jsonify({
                "success": False,
                "error": "Cities must be provided as an array"
            }), 400
        
        # Clean and validate city names
        clean_cities = []
        for city in cities:
            if isinstance(city, str) and city.strip():
                clean_cities.append(city.strip())
        
        if not clean_cities:
            return jsonify({
                "success": False,
                "error": "No valid city names provided"
            }), 400
        
        if len(clean_cities) > 20:  # Reasonable limit
            return jsonify({
                "success": False,
                "error": "Too many cities requested (max 20)"
            }), 400
        
        # # Check API key
        # if WEATHER_API_KEY == 'your-api-key-here':
        #     return jsonify({
        #         "success": False,
        #         "error": "Weather API key not configured"
        #     }), 500
        
        # Process weather request
        weather_response = process_weather_request(clean_cities)

        # Return appropriate get_weather_for_city HTTP status
        if weather_response["success"]:
            return jsonify(weather_response), 200
        else:
            return jsonify(weather_response), 500
            
    except Exception as e:
        logger.error(f"Unexpected error in process_weather endpoint: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500

@app.route('/get-recent-data', methods=['GET'])
def get_recent_data():
    """Get recent weather data for the most recent request"""
    try:
        # Get the most recent request
        with recent_requests_lock:
            if not recent_requests:
                return jsonify({
                    "success": False,
                    "error": "No recent requests found"
                }), 404
            
            latest_request = recent_requests[-1]  # Get the most recent
        
        city_names = latest_request['cities']
        if not city_names:
            return jsonify({
                "success": False,
                "error": "No successful cities in recent request"
            }), 404
        
        # Get weather data from database
        weather_data = db_manager.get_recent_weather_data(city_names)
        
        if not weather_data:
            return jsonify({
                "success": False,
                "error": "No weather data found for recent cities"
            }), 404
        
        response = {
            "success": True,
            "request_id": latest_request['request_id'],
            "data": weather_data,
            "cities_count": len(weather_data),
            "requested_at": latest_request['requested_at'],
            "retrieved_at": datetime.now(timezone.utc).isoformat()
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Error in get_recent_data: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500

@app.route('/get-data-by-cities', methods=['POST'])
def get_data_by_cities():
    """Get weather data for specific cities"""
    try:
        if not request.is_json:
            return jsonify({
                "success": False,
                "error": "Content-Type must be application/json"
            }), 400
        
        data = request.get_json()
        cities = data.get('cities', [])
        
        if not cities:
            return jsonify({
                "success": False,
                "error": "No cities provided"
            }), 400
        
        # Get weather data from database
        weather_data = db_manager.get_recent_weather_data(cities)
        
        response = {
            "success": True,
            "data": weather_data,
            "cities_count": len(weather_data),
            "retrieved_at": datetime.now(timezone.utc).isoformat()
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Error in get_data_by_cities: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500

@app.route('/recent-requests', methods=['GET'])
def get_recent_requests():
    """Get list of recent requests"""
    try:
        with recent_requests_lock:
            requests_list = list(recent_requests)
        
        return jsonify({
            "success": True,
            "requests": requests_list,
            "count": len(requests_list)
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_recent_requests: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500

@app.route('/status', methods=['GET'])
def service_status():
    """Status endpoint with service information"""
    try:
        # Check database connection
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM locations")
            locations_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM weather")
            weather_count = cursor.fetchone()[0]
        
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
        locations_count = 0
        weather_count = 0
    
    with recent_requests_lock:
        recent_requests_count = len(recent_requests)
    
    return jsonify({
        "service": "weather-api-handler",
        "version": "2.0.0",
        "status": "running",
        "database": {
            "status": db_status,
            "locations_count": locations_count,
            "weather_records_count": weather_count
        },
        "recent_requests": recent_requests_count,
        "config": {
            "max_concurrent_requests": MAX_CONCURRENT_REQUESTS,
            "request_timeout": REQUEST_TIMEOUT
        },
        "timestamp": time.time()
    }), 200

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "success": False,
        "error": "Endpoint not found"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({
        "success": False,
        "error": "Internal server error"
    }), 500

if __name__ == '__main__':
    # Validate configuration on startup
    if WEATHER_API_KEY == 'your-api-key-here':
        logger.warning("Weather API key not configured! Set WEATHER_API_KEY environment variable.")
    
    logger.info("Starting Enhanced Weather API Handler service...")
    logger.info(f"Database: {DB_CONFIG.get('host')}:{DB_CONFIG.get('port')}/{DB_CONFIG.get('database')}")
    logger.info(f"Configuration: max_concurrent={MAX_CONCURRENT_REQUESTS}, timeout={REQUEST_TIMEOUT}s")
    
    # For development
    app.run(host='0.0.0.0', port=8080)