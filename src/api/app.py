from flask import Flask, jsonify, request
import time
import logging
from kafka_utils import publish_reading, get_latest_reading_from_kafka
from lake_utils import get_daily_stats, get_recent_anomalies

app = Flask(__name__)
# log
app.logger.setLevel(logging.INFO)

VALID_SENSORS = {"temperature", "humidity", "pressure"}

# Global error handling 
@app.errorhandler(400)
def bad_request(e):
    return jsonify({"status": "error", "code": 400, "message": "Bad Request: Malformed syntax"}), 400

@app.errorhandler(404)
def not_found(e):
    return jsonify({"status": "error", "code": 404, "message": "Resource not found"}), 404

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"status": "error", "code": 405, "message": "Method not allowed"}), 405

@app.errorhandler(422)
def unprocessable_entity(e):
    return jsonify({"status": "error", "code": 422, "message": str(e)}), 422

@app.errorhandler(500)
def internal_error(e):
    app.logger.error(f"Internal Server Error: {e}") # Server-side logging
    return jsonify({"status": "error", "code": 500, "message": "Internal server error"}), 500

# Endpoints
@app.route("/api/v1/health", methods=["GET"])
def health_check():
    return jsonify({"status": "ok", "service": "AeroSense API"}), 200

@app.route("/api/v1/sensors", methods=["GET"])
def list_sensors():
    return jsonify({"status": "success", "data": list(VALID_SENSORS)}), 200

@app.route("/api/v1/sensors/<type>/latest", methods=["GET"])
def latest_reading(type):
    if type not in VALID_SENSORS:
        return jsonify({"status": "error", "message": "Invalid sensor type"}), 404
        
    reading = get_latest_reading_from_kafka(type)
    if not reading:
        return jsonify({"status": "error", "message": "No recent data found in Kafka"}), 404
    return jsonify({"status": "success", "data": reading}), 200

@app.route("/api/v1/sensors/<type>/stats", methods=["GET"])
def sensor_stats(type):
    if type not in VALID_SENSORS:
        return jsonify({"status": "error", "message": "Invalid sensor type"}), 404
        
    try:
        #  days 
        days = int(request.args.get("days", 7))
        if not (1 <= days <= 90): 
            from werkzeug.exceptions import UnprocessableEntity
            raise UnprocessableEntity("Days must be between 1 and 90")
    except ValueError:
        return jsonify({"status": "error", "message": "Days must be an integer"}), 400
        
    stats = get_daily_stats(type, days)
    if stats is None:
        return jsonify({"status": "error", "message": "No stats data found for this sensor"}), 404
        
    return jsonify({"status": "success", "data": stats}), 200

@app.route("/api/v1/anomalies", methods=["GET"])
def recent_anomalies():
    sensor_type = request.args.get("sensor")
    if not sensor_type or sensor_type not in VALID_SENSORS:
        return jsonify({"status": "error", "message": "Valid sensor query parameter is required"}), 400
        
    try:
        limit = int(request.args.get("limit", 10))
    except ValueError:
        return jsonify({"status": "error", "message": "Limit must be an integer"}), 400
        
    data = get_recent_anomalies(sensor_type, limit)
    return jsonify({"status": "success", "data": data}), 200

@app.route("/api/v1/readings", methods=["POST"])
def publish_new_reading():
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"status": "error", "message": "JSON body required"}), 400
        
    # Strictly distinguish between 400 (format error) and 422 (semantic error) errors.
    sensor = body.get("sensor")
    value = body.get("value")
    
    if sensor not in VALID_SENSORS:
        from werkzeug.exceptions import UnprocessableEntity
        raise UnprocessableEntity("Invalid sensor type") 
        
    if not isinstance(value, (int, float)):
        from werkzeug.exceptions import UnprocessableEntity
        raise UnprocessableEntity("Value must be numeric")
        
    payload = {
        "sensor": sensor,
        "value": float(value),
        "unit": "C" if sensor == "temperature" else "%" if sensor == "humidity" else "hPa",
        "timestamp": int(time.time() * 1000),
        "source": "api-gateway",
        "anomaly": False
    }
    
    success = publish_reading(sensor, payload)
    if success:
        return jsonify({"status": "success", "message": "Reading published to Kafka"}), 201
    else:
        return jsonify({"status": "error", "message": "Failed to publish"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)