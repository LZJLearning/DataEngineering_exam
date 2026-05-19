#!/bin/bash
API_URL="http://127.0.0.1:5000/api/v1"

echo " AeroSense API Integration Tests"

echo -e "\n1. Testing GET /health"
curl -s -X GET "$API_URL/health"

echo -e "\n\n2. Testing GET /sensors"
curl -s -X GET "$API_URL/sensors"

echo -e "\n\n3. Testing GET /sensors/temperature/latest"
curl -s -X GET "$API_URL/sensors/temperature/latest"

echo -e "\n\n4. Testing GET /sensors/temperature/stats?days=7"
curl -s -X GET "$API_URL/sensors/temperature/stats?days=7"

echo -e "\n\n5. Testing GET /anomalies?sensor=temperature&limit=3"
curl -s -X GET "$API_URL/anomalies?sensor=temperature&limit=3"

echo -e "\n\n6. Testing POST /readings"
curl -s -X POST "$API_URL/readings" \
     -H "Content-Type: application/json" \
     -d '{"sensor": "pressure", "value": 1050.5}'

echo -e "\n"
echo " Tests Completed!"