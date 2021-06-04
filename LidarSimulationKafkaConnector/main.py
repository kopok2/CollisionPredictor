"""
Unity3D Lidar Simulation - Kafka connector.
"""
import json
from kafka import KafkaProducer
from fastapi import FastAPI, Request

app = FastAPI()
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=["localhost:9092"])
processed_points = set()


@app.post("/")
async def root(request: Request):
    """Root point POST endpoint."""
    form = await request.form()
    hash_payload = hash(form['payload'])
    if hash_payload in processed_points:
        return {"success": True}
    else:
        processed_points.add(hash_payload)
        payload = eval(form['payload'])
        print(f"Processed {len(payload)} points.")
        producer.send('point-cloud', {'points': [list(point) for point in payload]})
        return {"success": True}
