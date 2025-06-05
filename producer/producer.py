from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
from datetime import datetime

app = Flask(__name__)



cities= {
    "delhi": "north",
    "mumbai": "west",
    "kolkata": "east",
    "chennai": "south",
    "bangalore": "south",
    "hyderabad": "south",
    "ahmedabad": "west",
    "pune": "west",
    "lucknow": "north",
    "jaipur": "north",
    "kanpur": "north",
    "nagpur": "central",
    "surat": "west",
    "patna": "east",
    "indore": "west",
    "thane": "west",
    "bhopal": "central",
    "visakhapatnam": "east",
    "pimpri": "west",
    "coimbatore": "south",
    "agra": "north",
    "madurai": "south",
    "varanasi": "north",
    "meerut": "north",
    "jodhpur": "northwest",
    "gwalior": "north",
    "guwahati": "east",
    "ranchi": "east",
    "allahabad": "north",
    "jabalpur": "central",
    "mangalore": "southwest",
    "vijayawada": "south",
    "thiruvananthapuram": "south",
    "dhanbad": "east",
    "faridabad": "north",
    "asansol": "east",
    "rajkot": "west",
    "kalyan": "west",
    "vasai": "west",
    "meerut": "north",
    "jalandhar": "north",
    "belgaum": "southwest",
    "sonipat": "north",
    "bhubaneswar": "east",
    "muzzafarpur": "east",
    "moradabad": "north",
    "gurgaon": "north",
    "ghaziabad": "north",
    "amritsar": "northwest",
    "rajahmundry": "south",
    "silchar": "east",
    "tirupati": "south",
    "guntur": "south",
    "bhilai": "central",
    "jamshedpur": "east",
    "ulhasnagar": "west",
    "tiruchirappalli": "south",
    "bareilly": "north",
    "aligarh": "north",
}


@app.route('/', methods=['POST'])
def send_to_kafka():
    producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    date_time = datetime.now().strftime("%d@%m!%Y %H:%M:%S")
    data = request.get_json()
    data['date_time'] = date_time
    city = data.get("login_location")

    if city.lower() in cities:
        topic = cities[city.lower()]
        producer.send(topic, value = data)
    else:
        producer.send('other', value = data)
    producer.flush()
    producer.close()

    return jsonify({"status": "success", "topic": topic, "data": data}), 200

if __name__ == '__main__':
    app.run(host= '0.0.0.0', port = 5000)






