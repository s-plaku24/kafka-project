import time
import random
from datetime import datetime
from quixstreams import Application

def main():
    print("Starting producer...")
    
    app = Application(broker_address="localhost:9092")
    topic = app.topic(name="sensor")
    
    with app.get_producer() as producer:
        t = 0
        while True:
            temperature = 20 + random.uniform(-5, 15)
            
            message = {
                "timestamp": datetime.now().isoformat(),
                "device_id": "sensor-01",
                "temperature": round(temperature, 2)
            }
            
            kafka_msg = topic.serialize(key="sensor-01", value=message)
            producer.produce(
                topic=topic.name,
                key=kafka_msg.key,
                value=kafka_msg.value,
            )
            
            print(f"Sent: {message}")
            time.sleep(1)
            t += 1

if __name__ == "__main__":
    main()