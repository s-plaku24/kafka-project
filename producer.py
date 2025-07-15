import time
import logging
import math
import random
from datetime import datetime
from quixstreams import Application

def get_sensor_measurement(t, device_id="machine-01", frequency=0.05, noise_std=2, outlier_prob = 0.05):
    """Simulates a temperature measurement for an IoT sensor at time t"""
    # create a noisy sin function
    base_temp = 22 + 15 * math.sin(2 * math.pi * frequency * t)
    noise = random.gauss(0, noise_std)
    temp = base_temp + noise
    # Add sometimes some random uutlier
    if random.random() < outlier_prob:
        temp += random.choice([20, 50])

    # Create a message as a dictonary (will be later translated into a JSON)
    message = {
        "timestamp": datetime.now().isoformat(),
        "device_id": device_id,
        "temperature": round(temp, 2)
    }
    return message

def main():
    print("Starting producer...")
    print("Setting up logging...")
    logging.basicConfig(level=logging.INFO)
    
    try:
        print("Connecting to Kafka at localhost:9092...")
        app = Application(broker_address="localhost:9092")
        topic = app.topic(name="sensor")
        print("Connected to Kafka successfully!")
        print("Starting to produce messages...")
        
        t = 0
        with app.get_producer() as producer:
            print("Producer created, starting message loop...")
            while True:
                measurement = get_sensor_measurement(t)
                print(f"Generated measurement: {measurement}")
                
                kafka_msg = topic.serialize(key=measurement["device_id"], value=measurement)
                producer.produce(
                    topic=topic.name,
                    key=kafka_msg.key,
                    value=kafka_msg.value,
                )
                print(f"Produced message {t}. Sleeping for 1 second...")
                time.sleep(1)
                t = t + 1
                
    except KeyboardInterrupt:
        print("Producer stopped by user")
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()