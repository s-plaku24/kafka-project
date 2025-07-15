from quixstreams import Application

def main():
    print("Starting consumer...")
    
    app = Application(
        broker_address="localhost:9092",
        consumer_group="alert_consumer",
        auto_offset_reset="latest",
    )
    
    input_topic = app.topic("sensor", value_deserializer="json")
    output_topic = app.topic("alert", value_serializer="json")
    
    sdf = app.dataframe(input_topic)
    
    # Filter for high temperatures (> 30°C)
    def is_high_temp(msg):
        return msg["temperature"] > 30
    
    sdf = sdf.filter(is_high_temp)
    sdf = sdf.print(metadata=True)
    sdf.to_topic(output_topic)
    
    app.run(sdf)

if __name__ == "__main__":
    main()