from quixstreams import Application

def main():
    print("Starting temperature average calculator...")
    
    app = Application(
        broker_address="localhost:9092",
        consumer_group="temp_average",
        auto_offset_reset="latest",
    )
    
    input_topic = app.topic("sensor", value_deserializer="json")
    output_topic = app.topic("temp_average", value_serializer="json")
    
    sdf = app.dataframe(input_topic)
    
    sdf = sdf.apply(lambda msg: msg["temperature"])
    
    sdf = sdf.tumbling_window(duration_ms=10000).mean().current()
    
    def format_average(result):
        return {
            "timestamp": result["end"],
            "avg_temperature": round(result["value"], 2)
        }
    
    sdf = sdf.apply(format_average)
    sdf = sdf.print()
    sdf.to_topic(output_topic)
    
    app.run(sdf)

if __name__ == "__main__":
    main()