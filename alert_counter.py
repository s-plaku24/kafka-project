from quixstreams import Application

def main():
    print("Starting alert counter...")
    
    app = Application(
        broker_address="localhost:9092",
        consumer_group="alert_counter",
        auto_offset_reset="latest",
    )
    
    input_topic = app.topic("alert", value_deserializer="json")
    output_topic = app.topic("alert_count", value_serializer="json")
    
    sdf = app.dataframe(input_topic)
    
    # Count alerts in 5-second windows
    sdf = sdf.tumbling_window(duration_ms=5000).count().current()
    
    def format_count(result):
        return {
            "timestamp": result["end"],
            "alert_count": result["value"]
        }
    
    sdf = sdf.apply(format_count)
    sdf = sdf.print()
    sdf.to_topic(output_topic)
    
    app.run(sdf)

if __name__ == "__main__":
    main()