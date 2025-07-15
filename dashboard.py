from datetime import datetime
from quixstreams import Application
import streamlit as st
from collections import deque

temperature_buffer = deque(maxlen=100)
timestamp_buffer = deque(maxlen=100)

st.title("Real-Time IoT Dashboard")

@st.cache_resource
def kafka_connection():
    return Application(
        broker_address="localhost:9092",
        consumer_group="dashboard",
        auto_offset_reset="latest",
    )

app = kafka_connection()
sensor_topic = app.topic("sensor")
alert_topic = app.topic("alert")
alert_count_topic = app.topic("alert_count")
temp_average_topic = app.topic("temp_average")

# Create columns for metrics
col1, col2, col3 = st.columns(3)

with col1:
    st_metric_temp = st.empty()
with col2:
    st_metric_alert_count = st.empty()
with col3:
    st_metric_avg_temp = st.empty()

st_chart = st.empty()

with app.get_consumer() as consumer:
    consumer.subscribe([sensor_topic.name, alert_topic.name, alert_count_topic.name, temp_average_topic.name])
    previous_temp = 0
    current_alert_count = 0
    current_avg_temp = 0
    
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None:
            if msg.topic() == sensor_topic.name:
                sensor_msg = sensor_topic.deserialize(msg)
                temperature = sensor_msg.value.get('temperature')
                device_id = sensor_msg.value.get('device_id')
                timestamp = datetime.fromisoformat(sensor_msg.value.get('timestamp'))
                diff = temperature - previous_temp
                previous_temp = temperature
                timestamp_str = timestamp.strftime("%H:%M:%S")
                
                # Update temperature metric
                st_metric_temp.metric(
                    label=f"Current Temp ({device_id})", 
                    value=f"{temperature:.2f} °C", 
                    delta=f"{diff:.2f} °C"
                )
                
                # Update chart
                timestamp_buffer.append(timestamp_str)
                temperature_buffer.append(temperature)
                st_chart.line_chart(
                    data={
                        "time": list(timestamp_buffer),
                        "temperature": list(temperature_buffer)
                    },
                    x="time",
                    y="temperature",
                    use_container_width=True,
                )
                
            elif msg.topic() == alert_count_topic.name:
                alert_count_msg = alert_count_topic.deserialize(msg)
                current_alert_count = alert_count_msg.value.get('alert_count', 0)
                st_metric_alert_count.metric(
                    label="Alerts (5s window)", 
                    value=f"{current_alert_count}",
                    delta=None
                )
                
            elif msg.topic() == temp_average_topic.name:
                avg_temp_msg = temp_average_topic.deserialize(msg)
                new_avg_temp = avg_temp_msg.value.get('avg_temperature', 0)
                diff_avg = new_avg_temp - current_avg_temp
                current_avg_temp = new_avg_temp
                st_metric_avg_temp.metric(
                    label="Avg Temp (10s window)", 
                    value=f"{current_avg_temp:.2f} °C",
                    delta=f"{diff_avg:.2f} °C"
                )