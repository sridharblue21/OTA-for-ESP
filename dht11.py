#Python script to transfer data from mqtt to InfluxDB

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import datetime
import os
import sys
import config

measurement = "dht11"
location = "office"
username = config.USR
password = config.PWD
brokerID = "192.168.0.101"

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))  

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("/esp8266/dhtreadings/dht11")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = (msg.payload) 
    final = data.decode('utf-8')
    temp,hum = final.split(',') 
    temperature = float(temp) 
    humidity = float(hum)
    json_body = [
    {
        "measurement": measurement,
        "tags": {
            "location": location,
        },
        "time": str(current_time),
        "fields": {
            #"value": final,
            "temperature": temperature,
            "humidity": humidity,
        }
    }
    ]
    influx_client.write_points(json_body)

    print("STARTS HERE: ")
    print(temperature)
    print(humidity)

influx_client = InfluxDBClient(brokerID, 8086, database='dht11db') 
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username,password)
client.connect(brokerID,1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
