from time import sleep
from collections import namedtuple
from dataclasses import dataclass
import paho.mqtt.client as mqtt

MQTT_BROKER = "test.mosquitto.org"

ONLINE_STRING = "Online"
OFFLINE_STRING = "OFFLINE"
UNEXPECTED_OFFLINE_STRING = "OFFLINE unexpectedly"
STATUS_TOPIC = "karri/test/status"
COUNTER_TOPIC = "karri/test/counter"
BUTTON1_TOPIC = "karri/test/button1"

#clientUserData = namedtuple('clientUserData', 'button1 connected')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    print("userdata: {}".format(userdata))
    userdata.connected = True
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(BUTTON1_TOPIC) #("$SYS/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    print("userdata: {}".format(userdata))
    userdata.button1 = str(msg.payload)


@dataclass
class Context:
    button1 : str
    connected : bool


def main():

    ctx = Context(button1="0", connected=False)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.user_data_set(ctx)
    
    client.will_set(STATUS_TOPIC, UNEXPECTED_OFFLINE_STRING, qos=1, retain=True)

    print("trying to connect")
    client.connect(MQTT_BROKER, 1883, 60)

    while not ctx.connected:
        client.loop()
    
    counter = 0
    sleep_cnt = 0
    
    try:
        while True:
            print("Publishing status and counter {c}. Button status: {b}".format(c=counter, b=ctx.button1))
            client.publish(STATUS_TOPIC, ONLINE_STRING, qos=1, retain=True)
            client.publish(COUNTER_TOPIC, counter, qos=1, retain=True)
            while sleep_cnt < 2:
                sleep(1)
                client.loop()
                sleep_cnt += 1

            sleep_cnt = 0
            counter += 1
    finally:
        client.publish(STATUS_TOPIC, OFFLINE_STRING, qos=1, retain=True)



# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
#client.loop_forever()



if __name__ == "__main__":
    main()