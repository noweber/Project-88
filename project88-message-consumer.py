from azure.eventhub import EventHubClient, Receiver, Offset
from elasticsearch import Elasticsearch
from datetime import datetime
import json
import random
import datetime
import uuid
import time

# TODO: Take in arguments from the command line

# Address can be in either of these formats:
# "amqps://<URL-encoded-SAS-policy>:<URL-encoded-SAS-key>@<mynamespace>.servicebus.windows.net/myeventhub"
# "amqps://<mynamespace>.servicebus.windows.net/myeventhub"
# For example:
ADDRESS = "amqps://project-88-messaging-tier.servicebus.windows.net/skirmish-parties"

# SAS policy and key are not required if they are encoded in the URL
USER = "RootManageSharedAccessKey"
KEY = "T7s4JSiRwnxHSKtIh/FogNMQLwbVocIxSrvqklni090="

# Create an Event Hubs client
client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)

# Add a receiver to the client
CONSUMER_GROUP = "$Default"

try:
    receiver = client.add_receiver(CONSUMER_GROUP, partition=0, auto_reconnect=True)
    client.run()
    while True:
        for event_data in receiver.receive(timeout=100):
            print("Received: {}".format(event_data.body_as_str(encoding='UTF-8')))
        time.sleep(2)
    client.stop()

except KeyboardInterrupt:
    pass
finally:
    client.stop()