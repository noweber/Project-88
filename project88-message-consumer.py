from azure.eventhub import EventHubClient, Receiver, Offset
from elasticsearch import Elasticsearch
from datetime import datetime
import dateutil.parser
import argparse
import json
import random
import datetime
import uuid
import time

# Receive input parameters from command line arguments:
prog = "Azure Event Hub Message Consumer"
desc = "Consumes messages from Azure Event Hub"
parser = argparse.ArgumentParser(prog=prog, description=desc)
parser.add_argument('--event_hub_address', '-address', default="amqps://project-88-messaging-tier.servicebus.windows.net/skirmish-parties", type=str, required=False)
parser.add_argument('--event_hub_username', '-user', default="RootManageSharedAccessKey", type=str, required=False)
parser.add_argument('--event_hub_password', '-password', default="T7s4JSiRwnxHSKtIh/FogNMQLwbVocIxSrvqklni090=", type=str, required=False)
parser.add_argument('--event_hub_consumer_group', '-consume_group', default="$Default", type=str, required=False)
parser.add_argument('--event_hub_message_polling_interval_in_seconds', '-polling_interval_seconds', default=10, type=int, required=False)
parser.add_argument('--elasticsearch_index_name', '-es_index', default="skirmish-parties", type=str, required=False)

# Parse the input parameters:
parsed_args = parser.parse_args()
event_hub_address = parsed_args.event_hub_address
event_hub_username = parsed_args.event_hub_username
event_hub_password = parsed_args.event_hub_password
event_hub_consumer_group = parsed_args.event_hub_consumer_group
event_hub_message_polling_interval_in_seconds = parsed_args.event_hub_message_polling_interval_in_seconds
elasticsearch_index_name = parsed_args.elasticsearch_index_name


# TODO: Take in arguments from the command line

# Create an Event Hubs client
client = EventHubClient(event_hub_address, debug=False, username=event_hub_username, password=event_hub_password)

try:
    # Add a receiver to the client and run it:
    receiver = client.add_receiver(event_hub_consumer_group, partition=0, auto_reconnect=True)
    client.run()

    # TODO: Create an Elasticsearch client and post events to it idempotently:
    elastic = Elasticsearch()
    
    while True:
        for event_data in receiver.receive(timeout=100):
            event_json = "{}".format(event_data.body_as_str(encoding='UTF-8'))

            # Format the data dictionary types for reserialization:
            event_dictionary = json.loads(event_json)
            event_datetime = datetime.datetime.strptime(event_dictionary["Timestamp"], "%m/%d/%Y %H:%M:%S")
            event_dictionary["Timestamp"] = event_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            for key in event_dictionary:
                if " Count" in key:
                    event_dictionary[key] = int(event_dictionary[key])
                if "Vict" in key:
                    if "True" in event_dictionary[key]:
                        event_dictionary[key] = True
                    elif "False" in event_dictionary[key]:
                        event_dictionary[key] = False

            # Reserialize the dictionary now that types are accounted for:
            event_json = json.dumps(event_dictionary)
            #print(event_json)
            
            # Create a unique id for this event so that re-writes will be idempotent:
            event_id_base = event_dictionary["Timestamp"] + event_dictionary["SkirmishIdentifier"] + event_dictionary["PartyIdentifier"] 
            event_id = ''.join(str(ord(ascii_character)) for ascii_character in event_id_base)

            elastic.index(index=elasticsearch_index_name, body=event_json, id=event_id)
        
        # Poll for new messages every X seconds
        time.sleep(event_hub_message_polling_interval_in_seconds)
    client.stop()
except KeyboardInterrupt:
    pass
finally:
    client.stop()