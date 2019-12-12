using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Project88
{
    public static class Project88MessageProducer
    {
        // NOTE: This event hub connection string is used only for development purposes. In production, this value will be pushed to a configuration JSON file and its value will be written during the CI/CD pipeline.
        private static EventHubClient eventHubClient;
        private const string eventHubConnectionString = "Endpoint=sb://project-88-messaging-tier.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=T7s4JSiRwnxHSKtIh/FogNMQLwbVocIxSrvqklni090=";
        private const string eventHubEntityPath = "skirmish-parties";

        /// <summary>
        /// This is the main entry point of the serverless Azure Function.
        /// The trigger point of this function is a new BLOB being created within the "skirmishes" container.
        /// Note that skirmish is just another name for battle. They are a normal JRPG style battle, but have a limited number of turns.
        /// The skirmishes container is the data source of this data processing pipeline.
        /// Entries to the data source are created by a .NET core web application which is the backend to a Unity 3D game frontend.
        /// The main point of this function is to read a BLOB file and extract a subset of the data to push through the processing pipeline.
        /// This function writes the subset of the data as messages to Azure Event Hubs which will be consumed later by a Kafka consumer.
        /// </summary>
        /// <param name="myBlob">This is the BLOB data stream which was created within the skirmishes container.</param>
        /// <param name="name">This is the name of the BLOB file which was created.</param>
        /// <param name="log">This is a logging dependency used to write to the Azure Function monitoring.</param>
        [FunctionName("Project88MessageProducer")]
        public static void Run([BlobTrigger("skirmishes/{name}", Connection = "AzureWebJobsStorage")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            MainAsync(new DynamicDataModel(myBlob)).GetAwaiter().GetResult();
            log.LogInformation("Messages have been published to Azure Event Hubs.");
        }

        private static async Task MainAsync(DynamicDataModel skirmishData)
        {
            // Make dictionarys of the skirmish data for each party:
            string skirmishIdentifier = skirmishData.Data.SkirmishIdentifier;
            string victoriousPartyIdentifier = skirmishData.Data.VictoriousPartyIdentifier;
            string skirmishTimestamp = skirmishData.Data.Timestamp;
            string attackingPartySerializedJson = GetSerializedJsonOfSkirmishPartyData(skirmishIdentifier, victoriousPartyIdentifier, skirmishTimestamp, skirmishData.Data.AttackingParty, true);
            string defendingPartySerializedJson = GetSerializedJsonOfSkirmishPartyData(skirmishIdentifier, victoriousPartyIdentifier, skirmishTimestamp, skirmishData.Data.DefendingParty, false);

            // Send the serialized JSON data as messages to Azure Event Hub:
            EventHubsConnectionStringBuilder connectionStringBuilder = new EventHubsConnectionStringBuilder(eventHubConnectionString)
            {
                EntityPath = eventHubEntityPath
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
            await PublishEventMessage(attackingPartySerializedJson);
            await PublishEventMessage(defendingPartySerializedJson);
            await eventHubClient.CloseAsync();
        }

        /// <summary>
        /// Publishes a message to Azure Event Hubs using the connection string and topic from the object's constructor.
        /// </summary>
        /// <param name="message">The message which will be published to Azure Event Hubs.</param>
        public static async Task PublishEventMessage(string message)
        {
            await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
        }

        /// <summary>
        /// This function takes in data related to the skirmish being processed and returns serialized JSON of the "skirmish-party" message.
        /// The message will then be written to Azure Event Hub.
        /// </summary>
        /// <param name="skirmishIdentifier">the GUID which identifies the skirmish</param>
        /// <param name="victoriousPartyIdentifier">the GUID which identifies the party which won the skirmish</param>
        /// <param name="skirmishTimestamp">the timestamp of the skirmish</param>
        /// <param name="PartyData">a dynamic data object of the party for which a skirmish party message is being created for</param>
        /// <returns>
        /// This method returns a serialized JSON object with the following fields:
        /// SkirmishIdentifier: the GUID which identifies the skirmish
        /// PartyIdentifier: the GUID which identifies the party (a set of characters) in the skirmish
        /// Timestamp: the time of the skirmish
        /// Victorious: whether the identifier party won or lost (a skirmish party message will be created for both the winner and loser)
        /// 'Class' Count: the number of each class (Mage, Warrior, Cleric, Rogue) which was present in the skirmish.
        /// </returns>
        private static string GetSerializedJsonOfSkirmishPartyData(string skirmishIdentifier, string victoriousPartyIdentifier, string skirmishTimestamp, dynamic PartyData, bool isAttacker)
        {
            Dictionary<string, string> partyDataDictionary = CreateSkirmishPartyDataDictionaryWithMetaData(skirmishIdentifier, victoriousPartyIdentifier, skirmishTimestamp, PartyData);
            if(isAttacker)
            {
                partyDataDictionary["Role"] = "Attacker";
            } else
            {
                partyDataDictionary["Role"] = "Defender";
            }
            return JsonConvert.SerializeObject(partyDataDictionary);
        }

        /// <summary>
        /// This function creates and returns a dictionary of the skirmish party data.
        /// </summary>
        /// <param name="skirmishIdentifier">the GUID which identifies the skirmish</param>
        /// <param name="victoriousPartyIdentifier">the GUID which identifies the party which won the skirmish</param>
        /// <param name="skirmishTimestamp">the timestamp of the skirmish</param>
        /// <param name="PartyData">a dynamic data object of the party for which a skirmish party message is being created for</param>
        /// <returns> Returns a dictionary with party data and meta data related to the skirmish for aggregations. </returns>
        private static Dictionary<string, string> CreateSkirmishPartyDataDictionaryWithMetaData(string skirmishIdentifier, string victoriousPartyIdentifier, string skirmishTimestamp, dynamic PartyData)
        {
            Dictionary<string, string> skirmishDataDictionary = CreateDataDictionaryOfPartyData(PartyData);
            skirmishDataDictionary["SkirmishIdentifier"] = skirmishIdentifier;
            skirmishDataDictionary["Timestamp"] = skirmishTimestamp;
            skirmishDataDictionary["Victorious"] = "False";
            if (string.Equals(skirmishDataDictionary["PartyIdentifier"], victoriousPartyIdentifier))
            {
                skirmishDataDictionary["Victorious"] = "True";
            }
            return skirmishDataDictionary;
        }

        /// <summary>
        /// This function takes in a dynamic object created from parsing the BLOB JSON and extracting the section related to one of the parties and then created a dictionary which is a subset of that data.
        /// </summary>
        /// <param name="PartyData">a dynamic data object of the party for which a skirmish party message is being created for</param>
        /// <returns>Returns a dictionary of the party data from a dynamic object created from the BLOB JSON.</returns>
        private static Dictionary<string, string> CreateDataDictionaryOfPartyData(dynamic partyData)
        {
            Dictionary<string, string> skirmishPartyDataDictionary = new Dictionary<string, string>
            {
                ["PartyIdentifier"] = partyData.Identifier
            };
            foreach (var character in partyData.Characters)
            {
                string className = character.Character.CharacterClass.Name;
                string classCountKey = className + " Count";
                if (skirmishPartyDataDictionary.ContainsKey(classCountKey))
                {
                    int currentCount;
                    Int32.TryParse(skirmishPartyDataDictionary[classCountKey], out currentCount);
                    currentCount += 1;
                    skirmishPartyDataDictionary[classCountKey] = currentCount.ToString();
                }
                else
                {
                    skirmishPartyDataDictionary[classCountKey] = "1";
                }
            }
            return skirmishPartyDataDictionary;
        }


        internal class DynamicDataModel
        {
            public dynamic Data { get; private set; }

            public DynamicDataModel(Stream streamData)
            {
                // Convert the stream data into text and then deserialize it (under the assumption that it is JSON) to a dynamic object:
                using (StreamReader reader = new StreamReader(streamData))
                {
                    string dataText = reader.ReadToEnd();
                    this.Data = JsonConvert.DeserializeObject<object>(dataText);
                }
            }
        }
    }
}
