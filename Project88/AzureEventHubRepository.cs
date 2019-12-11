using Microsoft.Azure.EventHubs;
using System.Text;

namespace Project88.Message.Producer
{
    public class AzureEventHubRepository : IAzureEventHubRepository
    {
        private readonly EventHubClient eventHubClient;

        /// <summary>
        /// Constructs for an AzureEventHubRepository object.
        /// </summary>
        /// <param name="eventHubConnectionString">{Event Hubs connection string} which contains the credentials to connect to a specific instance of Event Hubs.</param>
        /// <param name="eventHubName">{Event Hub path/name} which is the messaging topic.</param>
        public AzureEventHubRepository(string eventHubConnectionString, string eventHubName)
        {
            EventHubsConnectionStringBuilder connectionStringBuilder = new EventHubsConnectionStringBuilder(eventHubConnectionString)
            {
                EntityPath = eventHubName,
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
        }

        /// <summary>
        /// Publishes a message to Azure Event Hubs using the connection string and topic from the object's constructor.
        /// </summary>
        /// <param name="message">The message which will be published to Azure Event Hubs.</param>
        public void PublishEventMessage(string message)
        {
            eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
        }
    }
}
