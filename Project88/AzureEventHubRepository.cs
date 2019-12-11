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
        /// <param name="eventHubConnectionString">{Event Hubs connection string}</param>
        /// <param name="eventHubName">{Event Hub path/name}</param>
        public AzureEventHubRepository(string eventHubConnectionString, string eventHubName)
        {
            EventHubsConnectionStringBuilder connectionStringBuilder = new EventHubsConnectionStringBuilder(eventHubConnectionString)
            {
                EntityPath = eventHubName,
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
        }

        public void PublishEventMessage(string message)
        {
            eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
        }
    }
}
