using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Project88.Message.Producer;

namespace Project88
{
    public static class Project88MessageProducer
    {
        private const string eventHubConnectionString = "Endpoint=sb://project-elk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ahfjt7PUE0YrDiACvk7WfNLUoq7fYdpUM+VAkgEa21A=";
        private const string eventHubName = "logs";

        [FunctionName("Project88MessageProducer")] 
        public static void Run([BlobTrigger("skirmishes/{name}", Connection = "AzureWebJobsStorage")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            // TODO: Extract data
            // TODO: Formulate data into event hub message
            // TODO:  // Send the messages to the event hub:
            IAzureEventHubRepository eventHubRepository = new AzureEventHubRepository(eventHubConnectionString, eventHubName);
        }
    }
}
