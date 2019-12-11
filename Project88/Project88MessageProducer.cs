using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace Project88
{
    public static class Project88MessageProducer
    {
        [FunctionName("Project88MessageProducer")] 
        public static void Run([BlobTrigger("skirmishes/{name}", Connection = "AzureWebJobsStorage")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            // TODO: Extract data
            // TODO: Formulate data into event hub message
            // TODO: write to event hub.
        }
    }
}
