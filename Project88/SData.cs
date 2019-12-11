using Newtonsoft.Json;
using System.IO;

namespace Project88.Message.Producer
{
    public class SData
    {
        public SData(Stream streamData)
        {
            // Convert the stream data into text and then deserialize it (under the assumption that it is JSON) to POCOs:
            using (StreamReader reader = new StreamReader(streamData))
            {
                string dataText = reader.ReadToEnd();
                dynamic data = JsonConvert.DeserializeObject<object>(dataText);
            }
        }
    }
}
