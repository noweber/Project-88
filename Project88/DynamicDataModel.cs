using Newtonsoft.Json;
using System.IO;

namespace Project88.Message.Producer
{
    public class DynamicDataModel
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
