namespace Project88.Message.Producer
{
    public interface IAzureEventHubRepository
    {
        void PublishEventMessage(string message);
    }
}
