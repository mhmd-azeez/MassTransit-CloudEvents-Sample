using System.Threading.Tasks;

using MassTransit;

using Microsoft.Extensions.Logging;

namespace GettingStarted
{
    public class Message
    {
        public string Text { get; set; }
    }

    public class MessageConsumer :
        IConsumer<Message>
    {
        readonly ILogger<MessageConsumer> _logger;

        public MessageConsumer(ILogger<MessageConsumer> logger)
        {
            _logger = logger;
        }

        public Task Consume(ConsumeContext<Message> context)
        {
            if (context.ReceiveContext is CloudEventReceiveContext receiveContext)
                _logger.LogInformation("Received Text: {Text}. Sent By User: {UserId}", context.Message.Text, receiveContext.Envelope.GetUserId());
            else
                _logger.LogInformation("Received Text: {Text}", context.Message.Text);

            return Task.CompletedTask;
        }
    }
}