using System.Threading.Tasks;

using MassTransit;

using Microsoft.Extensions.Logging;

namespace GettingStarted
{
    public class Message
    {
        public string Text { get; set; }
    }

    public interface Message2
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
            if (context.ReceiveContext.TryGetCloudEventEnvelope(out var envelope))
                _logger.LogInformation("Received Text: {Text}. Sent By User: {UserId}", context.Message.Text, envelope.GetUserId());
            else
                _logger.LogInformation("Received Text: {Text}", context.Message.Text);

            return Task.CompletedTask;
        }
    }

    public class Message2Consumer :
        IConsumer<Message2>
    {
        readonly ILogger<MessageConsumer> _logger;

        public Message2Consumer(ILogger<MessageConsumer> logger)
        {
            _logger = logger;
        }

        public Task Consume(ConsumeContext<Message2> context)
        {
            if (context.ReceiveContext is CloudEventReceiveContext receiveContext)
                _logger.LogInformation("MESSAGE 2: Received Text: {Text}. Sent By User: {UserId}", context.Message.Text, receiveContext.Envelope.GetUserId());
            else
                _logger.LogInformation("MESSAGE 2: Received Text: {Text}", context.Message.Text);

            return Task.CompletedTask;
        }
    }
}