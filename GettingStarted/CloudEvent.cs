using MassTransit;
using System.Net.Mime;
using GreenPipes;
using System.IO;
using Newtonsoft.Json;
using MassTransit.Serialization;
using System;
using System.Runtime.Serialization;
using Newtonsoft.Json.Serialization;
using MassTransit.Serialization.JsonConverters;
using System.Collections.Generic;
using MassTransit.Context;
using System.Linq;
using CloudNative.CloudEvents;
using Newtonsoft.Json.Linq;
using System.Diagnostics;

namespace GettingStarted
{
    //public static class UserId
    //{
    //    public static CloudEventAttribute UserId =>
    //        CloudEventAttribute.CreateExtension("userId", CloudEventAttributeType.String);

    //}

    // FROM: https://github.com/jbw/TooBigToFailBurgerShop/blob/develop/src/services/Ordering/Ordering.StateService/Application/Extensions/Dapr/CloudEventMessageEnvelope.cs

    public class CloudEventConsumeContext : DeserializerConsumeContext
    {
        private readonly IReadOnlyDictionary<string, Type> _typeMap;
        private CloudEvent _cloudEvent;

        public CloudEventConsumeContext(ReceiveContext receiveContext, IReadOnlyDictionary<string, Type> typeMap, CloudEvent cloudEvent)
            : base(receiveContext)
        {
            _typeMap = typeMap;
            _cloudEvent = cloudEvent;
            MessageId = receiveContext.TransportHeaders.Get<Guid>(nameof(MessageContext.MessageId));
        }

        public override Guid? MessageId { get; } = default;

        public override Guid? RequestId => new Guid(_cloudEvent.Id);

        public override Guid? CorrelationId { get; } = default;

        public override Guid? ConversationId { get; } = default;

        public override Guid? InitiatorId { get; } = default;

        public override DateTime? ExpirationTime => default;

        public override Uri SourceAddress => _cloudEvent.Source;

        public override Uri DestinationAddress { get; } = default;

        public override Uri ResponseAddress { get; } = default;

        public override Uri FaultAddress { get; } = default;

        public override DateTime? SentTime => _cloudEvent.Time;

        public override Headers Headers => NoMessageHeaders.Instance;

        public override HostInfo Host => default;
        public override IEnumerable<string> SupportedMessageTypes => Enumerable.Empty<string>();

        private Type GetMessageType()
        {
            return _typeMap[_cloudEvent.Type];
        }

        public override bool TryGetMessage<T>(out ConsumeContext<T> consumeContext)
        {
            consumeContext = null;
            try
            {
                var dataObject = ((JToken)_cloudEvent.Data).ToObject(GetMessageType());

                if (dataObject is T msg)
                {
                    consumeContext = new MessageConsumeContext<T>(this, msg);
                    return consumeContext != null;
                };

                return false;
            }
            catch (Exception e)
            {
                Trace.TraceError("Error Deserializing CloudEvent {ErrorMessage}\n Stack:{stack}", e.Message,
                                 e.StackTrace);

                return false;
            }
        }

        public override bool HasMessageType(Type messageType)
        {
            return GetMessageType() == messageType;
        }
    }

    public class CloudEventDeserializer : IMessageDeserializer
    {
        public CloudEventDeserializer(IReadOnlyDictionary<string, Type> typeMap)
        {
            _typeMap = typeMap.ToDictionary(p => p.Key, p => p.Value);

            _typeMap[nameof(MassTransit.Events.ReceiveFaultEvent)] = typeof(MassTransit.Events.ReceiveFaultEvent);
        }

        private readonly Dictionary<string, Type> _typeMap;

        public void Probe(ProbeContext context)
        {
            context.CreateScope(CloudEvent.MediaType.Split("/")[1]).Add("contentType", CloudEvent.MediaType);
        }

        public ConsumeContext Deserialize(ReceiveContext receiveContext)
        {
            var cloudFormatter = new JsonEventFormatter();

            using var stream = receiveContext.GetBodyStream();
            using var streamReader = new StreamReader(stream);
            using var reader = new JsonTextReader(streamReader);

            var cloudEvent = cloudFormatter.DecodeStructuredEvent(receiveContext.GetBodyStream());
            return new CloudEventConsumeContext(receiveContext, _typeMap, cloudEvent);
        }

        public ContentType ContentType => new ContentType(CloudEvent.MediaType);
    }

    public class CloudEventSerializer :
        IMessageSerializer
    {
        private Dictionary<Type, string> _typeMap;
        private Uri _source;

        public CloudEventSerializer(string source, IReadOnlyDictionary<string, Type> typeMap)
        {
            _typeMap = typeMap
                .ToDictionary(p => p.Value, p => p.Key);

            _typeMap[typeof(MassTransit.Events.ReceiveFaultEvent)] = nameof(MassTransit.Events.ReceiveFaultEvent);
            _source = new Uri(source);
        }

        public void Serialize<T>(Stream stream, SendContext<T> context)
            where T : class
        {
            try
            {
                context.ContentType = new ContentType(CloudEvent.MediaType);

                var messageType = _typeMap[context.Message.GetType()];
                var envelope = new CloudEvent(messageType, _source, context.MessageId?.ToString(), context.SentTime, DistributedTracing);
                envelope.Data = context.Message;
                envelope.DataContentType = new ContentType("application/json");

                var cloudFormatter = new JsonEventFormatter();
                var bytes = cloudFormatter.EncodeStructuredEvent(envelope, out var contentType);
                using var memoryStream = new MemoryStream(bytes);
                memoryStream.CopyTo(stream);
            }
            catch (SerializationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new SerializationException("Failed to serialize message", ex);
            }
        }

        private static readonly ContentType _contentType = new ContentType(CloudEvent.MediaType);

        public ContentType ContentType => _contentType;
    }
}
