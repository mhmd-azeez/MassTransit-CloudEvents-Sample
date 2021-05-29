using MassTransit;
using System.Net.Mime;
using GreenPipes;
using System.IO;
using MassTransit.Serialization;
using System;
using System.Runtime.Serialization;
using System.Collections.Generic;
using MassTransit.Context;
using System.Linq;
using CloudNative.CloudEvents;
using System.Diagnostics;
using CloudNative.CloudEvents.Core;
using CloudNative.CloudEvents.SystemTextJson;
using System.Text.Json;
using System.Buffers;
using System.Threading.Tasks;
using MassTransit.Topology;
using System.Threading;

namespace GettingStarted
{
    // https://stackoverflow.com/a/61047681/7003797
    public static partial class JsonExtensions
    {
        public static T ToObject<T>(this JsonElement element, JsonSerializerOptions options = null)
        {
            var bufferWriter = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(bufferWriter))
            {
                element.WriteTo(writer);
            }

            return JsonSerializer.Deserialize<T>(bufferWriter.WrittenSpan, options);
        }

        public static T ToObject<T>(this JsonDocument document, JsonSerializerOptions options = null)
        {
            if (document == null)
            {
                throw new ArgumentNullException(nameof(document));
            }

            return document.RootElement.ToObject<T>(options);
        }

        public static object ToObject(this JsonElement element, Type returnType, JsonSerializerOptions options = null)
        {
            var bufferWriter = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(bufferWriter))
            {
                element.WriteTo(writer);
            }

            return JsonSerializer.Deserialize(bufferWriter.WrittenSpan, returnType, options);
        }

        public static object ToObject(this JsonDocument document, Type returnType, JsonSerializerOptions options = null)
        {
            if (document == null)
            {
                throw new ArgumentNullException(nameof(document));
            }

            return document.RootElement.ToObject(returnType, options);
        }
    }

    public static class UserId
    {
        private static readonly CloudEventAttribute[] cloudEventAttributes = new[] { UserIdAttribute };
        public static IEnumerable<CloudEventAttribute> AllAttributes { get; } = cloudEventAttributes;

        public static CloudEventAttribute UserIdAttribute =>
            CloudEventAttribute.CreateExtension("userid", CloudEventAttributeType.String);

        public static CloudEvent SetUserId(this CloudEvent cloudEvent, string userId)
        {
            Validation.CheckNotNull(cloudEvent, nameof(cloudEvent));
            cloudEvent[UserIdAttribute] = userId;

            return cloudEvent;
        }

        public static string GetUserId(this CloudEvent cloudEvent)
        {
            return (string)Validation.CheckNotNull(cloudEvent, nameof(cloudEvent))[UserIdAttribute];
        }
    }

    // FROM: https://github.com/jbw/TooBigToFailBurgerShop/blob/develop/src/services/Ordering/Ordering.StateService/Application/Extensions/Dapr/CloudEventMessageEnvelope.cs

    public class CloudEventReceiveContext : ReceiveContext
    {
        public CloudEventReceiveContext(CloudEvent envelope, ReceiveContext innerContext)
        {
            Envelope = envelope;
            InnerContext = innerContext;
        }

        public CloudEvent Envelope { get; }
        public ReceiveContext InnerContext { get; }

        public Stream GetBodyStream() => InnerContext.GetBodyStream();

        public byte[] GetBody() => InnerContext.GetBody();

        public Task NotifyConsumed<T>(ConsumeContext<T> context, TimeSpan duration, string consumerType) where T : class
            => InnerContext.NotifyConsumed(context, duration, consumerType);

        public Task NotifyFaulted<T>(ConsumeContext<T> context, TimeSpan duration, string consumerType, Exception exception) where T : class
            => InnerContext.NotifyFaulted(context, duration, consumerType, exception);

        public Task NotifyFaulted(Exception exception)
            => InnerContext.NotifyFaulted(exception);

        public void AddReceiveTask(Task task)
            => InnerContext.AddReceiveTask(task);

        public TimeSpan ElapsedTime => InnerContext.ElapsedTime;
        public Uri InputAddress => InnerContext.InputAddress;
        public ContentType ContentType => InnerContext.ContentType;
        public bool Redelivered => InnerContext.Redelivered;
        public Headers TransportHeaders => InnerContext.TransportHeaders;
        public Task ReceiveCompleted => InnerContext.ReceiveCompleted;
        public bool IsDelivered => InnerContext.IsDelivered;
        public bool IsFaulted => InnerContext.IsFaulted;
        public ISendEndpointProvider SendEndpointProvider => InnerContext.SendEndpointProvider;
        public IPublishEndpointProvider PublishEndpointProvider => InnerContext.PublishEndpointProvider;
        public IPublishTopology PublishTopology => InnerContext.PublishTopology;
        public bool PublishFaults => InnerContext.PublishFaults;

        public bool HasPayloadType(Type payloadType) => InnerContext.HasPayloadType(payloadType);

        public bool TryGetPayload<T>(out T payload) where T : class
            => InnerContext.TryGetPayload(out payload);

        public T GetOrAddPayload<T>(PayloadFactory<T> payloadFactory) where T : class
            => InnerContext.GetOrAddPayload(payloadFactory);

        public T AddOrUpdatePayload<T>(PayloadFactory<T> addFactory, UpdatePayloadFactory<T> updateFactory) where T : class
            => InnerContext.AddOrUpdatePayload(addFactory, updateFactory);

        public CancellationToken CancellationToken => InnerContext.CancellationToken;
    }

    internal class CloudEventConsumeContext : DeserializerConsumeContext
    {
        private readonly IReadOnlyDictionary<string, Type> _typeMap;
        private CloudEvent _cloudEvent;

        internal CloudEventConsumeContext(ReceiveContext receiveContext, IReadOnlyDictionary<string, Type> typeMap, CloudEvent cloudEvent)
            : base(new CloudEventReceiveContext(cloudEvent, receiveContext))
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

        public override DateTime? SentTime => _cloudEvent.Time?.UtcDateTime;

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
                var dataObject = ((JsonElement)_cloudEvent.Data).ToObject(GetMessageType());

                if (dataObject is T msg)
                {
                    consumeContext = new MessageConsumeContext<T>(this, msg);
                    return true;
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

            var cloudEvent = cloudFormatter.DecodeStructuredModeMessage(
                receiveContext.GetBodyStream(),
                new ContentType(CloudEvent.MediaType),
                UserId.AllAttributes);

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

                var envelope = new CloudEvent(UserId.AllAttributes);
                envelope.Id = context.MessageId?.ToString();
                envelope.Source = _source;
                envelope.Type = _typeMap[context.Message.GetType()];
                envelope.Time = context.SentTime;
                envelope.Data = context.Message;
                envelope.DataContentType = "application/json";

                if (context.Headers.TryGetHeader("userId", out var value) && value is string userId)
                {
                    envelope.SetUserId(userId);
                }

                var cloudFormatter = new JsonEventFormatter();
                var bytes = cloudFormatter.EncodeStructuredModeMessage(envelope, out var contentType);
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
