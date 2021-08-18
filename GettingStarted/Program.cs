
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MassTransit;
using System;
using System.Collections.Generic;
using CloudNative.CloudEvents;
using System.Net.Mime;

namespace GettingStarted
{

    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddMassTransit(x =>
                    {
                        x.AddConsumer<MessageConsumer>();
                        x.AddConsumer<Message2Consumer>();

                        var typeMap = new Dictionary<Type, string>
                        {
                            { typeof(Message), "compatibility.message" }
                        };

                        x.UsingRabbitMq((context, cfg) =>
                        {
                            cfg.AddMessageDeserializer(
                                new ContentType(CloudEvent.MediaType), 
                                () => new CloudEventDeserializer(typeMap));

                            cfg.SetMessageSerializer(
                                () => new CloudEventSerializer("https://cloudevents.io", typeMap));

                            cfg.ConfigureEndpoints(context);
                        });
                    });
                    services.AddMassTransitHostedService(true);

                    services.AddHostedService<Worker>();
                });
    }
}
