using System;
using System.Threading;
using System.Threading.Tasks;
using MassTransit;
using Microsoft.Extensions.Hosting;

namespace GettingStarted
{
    public class Worker : BackgroundService
    {
        readonly IBus _bus;

        public Worker(IBus bus)
        {
            _bus = bus;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await _bus.Publish(new Message {Text = $"The time is {DateTimeOffset.Now}"}, context =>
                {
                    context.Headers.Set("userId", "1");
                });

                await _bus.Publish<Message2>(new { Text = $"The time is {DateTimeOffset.Now}" }, context =>
                {
                    context.Headers.Set("userId", "1");
                });

                await Task.Delay(5_000, stoppingToken);
            }
        }
    }
}