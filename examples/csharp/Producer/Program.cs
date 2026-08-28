using System.Net;
using Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = Dns.GetHostName()
};

using (var producer = new ProducerBuilder<Null, string>(config).Build())
{
    for (int i = 0; i < 10; i++)
    {
        var value = $"My log message: #{i}";
        var message = new Message<Null, string> { Value = value };
        await producer.ProduceAsync("demo_topic", message);
        Console.WriteLine($"Send message: {value}");
    }
}