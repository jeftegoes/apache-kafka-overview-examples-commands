using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "default"
};

var cancellationToken = new CancellationToken();

using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
{
    consumer.Subscribe("demo_topic");

    while (true)
    {
        var consumeResult = consumer.Consume(cancellationToken);
        Console.WriteLine(consumeResult.Message.Value);
    }
}