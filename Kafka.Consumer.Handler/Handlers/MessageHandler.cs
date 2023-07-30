using Confluent.Kafka;

namespace Kafka.Consumer.Handler.Handlers
{
	public class MessageHandler : IHostedService
	{
		private readonly ILogger _logger;
		public MessageHandler(ILogger<MessageHandler> logger)
		{
			_logger = logger;
		}

		public Task StartAsync(CancellationToken cancellationToken)
		{
			/*
			 * Rodar o comando abaixo em um terminal docker para saner o ip do container
			 * docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka
			 */

			var conf = new ConsumerConfig
			{
				GroupId = "test-consumer-group",
				BootstrapServers = "172.19.0.5:9092",
				AutoOffsetReset = AutoOffsetReset.Earliest
			};

			using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
			{
				c.Subscribe("fila_pedido");
				var cts = new CancellationTokenSource();

				try
				{
					while (true)
					{
						var message = c.Consume(cts.Token);
						_logger.LogInformation($"Mensagem: {message.Value} recebida de {message.TopicPartitionOffset}");
					}
				}
				catch (OperationCanceledException)
				{
					c.Close();
				}
			}

			return Task.CompletedTask;
		}

		public Task StopAsync(CancellationToken cancellationToken)
		{
			return Task.CompletedTask;
		}
	}
}
