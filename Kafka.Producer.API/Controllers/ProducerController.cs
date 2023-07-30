
using System;
using System.Net.Sockets;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace Kafka.Producer.API.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class ProducerController : ControllerBase
	{
		[HttpPost]
		[ProducesResponseType(typeof(string), 201)]
		[ProducesResponseType(400)]
		[ProducesResponseType(500)]
		public IActionResult Post([FromQuery] string msg)
		{
			return Created("", SendMessageByKafka(msg));
		}

		private string SendMessageByKafka(string message)
		{
			/*
			 * Rodar o comando abaixo em um terminal docker para saner o ip do container
			 * docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka
			 */

			var config = new ProducerConfig { BootstrapServers = "172.18.0.5:9092" };

			using (var producer = new ProducerBuilder<Null, string>(config).Build())
			{
				try
				{
					var sendResult = producer.ProduceAsync("fila_pedido", new Message<Null, string> { Value = message })
											 .GetAwaiter()
											 .GetResult();

					return $"Mensagem '{sendResult.Value}' de '{sendResult.TopicPartitionOffset}'";
				}
				catch (ProduceException<Null, string> e)
				{
					Console.WriteLine($"Delivery failed: {e.Error.Reason}");
				}
			}

			return string.Empty;
		}
	}
}

