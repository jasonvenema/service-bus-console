using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;

namespace ServiceBusTopicSender
{
    class Program
    {
        public static ITopicClient topicClient1;
        public static ITopicClient topicClient2;
        public static ITopicClient topicClient3;
        public static IConfigurationRoot Configuration { get; set; }

        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder();
            // tell the builder to look for the appsettings.json file
            builder
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            builder.AddUserSecrets<Program>();
            Configuration = builder.Build();

            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            const int numberOfMessages = 1000;
            string connectionString = Configuration["ServiceBus:ConnectionString"];
            string topic1 = Configuration["ServiceBus:Topics:Topic1:Name"];
            string topic2 = Configuration["ServiceBus:Topics:Topic2:Name"];
            string topic3 = Configuration["ServiceBus:Topics:Topic3:Name"];

            topicClient1 = new TopicClient(connectionString, topic1);
            topicClient2 = new TopicClient(connectionString, topic2);
            topicClient3 = new TopicClient(connectionString, topic3);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after sending all the messages.");
            Console.WriteLine("======================================================");

            // Send messages.
            await SendMessagesAsync(numberOfMessages);

            Console.ReadKey();

            await topicClient1.CloseAsync();
            await topicClient2.CloseAsync();
            await topicClient3.CloseAsync();
        }

        static async Task SendMessagesAsync(int numberOfMessagesToSend)
        {
            try
            {
                Random rnd = new Random(Environment.TickCount);

                for (var i = 0; i < numberOfMessagesToSend; i++)
                {
                    // Create a new message to send to the queue.
                    string messageBody = $"Message {i}";
                    var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                    int topic = rnd.Next(1, 4);

                    if (topic == 1)
                    {
                        Console.WriteLine($"Sending message to topic {topicClient1.TopicName}: {messageBody}");
                        await topicClient1.SendAsync(message);
                    }
                    else if (topic == 2)
                    {
                        Console.WriteLine($"Sending message to topic {topicClient2.TopicName}: {messageBody}");
                        await topicClient2.SendAsync(message);
                    }
                    else
                    {
                        Console.WriteLine($"Sending message to topic {topicClient3.TopicName}: {messageBody}");
                        await topicClient3.SendAsync(message);
                    }

                    Thread.Sleep(2000);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }
    }
}
