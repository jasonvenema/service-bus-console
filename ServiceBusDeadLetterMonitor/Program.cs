using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;

namespace ServiceBusDeadLetterMonitor
{
    class Program
    {
        static IQueueClient queueClient;
        static IQueueClient dlqClient;
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
            string connectionString = Configuration["ServiceBus:ConnectionString"];
            string queueName = Configuration["ServiceBus:Queues:Name"];
            string dlqueueName = Configuration["ServiceBus:DeadLetterQueue:Name"];

            dlqClient = new QueueClient(connectionString, $"{queueName}/{dlqueueName}");
            queueClient = new QueueClient(connectionString, queueName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            // Register QueueClient's MessageHandler and receive messages in a loop
            RegisterOnMessageHandlerAndReceiveMessages();

            Console.ReadKey();

            await dlqClient.CloseAsync();
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of Concurrent calls to the callback `ProcessMessagesAsync`, set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = false
            };

            // Register the function that will process messages
            dlqClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message
            object dlReason = null;
            object dlError = null;
            message.UserProperties.TryGetValue("DeadLetterReason", out dlReason);
            message.UserProperties.TryGetValue("DeadLetterErrorDescription", out dlError);
            Console.WriteLine($"Read DLQ message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            if (dlReason != null && dlError != null)
            {
                Console.WriteLine($"Reason:'{dlReason.ToString()}'  Description:'{dlError.ToString()}'");
            }
            await SendMessagesAsync(message);
        }

        static async Task SendMessagesAsync(Message message)
        {
            try
            {
                // Write the body of the message to the console.
                Console.WriteLine($"Sending message: {Encoding.UTF8.GetString(message.Body)}");

                // Send the message to the queue.
                var cloneMessage = message.Clone();
                await queueClient.SendAsync(cloneMessage);
                await dlqClient.CompleteAsync(message.SystemProperties.LockToken);
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}
