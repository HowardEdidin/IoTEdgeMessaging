using System.Linq;

namespace MessageGeneratorModule
{
    using System;
    using System.IO;
    using System.Collections.Generic;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Microsoft.Azure.Devices.Shared;
    using Newtonsoft.Json;

    class Program
    {
        static void Main(string[] args)
        {
            // The Edge runtime gives us the connection string we need -- it is injected as an environment variable
            string connectionString = Environment.GetEnvironmentVariable("EdgeHubConnectionString");

            // Cert verification is not yet fully functional when using Windows OS for the container
            bool bypassCertVerification = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            if (!bypassCertVerification) InstallCert();
            Init(connectionString, bypassCertVerification).Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>) s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Add certificate in local cert store for use by client for secure connection to IoT Edge runtime
        /// </summary>
        static void InstallCert()
        {
            string certPath = Environment.GetEnvironmentVariable("EdgeModuleCACertificateFile");
            if (string.IsNullOrWhiteSpace(certPath))
            {
                // We cannot proceed further without a proper cert file
                Console.WriteLine($"Missing path to certificate collection file: {certPath}");
                throw new InvalidOperationException("Missing path to certificate file.");
            }
            else if (!File.Exists(certPath))
            {
                // We cannot proceed further without a proper cert file
                Console.WriteLine($"Missing path to certificate collection file: {certPath}");
                throw new InvalidOperationException("Missing certificate file.");
            }
            X509Store store = new X509Store(StoreName.Root, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);
            store.Add(new X509Certificate2(X509Certificate2.CreateFromCertFile(certPath)));
            Console.WriteLine("Added Cert: " + certPath);
            store.Close();
        }


        /// <summary>
        /// Initializes the DeviceClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init(string connectionString, bool bypassCertVerification = false)
        {
            Console.WriteLine($"[{DateTime.Now}] Connection String {0}", connectionString);

            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            // During dev you might want to bypass the cert verification. It is highly recommended to verify certs systematically in production
            if (bypassCertVerification)
            {
                mqttSetting.RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
            }
            ITransportSettings[] settings = {mqttSetting};

            // Open a connection to the Edge runtime
            DeviceClient ioTHubModuleClient = DeviceClient.CreateFromConnectionString(connectionString, settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine($"[{DateTime.Now}] IoT Hub module client initialized.");

            await GenerateMessages(ioTHubModuleClient);
        }

        private static async Task GenerateMessages(DeviceClient ioTHubModuleClient)
        {
            string outputName = "output";
            int batchSize = 1000;
            int counter = 0;
            while (true)
            {
                Console.WriteLine($"[{DateTime.Now}] Generating {batchSize} messages...");
                // generate 1000 messages
                var messages = Enumerable.Range(counter + 1, counter + batchSize)
                    .Select(i => new Message(Encoding.UTF8.GetBytes(i.ToString())));
                counter += batchSize;
                await ioTHubModuleClient.SendEventBatchAsync(outputName, messages);

                
                Console.WriteLine($"[{DateTime.Now}] Waiting 4 minutes...");
                await Task.Delay(TimeSpan.FromMinutes(4));

                Console.WriteLine($"[{DateTime.Now}] Sending 1 message every minute (for 2 minutes)...");
                // generate 1 message / minute for 2 minutes
                for (int x = 0; x < 2; x++)
                {
                    Console.WriteLine($"[{DateTime.Now}]     Sending message...");
                    await ioTHubModuleClient.SendEventAsync(outputName, new Message(Encoding.UTF8.GetBytes(counter++.ToString())));
                    await Task.Delay(TimeSpan.FromMinutes(1));
                }
                
                Console.WriteLine($"[{DateTime.Now}] Sending 1 message every second (for 1 minutes)...");
                // generate 1 message / minute for 1 minutes
                for (int x = 0; x < 60; x++)
                {
                    Console.WriteLine($"[{DateTime.Now}]     Sending message...");
                    await ioTHubModuleClient.SendEventAsync(outputName, new Message(Encoding.UTF8.GetBytes((counter++.ToString()))));
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
                
                
                Console.WriteLine($"[{DateTime.Now}] Sending 20 messages every second (for 1 minutes)...");
                // generate 1 message / minute for 1 minutes
                for (int x = 0; x < 60; x++)
                {
                    Console.WriteLine($"[{DateTime.Now}]     Sending 20 messages...");
                    
                    messages = Enumerable.Range(counter + 1, counter + 20)
                        .Select(i => new Message(Encoding.UTF8.GetBytes(i.ToString())));
                    counter += 20;
                    await ioTHubModuleClient.SendEventBatchAsync(outputName, messages);
                    
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
                Console.WriteLine($"[{DateTime.Now}] Waiting 5 minutes...");
                await Task.Delay(TimeSpan.FromMinutes(5));
            }
        }
    }
}