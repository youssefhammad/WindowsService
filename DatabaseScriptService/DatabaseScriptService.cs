using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;
using System.Timers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;

namespace DatabaseScriptService
{
    public class DatabaseMessage
    {
        public string DatabaseName { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public partial class DatabaseScriptService : ServiceBase
    {
        private Timer timer1 = null;
        private IConnection rabbitMqConnection;
        private IModel rabbitMqChannel;
        private const string QueueName = "connection_string_queue";
        private readonly string fixedConnectionString = @"Server=DESKTOP-IGHTSU5\SQLEXPRESS;Database=test101;User Id=admin;Password=admin;TrustServerCertificate=True;multipleactiveresultsets=True";

        public DatabaseScriptService()
        {
            InitializeComponent();

            // Initialize EventLog
            eventLog1 = new EventLog();
            if (!EventLog.SourceExists("DatabaseScriptService"))
            {
                EventLog.CreateEventSource("DatabaseScriptService", "Application");
            }
            eventLog1.Source = "DatabaseScriptService";
            eventLog1.Log = "Application";

            // Initialize Timer
            timer1 = new Timer();
            timer1.Interval = 3600000; // 1 hour
            timer1.Elapsed += new ElapsedEventHandler(OnElapsedTime);
            timer1.AutoReset = true;
            timer1.Enabled = false;
        }

        protected override void OnStart(string[] args)
        {
            eventLog1.WriteEntry("DatabaseScriptService started.");
            InitializeRabbitMQ();
            timer1.Enabled = true;
        }

        protected override void OnStop()
        {
            timer1.Enabled = false;
            CloseRabbitMQConnection();
            eventLog1.WriteEntry("DatabaseScriptService stopped.");
        }

        private void InitializeRabbitMQ()
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = "localhost",
                    UserName = "guest",
                    Password = "guest"
                };

                rabbitMqConnection = factory.CreateConnection();
                rabbitMqChannel = rabbitMqConnection.CreateModel();

                rabbitMqChannel.QueueDeclare(
                    queue: QueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );

                var consumer = new EventingBasicConsumer(rabbitMqChannel);
                consumer.Received += (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        var databaseMessage = JsonSerializer.Deserialize<DatabaseMessage>(message);

                        eventLog1.WriteEntry($"Received message for database: {databaseMessage.DatabaseName}",
                            EventLogEntryType.Information);

                        CreateDatabaseAndExecuteScript(databaseMessage.DatabaseName);

                        rabbitMqChannel.BasicAck(ea.DeliveryTag, false);
                    }
                    catch (Exception ex)
                    {
                        eventLog1.WriteEntry($"Error processing message: {ex.Message}", EventLogEntryType.Error);
                        rabbitMqChannel.BasicNack(ea.DeliveryTag, false, true);
                    }
                };

                rabbitMqChannel.BasicConsume(
                    queue: QueueName,
                    autoAck: false,
                    consumer: consumer
                );

                eventLog1.WriteEntry("RabbitMQ connection initialized successfully.", EventLogEntryType.Information);
            }
            catch (Exception ex)
            {
                eventLog1.WriteEntry($"Error initializing RabbitMQ: {ex.Message}", EventLogEntryType.Error);
            }
        }

        private void CloseRabbitMQConnection()
        {
            try
            {
                if (rabbitMqChannel?.IsOpen ?? false)
                {
                    rabbitMqChannel.Close();
                    rabbitMqChannel.Dispose();
                }

                if (rabbitMqConnection?.IsOpen ?? false)
                {
                    rabbitMqConnection.Close();
                    rabbitMqConnection.Dispose();
                }
            }
            catch (Exception ex)
            {
                eventLog1.WriteEntry($"Error closing RabbitMQ connection: {ex.Message}", EventLogEntryType.Error);
            }
        }

        private void CreateDatabaseAndExecuteScript(string databaseName)
        {
            try
            {
                // Create master connection string
                var builder = new SqlConnectionStringBuilder(fixedConnectionString)
                {
                    InitialCatalog = "master"
                };
                string masterConnectionString = builder.ConnectionString;

                // Create the database
                using (SqlConnection masterConnection = new SqlConnection(masterConnectionString))
                {
                    masterConnection.Open();
                    string createDbQuery = $"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{databaseName}') " +
                                         $"CREATE DATABASE [{databaseName}]";

                    using (SqlCommand cmd = new SqlCommand(createDbQuery, masterConnection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                // Create connection string for new database
                builder = new SqlConnectionStringBuilder(fixedConnectionString)
                {
                    InitialCatalog = databaseName
                };
                string newDbConnectionString = builder.ConnectionString;

                // Execute the script on the new database
                ExecuteDatabaseScript(newDbConnectionString);

                eventLog1.WriteEntry($"Database {databaseName} created and script executed successfully.",
                    EventLogEntryType.Information);
            }
            catch (Exception ex)
            {
                eventLog1.WriteEntry($"Error creating database and executing script: {ex.Message}",
                    EventLogEntryType.Error);
                throw;
            }
        }

        private void OnElapsedTime(object source, ElapsedEventArgs e)
        {
            eventLog1.WriteEntry("Timer event fired.", EventLogEntryType.Information);
        }

        private void ExecuteDatabaseScript(string connectionString)
        {
            try
            {
                string script = File.ReadAllText(@"F:\WindowsServicePOC.sql");

                script = script.Replace("\r\n", "\n");
                var scriptLines = script.Split(new[] { '\n' }, StringSplitOptions.None);
                StringBuilder sqlBuilder = new StringBuilder();
                List<string> commandList = new List<string>();

                foreach (var line in scriptLines)
                {
                    if (line.Trim().Equals("GO", StringComparison.OrdinalIgnoreCase))
                    {
                        if (sqlBuilder.Length > 0)
                        {
                            commandList.Add(sqlBuilder.ToString());
                            sqlBuilder.Clear();
                        }
                    }
                    else
                    {
                        sqlBuilder.AppendLine(line);
                    }
                }

                if (sqlBuilder.Length > 0)
                {
                    commandList.Add(sqlBuilder.ToString());
                }

                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    conn.Open();

                    foreach (string commandText in commandList)
                    {
                        string trimmedCommand = commandText.Trim();
                        if (trimmedCommand.Length > 0)
                        {
                            using (SqlCommand cmd = new SqlCommand(trimmedCommand, conn))
                            {
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                }

                eventLog1.WriteEntry("Database script executed successfully.", EventLogEntryType.Information);
            }
            catch (Exception ex)
            {
                string errorMsg = $"Error executing database script: {ex.Message}\n{ex.StackTrace}";
                eventLog1.WriteEntry(errorMsg, EventLogEntryType.Error);
                throw;
            }
        }
    }
}