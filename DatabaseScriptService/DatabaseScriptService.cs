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

namespace DatabaseScriptService
{
    public partial class DatabaseScriptService : ServiceBase
    {
        private Timer timer1 = null;
        public DatabaseScriptService()
        {
            InitializeComponent();

            // Instantiate eventLog1 if not using Designer
            eventLog1 = new EventLog();
            if (!EventLog.SourceExists("DatabaseScriptService"))
            {
                EventLog.CreateEventSource("DatabaseScriptService", "Application");
            }
            eventLog1.Source = "DatabaseScriptService";
            eventLog1.Log = "Application";

            // Initialize the timer if not using Designer
            timer1 = new Timer();
            timer1.Interval = 3600000; // 1 hour
            timer1.Elapsed += new ElapsedEventHandler(OnElapsedTime);
            timer1.AutoReset = true;
            timer1.Enabled = false; // We'll enable this in OnStart
        }

        protected override void OnStart(string[] args)
        {
            // Log the service start event
            eventLog1.WriteEntry("DatabaseScriptService started.");

            // Execute immediately when service starts
            ExecuteDatabaseScript();

            // Enable timer for subsequent executions
            timer1.Enabled = true;
        }

        protected override void OnStop()
        {
            timer1.Enabled = false;

            // Log the service stop event
            eventLog1.WriteEntry("DatabaseScriptService stopped.");
        }

        private void OnElapsedTime(object source, ElapsedEventArgs e)
        {
            eventLog1.WriteEntry("OnElapsedTime event fired. Starting script execution.", EventLogEntryType.Information);

            ExecuteDatabaseScript();
        }

        private void ExecuteDatabaseScript()
        {
            try
            {
                // Define your connection string
                string connectionString = @"Server=DESKTOP-IGHTSU5\SQLEXPRESS;Database=test90;User Id=admin;Password=admin;TrustServerCertificate=True;multipleactiveresultsets=True";
                // Read the SQL script from a file
                string script = File.ReadAllText(@"F:\WindowsServicePOC.sql");

                // Normalize "GO" statements
                script = script.Replace("\r\n", "\n"); // Normalize line endings
                var scriptLines = script.Split(new[] { '\n' }, StringSplitOptions.None);
                StringBuilder sqlBuilder = new StringBuilder();
                List<string> commandList = new List<string>();

                foreach (var line in scriptLines)
                {
                    if (line.Trim().Equals("GO", StringComparison.OrdinalIgnoreCase))
                    {
                        // End of batch
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

                // Add any remaining SQL statements
                if (sqlBuilder.Length > 0)
                {
                    commandList.Add(sqlBuilder.ToString());
                }

                // Execute each command
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

                // Log success
                eventLog1.WriteEntry("Database script executed successfully.", EventLogEntryType.Information);
            }
            catch (Exception ex)
            {
                // Log any errors
                string errorMsg = $"Error executing database script: {ex.Message}\n{ex.StackTrace}";
                eventLog1.WriteEntry(errorMsg, EventLogEntryType.Error);
            }
        }
    }
}