using System;
using System.ServiceProcess;
using System.Timers;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using System.Linq;

namespace BSSCalculateAttendance
{
    public partial class AttendanceService : ServiceBase
    {
        private Timer _timer;
        private DateTime _nextRunTime;
        private TimeSpan[] _runTimes;
        private int _maxRetries;
        private int _retryDelaySeconds;

        protected override void OnStart(string[] args)
        {
            WriteLog("Service started...");
            // Read config values
            _maxRetries = int.TryParse(ConfigurationManager.AppSettings["RetryCount"], out int retries) ? retries : 3;
            _retryDelaySeconds = int.TryParse(ConfigurationManager.AppSettings["RetryDelaySeconds"], out int retryDelay) ? retryDelay : 30;

            // Parse multiple run times (e.g., "09:00,13:00,18:30")
            string runTimesStr = ConfigurationManager.AppSettings["RunTimes"] ?? "09:00";
            _runTimes = runTimesStr.Split(',')
                                   .Select(t => TimeSpan.TryParse(t.Trim(), out var ts) ? ts : new TimeSpan(9, 0, 0))
                                   .OrderBy(t => t)
                                   .ToArray();

            if (_runTimes.Length == 0)
                _runTimes = new[] { new TimeSpan(9, 0, 0) };


            ScheduleNextRun();
        }

        protected override void OnStop()
        {
            _timer?.Stop();
            WriteLog("Service stopped...");
        }

        private void ScheduleNextRun()
        {
            try
            {
                DateTime now = DateTime.Now;

                // Find the next run time for today
                DateTime? next = _runTimes
                    .Select(rt => now.Date.Add(rt))
                    .FirstOrDefault(dt => dt > now);

                // If all today's times passed, schedule first time tomorrow
                if (next == null)
                    next = now.Date.AddDays(1).Add(_runTimes.First());

                _nextRunTime = next.Value;
                double interval = (_nextRunTime - now).TotalMilliseconds;
                if (interval < 0) interval = 0;

                WriteLog($"Next run scheduled at: {_nextRunTime}");

                _timer?.Dispose();
                _timer = new Timer(interval)
                {
                    AutoReset = false
                };

                _timer.Elapsed += async (s, e) =>
                {
                    _timer.Stop();
                    await RunAttendanceJobAsync();
                    ScheduleNextRun(); // re-evaluate next available run time
                };

                _timer.Start();
            }
            catch (Exception ex)
            {
                WriteLog($"Error scheduling next run: {ex.Message}");
            }
        }

        private async Task RunAttendanceJobAsync()
        {
            int attempt = 0;
            bool success = false;

            while (attempt < _maxRetries && !success)
            {
                attempt++;
                try
                {
                    WriteLog($"[Attempt {attempt}] Running Attendance Job at {DateTime.Now}");
                    // Get the current run time and pass to calculation
                    TimeSpan currentRunTime = DateTime.Now.TimeOfDay;
                    await ExecuteStoredProcedureAsync(currentRunTime);
                    WriteLog("Attendance job completed successfully.");
                    success = true;
                }
                catch (SqlException ex)
                {
                    WriteLog($"SQL Error on attempt {attempt}: {ex.Message}");
                }
                catch (Exception ex)
                {
                    WriteLog($"Unexpected error on attempt {attempt}: {ex.Message}");
                }

                if (!success && attempt < _maxRetries)
                {
                    WriteLog($"Retrying in {_retryDelaySeconds} seconds...");
                    await Task.Delay(_retryDelaySeconds * 1000);
                }
            }

            if (!success)
                WriteLog($"Attendance job failed after {_maxRetries} attempts.");
        }

        private async Task ExecuteStoredProcedureAsync(TimeSpan currentRunTime)
        {
            string connStr = ConfigurationManager.ConnectionStrings["DbConn"].ConnectionString;
            string procName = "BSS_CalculateDailyAttendance";
            int timeoutSeconds = int.TryParse(ConfigurationManager.AppSettings["CommandTimeoutSeconds"], out int t) ? t : 0;

            DateTime start = DateTime.Now;
            // Determine the calculation date based on current run time
            DateTime calculationDate = GetCalculationDate(currentRunTime);
            WriteLog($"Executing stored procedure '{procName}' for date: {calculationDate:yyyy-MM-dd}");

            using (SqlConnection conn = new SqlConnection(connStr))
            using (SqlCommand cmd = new SqlCommand(procName, conn))
            {
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandTimeout = timeoutSeconds; // 0 = infinite timeout                                                 
                cmd.Parameters.AddWithValue("@CalculationDate", calculationDate);
                await conn.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
            }

            double duration = (DateTime.Now - start).TotalMinutes;
            WriteLog($"Stored procedure '{procName}' completed in {duration:F2} minutes.");
        }

        private DateTime GetCalculationDate(TimeSpan currentRunTime)
        {
            DateTime now = DateTime.Now;

            // Define the time thresholds
            TimeSpan nightStart = new TimeSpan(0, 1, 0);    // 12:01 AM
            TimeSpan morningEnd = new TimeSpan(9, 0, 0);    // 9:00 AM

            // New logic:
            // - If time is between 12:01 AM and 9:00 AM (inclusive): use yesterday's date
            // - Otherwise: use today's date

            if (currentRunTime >= nightStart && currentRunTime <= morningEnd)
            {
                // Between 12:01 AM and 9:00 AM - use yesterday's date
                WriteLog($"Using yesterday's date (run time: {currentRunTime:hh\\:mm})");
                return now.Date.AddDays(-1);
            }
            else
            {
                // After 9:00 AM - use today's date
                WriteLog($"Using today's date (run time: {currentRunTime:hh\\:mm})");
                return now.Date;
            }
        }

        private void WriteLog(string message)
        {
            try
            {
                string logPath = AppDomain.CurrentDomain.BaseDirectory + "\\BSSCalculateDailyAttendanceService.txt";
                System.IO.File.AppendAllText(logPath, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}{Environment.NewLine}");
            }
            catch { /* Ignore logging errors */ }
        }

        public void DebugOnStart()
        {
            Console.WriteLine("Debug: Calling OnStart...");
            OnStart(null); // Calls your actual OnStart method
            Console.WriteLine("Debug: OnStart completed");
        }

        public void DebugOnStop()
        {
            Console.WriteLine("Debug: Calling OnStop...");
            OnStop(); // Calls your actual OnStop method
            Console.WriteLine("Debug: OnStop completed");
        }
    }
}
