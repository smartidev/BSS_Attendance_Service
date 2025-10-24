using System;
using System.ServiceProcess;
using System.Runtime.InteropServices;

namespace BSSCalculateAttendance
{
    static class Program
    {
        [DllImport("kernel32.dll")]
        static extern bool AllocConsole();

        [DllImport("kernel32.dll")]
        static extern bool FreeConsole();

        static void Main()
        {
            if (Environment.UserInteractive)
            {
                // Debug mode - run as console app
                AllocConsole();

                try
                {
                    var service = new AttendanceService();

                    Console.WriteLine("=== BSS Attendance Service Debug Mode ===");
                    Console.WriteLine("Starting service...");
                    service.DebugOnStart();

                    Console.WriteLine("Service is running. Press ENTER to stop...");
                    Console.ReadLine();

                    Console.WriteLine("Stopping service...");
                    service.DebugOnStop();
                    Console.WriteLine("Service stopped. Press any key to exit...");
                    Console.ReadKey();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error during debug execution: {ex.Message}");
                    Console.WriteLine("Press ENTER to exit...");
                    Console.ReadLine();
                }
                finally
                {
                    FreeConsole();
                }
            }
            else
            {
                // Production mode - run as service
                ServiceBase[] ServicesToRun = new ServiceBase[]
                {
                    new AttendanceService()
                };
                ServiceBase.Run(ServicesToRun);
            }
        }
    }
}