using System;
using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace BSSCalculateAttendance
{
    [RunInstaller(true)]
    public class ProjectInstaller : Installer
    {
        private ServiceProcessInstaller processInstaller;
        private ServiceInstaller serviceInstaller;

        public ProjectInstaller()
        {
            // Remove InitializeComponent() call

            processInstaller = new ServiceProcessInstaller();
            serviceInstaller = new ServiceInstaller();

            // Set the account type for the service
            processInstaller.Account = ServiceAccount.LocalSystem;

            // Configure the service installer
            serviceInstaller.ServiceName = "AttendanceService";
            serviceInstaller.DisplayName = "BSS Attendance Calculation Service";
            serviceInstaller.Description = "Service for calculating attendance records";
            serviceInstaller.StartType = ServiceStartMode.Manual;

            // Add both installers to the collection
            Installers.AddRange(new Installer[]
            {
                processInstaller,
                serviceInstaller
            });
        }
    }
}