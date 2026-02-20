Using command line Insallation with Admin:
C:\Windows\Microsoft.NET\Framework64\v4.0.30319\installutil.exe "C:\Work\BSSCalculateAttendance\bin\Debug\BSSCalculateAttendance.exe"

Verify Service Installation:
# Check if service exists
sc query "AttendanceService"

# Start the service
sc start "AttendanceService"

# Check service status
sc query "AttendanceService"

# Stop the service
sc stop "AttendanceService"


To uninstall (if needed):
C:\Windows\Microsoft.NET\Framework64\v4.0.30319\installutil.exe /u "C:\Work\BSSCalculateAttendance\bin\Debug\BSSCalculateAttendance.exe"


