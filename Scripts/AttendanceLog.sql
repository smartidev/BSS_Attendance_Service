-- Create log table for attendance service (if not exists)
CREATE TABLE AttendanceLog (
    LogID BIGINT IDENTITY(1,1) PRIMARY KEY,
    LogDate DATETIME NOT NULL,
    LogType VARCHAR(20) NOT NULL, -- INFO, ERROR, FATAL
    Message NVARCHAR(500) NOT NULL,
    Details NVARCHAR(MAX) NULL,
    EmployeeID BIGINT NULL,
    CreatedDate DATETIME DEFAULT GETDATE()
);
GO