CREATE PROCEDURE [dbo].[BSS_CalculateDailyAttendance]
@CalculationDate DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @BatchSize INT = 500; -- Process 500 employees at a time
    DECLARE @ProcessedEmployees INT = 0;
    DECLARE @FailedEmployees INT = 0;
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @MinEmployeeID BIGINT, @MaxEmployeeID BIGINT;
    
    BEGIN TRY
        -- Log procedure start
        INSERT INTO AttendanceLog (LogDate, LogType, Message, Details)
        VALUES (GETDATE(), 'INFO', 'BSS_CalculateDailyAttendance Started', 
                'Calculation Date: ' + CAST(@CalculationDate AS VARCHAR(10)) + 
                ', Batch Size: ' + CAST(@BatchSize AS VARCHAR(10)));
        
        -- Get the range of employee IDs to process
        SELECT @MinEmployeeID = MIN(EmployeeMasterID), 
               @MaxEmployeeID = MAX(EmployeeMasterID)
        FROM EmployeeMaster WHERE IsActive = 1 AND DateOfJoining <= @CalculationDate
            AND (ResignDate IS NULL OR ResignDate >= @CalculationDate);
        
        -- Process in batches
        DECLARE @CurrentBatchStart BIGINT = @MinEmployeeID;
        DECLARE @CurrentBatchEnd BIGINT;
        
        WHILE @CurrentBatchStart <= @MaxEmployeeID
        BEGIN
            SET @CurrentBatchEnd = @CurrentBatchStart + @BatchSize - 1;
            
            BEGIN TRY
                -- Process batch using temporary table
                CREATE TABLE #EmployeeBatch (
                    EmployeeMasterID BIGINT PRIMARY KEY,
                    EmployeeCode NVARCHAR(75),
                    EmployeeName NVARCHAR(300),
                    Processed BIT DEFAULT 0
                );
                
                -- Insert batch of employees
                INSERT INTO #EmployeeBatch (EmployeeMasterID, EmployeeCode, EmployeeName)
                SELECT em.EmployeeMasterID, em.EmployeeCode,
                    ISNULL(em.FirstName, '') + ' ' + ISNULL(em.MiddleName, '') + ' ' + ISNULL(em.LastName, '') AS EmployeeName
                FROM EmployeeMaster em
                WHERE em.EmployeeMasterID BETWEEN @CurrentBatchStart AND @CurrentBatchEnd
                    AND em.IsActive = 1 AND em.DateOfJoining <= @CalculationDate
                    AND (em.ResignDate IS NULL OR em.ResignDate >= @CalculationDate);
                
                -- Process each employee in the current batch
                DECLARE @EmployeeID BIGINT;
                DECLARE @EmployeeCode NVARCHAR(75);
                DECLARE @EmployeeName NVARCHAR(300);
                
                DECLARE batch_cursor CURSOR LOCAL FAST_FORWARD FOR 
                SELECT EmployeeMasterID, EmployeeCode, EmployeeName 
                FROM #EmployeeBatch 
                ORDER BY EmployeeMasterID;
                
                OPEN batch_cursor;
                FETCH NEXT FROM batch_cursor INTO @EmployeeID, @EmployeeCode, @EmployeeName;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    BEGIN TRY
                        EXEC CalculateDailyAttendanceOfEmployee 
                            @AttendanceDate = @CalculationDate,
                            @EmployeeID = @EmployeeID;
                        
                        SET @ProcessedEmployees = @ProcessedEmployees + 1;
                        
                        -- Update batch progress every 100 records
                        IF (@ProcessedEmployees % 100 = 0)
                        BEGIN
                            INSERT INTO AttendanceLog (LogDate, LogType, Message, Details)
                            VALUES (GETDATE(), 'INFO', 'Batch Processing Progress', 
                                    'Processed: ' + CAST(@ProcessedEmployees AS VARCHAR(10)) + 
                                    ', Failed: ' + CAST(@FailedEmployees AS VARCHAR(10)) +
                                    ', Current Batch: ' + CAST(@CurrentBatchStart AS VARCHAR(20)) + '-' + CAST(@CurrentBatchEnd AS VARCHAR(20)));
                        END
                        
                    END TRY
                    BEGIN CATCH
                        SET @FailedEmployees = @FailedEmployees + 1;
                        
                        -- Log error but don't stop batch processing
                        INSERT INTO AttendanceLog (LogDate, LogType, Message, Details, EmployeeID)
                        VALUES (GETDATE(), 'ERROR', 
                                'Failed in batch processing',
                                'Employee: ' + ISNULL(@EmployeeCode, 'N/A') + ' - ' + ISNULL(@EmployeeName, 'N/A') + 
                                ' | Batch: ' + CAST(@CurrentBatchStart AS VARCHAR(20)) + '-' + CAST(@CurrentBatchEnd AS VARCHAR(20)) +
                                ' | Error: ' + ERROR_MESSAGE(),
                                @EmployeeID);
                    END CATCH
                    
                    FETCH NEXT FROM batch_cursor INTO @EmployeeID, @EmployeeCode, @EmployeeName;
                END
                
                CLOSE batch_cursor;
                DEALLOCATE batch_cursor;
                
                -- Clean up temporary table
                DROP TABLE #EmployeeBatch;
                
            END TRY
            BEGIN CATCH
                -- Log batch error but continue with next batch
                INSERT INTO AttendanceLog (LogDate, LogType, Message, Details)
                VALUES (GETDATE(), 'ERROR', 'Batch Processing Failed', 
                        'Batch: ' + CAST(@CurrentBatchStart AS VARCHAR(20)) + '-' + CAST(@CurrentBatchEnd AS VARCHAR(20)) +
                        ' | Error: ' + ERROR_MESSAGE());
            END CATCH
            
            -- Move to next batch
            SET @CurrentBatchStart = @CurrentBatchStart + @BatchSize;
            
            -- Small delay to prevent resource contention (optional)
            WAITFOR DELAY '00:00:01'; -- 1 second delay between batches
        END
        
        -- Log completion summary
        DECLARE @DurationMinutes DECIMAL(10,2) = DATEDIFF(SECOND, @StartTime, GETDATE()) / 60.0;
        DECLARE @SummaryMessage NVARCHAR(500) = 
            'BSS_CalculateDailyAttendance Completed. ' +
            'Processed: ' + CAST(@ProcessedEmployees AS VARCHAR(10)) + 
            ', Failed: ' + CAST(@FailedEmployees AS VARCHAR(10)) +
            ', Duration: ' + CAST(@DurationMinutes AS VARCHAR(10)) + ' minutes' +
            ', Batches: ' + CAST(CEILING((@MaxEmployeeID - @MinEmployeeID + 1) / CAST(@BatchSize AS FLOAT)) AS VARCHAR(10));
        
        INSERT INTO AttendanceLog (LogDate, LogType, Message, Details)
        VALUES (GETDATE(), 'INFO', @SummaryMessage, 
                'Calculation Date: ' + CAST(@CalculationDate AS VARCHAR(10)) +
                ', Total Employees: ' + CAST((@ProcessedEmployees + @FailedEmployees) AS VARCHAR(10)));
        
        -- Return summary
        SELECT 
            @CalculationDate AS CalculationDate,
            @ProcessedEmployees AS ProcessedEmployees,
            @FailedEmployees AS FailedEmployees,
            @DurationMinutes AS DurationMinutes,
            @BatchSize AS BatchSize,
            'SUCCESS' AS ExecutionStatus;
            
    END TRY
    BEGIN CATCH
        -- Log fatal error
        INSERT INTO AttendanceLog (LogDate, LogType, Message, Details)
        VALUES (GETDATE(), 'FATAL', 'BSS_CalculateDailyAttendance Failed', 
                'Error: ' + ERROR_MESSAGE() + 
                ' | Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A') + 
                ' | Line: ' + CAST(ERROR_LINE() AS VARCHAR(10)));
        
        -- Return error details
        SELECT 
            @CalculationDate AS CalculationDate,
            @ProcessedEmployees AS ProcessedEmployees,
            @FailedEmployees AS FailedEmployees,
            DATEDIFF(SECOND, @StartTime, GETDATE()) / 60.0 AS DurationMinutes,
            @BatchSize AS BatchSize,
            'FAILED: ' + ERROR_MESSAGE() AS ExecutionStatus;
        
        THROW;
    END CATCH
END;
GO