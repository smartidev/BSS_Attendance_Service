CREATE PROCEDURE [dbo].[CalculateDailyAttendanceOfEmployee]
    @AttendanceDate DATE,
    @EmployeeID BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    -- Load AttendanceParameter for employee
    DECLARE		
        @GraceLate INT,
        @GraceEarly INT,
        @AllowLateEarlyPenalty BIT,
        @DeductPenaltyFrom VARCHAR(50),
        @DeductPenaltyCount INT,
        @IsAutoShift BIT,
        @AllowOT BIT,
        @StartOTAfterHours INT,
        @MinOTHoursAllowed INT,
        @MaxOTHoursAllowed INT,
        @AllowOTOnHoliday BIT,
        @AllowOTOnWeekday BIT,
		@ClubbedLeaveAllowed BIT,
		@AllowCompOff BIT,
		@HalfDayCompoffHours INT,
		@FullDayCompoffHours INT,
		@TotalCompoffAllowedInMonth INT,
        @AllowSecondWeekOff BIT,
        @FirstWeekOffDay INT,
        @SecondWeekOffDay INT,
		@ExtraNum1 INT,
        @HalfDayHours DECIMAL(5,2),
        @FullDayHours DECIMAL(5,2),
		@CompanyID INT, 
		@CustomerID INT, 
		@LocationID INT, 
		@DepartmentID INT, 
		@DivisionID INT;	

	-- Select all the AttendanceParameter of an Employee
    SELECT TOP 1
		--Late -Early Settings
		@AllowLateEarlyPenalty = ap.AllowedLateEarlyPenalty, -- Allow Early Late Penalty?
		@GraceEarly = ap.GraceEarlyGoingMinutes,   -- Grace Early Time
        @GraceLate = ap.GraceLateGoingMinutes, -- Grace Late Time           
        @DeductPenaltyFrom = ap.DeductPenaltyFrom, -- Deduct Penalty From?
        @DeductPenaltyCount = ap.DeductLeaveCount, -- Deduct Penalty Count
		
		-- Shift Parameter Settings
        @IsAutoShift = ap.IsAutoShift, -- Is Auto Shift
		
		--OverTime Parameter Settings
        @AllowOT = ap.OTAllowedOrNot, -- Allow Over Time ?
        @StartOTAfterHours = ap.StartOTAfterHours, -- Start OT after Hours
        @MinOTHoursAllowed = ap.MinOTHoursAllowed, -- Minimum OT Hours Allowed
        @MaxOTHoursAllowed = ap.MaxOTAllowedHours, -- Maximum OT Hours Allowed
        @AllowOTOnHoliday = ap.AllowedOTonHoliday, -- Is OT Allowed on Holiday?
        @AllowOTOnWeekday = ap.AllowedOTOnWeekDay, -- Is OT Allowed on Weekday?
		
		--Leave Parameter Settings
		@ClubbedLeaveAllowed =  ap.ClubbedLeaveAllowed, -- Is Clubbed Leave Allowed?
		
		--Compoff Parameter Settings
		@AllowCompOff = ap.AllowCompOff, -- Is Comp Off Allowed ?
		@HalfDayCompoffHours = ap.HalfDayCompoffHours, -- Half Day Comp Off Hours
		@FullDayCompoffHours = ap.FullDayCompoffHours, -- Full Day Comp Off Hours
		@TotalCompoffAllowedInMonth = ap.TotalCompoffAllowedInMonth, -- Total Comp Off Allowed in a Month

		--WeekOff Settings
        @AllowSecondWeekOff = ap.AllowedSecondWeekOff, -- Allow Second Weekoff ?
		@SecondWeekOffDay = ap.SecondWeekOffDay, -- Second WeekOff Day
		@ExtraNum1 = ap.ExtraNum1, -- Second weekoff option
        @FirstWeekOffDay = ap.FirstWeekOffDay -- First WeekOff Day
       
	   --Others
        --@HalfDayHours = ap.HalfDayHours,
        --@FullDayHours = ap.TotalShiftHours
    FROM EmployeeParameterAssignments epa
    INNER JOIN AttendanceParameter ap ON epa.ParameterID = ap.ParameterID
    WHERE epa.EmployeeMasterID = @EmployeeID
    AND epa.IsActive = 1;  
	
 
   DECLARE 
        @CheckIn DATETIME, @CheckOut DATETIME,
        @ShiftStart TIME(7), @ShiftEnd TIME(7),
        @BreakStart TIME(7), @BreakEnd TIME(7),
        @ShiftName VARCHAR(100),
        @IsNight BIT,
        @WorkingHrs DECIMAL(5,2),
		@ExtraHrs DECIMAL(5,2),
        @BreakTime DECIMAL(5,2),
        @LateTime DECIMAL(5,2),
        @EarlyTime DECIMAL(5,2),
        @OTHrs DECIMAL(5,2),
        @DayStatus VARCHAR(10),
		@RequestStatusID INT,
		@RequestStatusApplicationID INT,
		@CompofWorkedHours INT,
		@IsWeekOff BIT, 
		@IsHoliday BIT,
		@IsLeave BIT = 0,
		@DeductPenaltyCounter INT = 0,
		@Remarks VARCHAR(500);

	-- Get Employee Shift For Current Date
    SELECT TOP 1
    @ShiftStart = sm.ShiftStartTime,
    @ShiftEnd = sm.ShiftEndTime,
    @BreakStart = sm.BreakStartTime,
    @BreakEnd = sm.BreakEndTime,
    @ShiftName = sm.ShiftName,
    @IsNight = sm.IsNightShift,
	@HalfDayHours = sm.HalfDayHours,
	@FullDayHours = sm.FullDayHours,
	@CustomerID = sm.CustomerID,
	@CompanyID = em.CompanyID,             
    @LocationID = em.LocationMasterID, 
    @DepartmentID = em.DepartmentID, 
    @DivisionID = em.DivisionID
    FROM EmployeeShiftAssignment esa
    JOIN ShiftMaster sm ON esa.ShiftID = sm.ShiftID
	INNER JOIN EmployeeMaster em ON esa.EmployeeMasterID = em.EmployeeMasterID
    WHERE esa.EmployeeMasterID = @EmployeeID
	ORDER BY esa.AssignmentDate DESC

	-- Setting all the default values
		SET @DayStatus = ''; -- Default status is 'Absent'
        SET @WorkingHrs = 0; 
		SET @ExtraHrs = 0;
		SET @BreakTime = 0;
        SET @LateTime = 0; 
		SET @EarlyTime = 0; 
		SET @OTHrs = 0;
		SET @CheckIn = NULL; 
		SET @CheckOut = NULL;
		SET @IsWeekOff = 0;
		SET @IsHoliday = 0;
		SET @IsLeave = 0;
		SET @RequestStatusID = NULL;	
		SET @RequestStatusApplicationID = NULL;
		SET @CompofWorkedHours = 0;
		SET @Remarks = '';
		
		-- Approved leave or other request
		SELECT @RequestStatusID = ID, @RequestStatusApplicationID = RequestApplicationID
		FROM RequestForm WHERE EmployeeMasterID = @EmployeeID  AND ApprovalStatusID = 2 --Approved Status
			AND @AttendanceDate BETWEEN StartDate AND EndDate --AND RequestApplicationID = 1
		IF @RequestStatusID IS NOT NULL
		BEGIN			
			SET @IsLeave = 1;			
		END    
		
		 -- Holiday check (assuming a HolidayDate column exists)
        IF EXISTS (SELECT 1 FROM Holidays WHERE IsActive = 1 AND CAST(FinancialYear AS DATE) = @AttendanceDate)
		BEGIN  
			SET @IsHoliday = 1;
		END    

		-- WEEK OFF CHECK SECTION
		DECLARE @WeekDay INT = (DATEPART(WEEKDAY, @AttendanceDate) + @@DATEFIRST - 1) % 7; -- Convert SQL weekday (1–7) to 0–6
		DECLARE @WeekOfMonth INT;
		-- Calculate the week number within the month
		SET @WeekOfMonth = DATEPART(WEEK, @AttendanceDate) - DATEPART(WEEK, DATEADD(DAY, 1 - DAY(@AttendanceDate), @AttendanceDate)) + 1;		
		-- First WeekOff applies every week (based on @FirstWeekOffDay)
		IF (@WeekDay = @FirstWeekOffDay)
		BEGIN
			SET @IsWeekOff = 1;		
		END	
		-- Handle Second WeekOff logic if enabled
		IF (@AllowSecondWeekOff = 1)
		BEGIN
			-- WeekOfOptions:
			-- 1 = All (1st, 2nd, 3rd, 4th,5th)
			-- 2 = Second and Fourth
			-- 3 = First and Third
			IF (@WeekDay = @SecondWeekOffDay)
			BEGIN
				IF (@ExtraNum1 = 1 AND @WeekOfMonth IN (1, 2, 3, 4))
					SET @IsWeekOff = 1;
				ELSE IF (@ExtraNum1 = 2 AND @WeekOfMonth IN (2, 4))
					SET @IsWeekOff = 1;
				ELSE IF (@ExtraNum1 = 3 AND @WeekOfMonth IN (1, 3))
					SET @IsWeekOff = 1;
			END
		END 		



		-- Check All Employee Punches
		SELECT 
             @CheckIn = MIN(CASE WHEN r.IOEntryStatus = 1 THEN t.aDateTime END),
             @CheckOut = 
				CASE 
					WHEN EXISTS (
						SELECT 1 
						FROM Transactions t2
						INNER JOIN Readers r2 ON r2.ReaderID = t2.ReaderID
						WHERE t2.EmployeeMasterID = @EmployeeID
						AND r2.IOEntryStatus = 2
					)
					THEN MAX(CASE WHEN r.IOEntryStatus = 2 THEN t.aDateTime END)
					ELSE MAX(CASE WHEN r.IOEntryStatus = 1 THEN t.aDateTime END)
				END
        FROM Transactions t
		INNER JOIN Readers r ON r.ReaderID = t.ReaderID
        WHERE EmployeeMasterID = @EmployeeID
        AND CAST(aDateTime AS DATE) = @AttendanceDate;          


        -- Calculate Employee working hours for current Date
        IF @BreakStart IS NOT NULL AND @BreakEnd IS NOT NULL
            SET @BreakTime = DATEDIFF(MINUTE, @BreakStart, @BreakEnd) / 60.0;


        IF @CheckIn IS NOT NULL AND @CheckOut IS NOT NULL
            SET @WorkingHrs = DATEDIFF(MINUTE, @CheckIn, @CheckOut) / 60.0 - ISNULL(@BreakTime, 0);	 
			

		 -- Calculate Employee Extra Hours
        IF @WorkingHrs > @FullDayHours        
            SET @ExtraHrs = @WorkingHrs - @FullDayHours;        


	    -- Calculate Late  / Early Timings		
		IF ISNULL(@AllowLateEarlyPenalty, 0) = 1
		BEGIN
			SET @GraceLate = ISNULL(@GraceLate, 0);
			SET @GraceEarly = ISNULL(@GraceEarly, 0);

			IF @CheckIn IS NOT NULL AND CONVERT(TIME, @CheckIn) > DATEADD(MINUTE, @GraceLate, @ShiftStart)
				SET @LateTime = DATEDIFF(MINUTE, DATEADD(MINUTE, @GraceLate, @ShiftStart), CONVERT(TIME, @CheckIn)) / 60.0;

			IF @CheckOut IS NOT NULL AND CONVERT(TIME, @CheckOut) < DATEADD(MINUTE, -@GraceEarly, @ShiftEnd)
				SET @EarlyTime = DATEDIFF(MINUTE, CONVERT(TIME, @CheckOut), DATEADD(MINUTE, -@GraceEarly, @ShiftEnd)) / 60.0;

			IF (@LateTime <> 0 OR @EarlyTime <> 0) AND (@LateTime <= @GraceLate OR @EarlyTime <= @GraceEarly)
				SET @DeductPenaltyCounter = @DeductPenaltyCounter + 1
		END
		ELSE
		BEGIN
			IF @CheckIn IS NOT NULL AND CONVERT(TIME, @CheckIn) > @ShiftStart        
			    SET @LateTime = DATEDIFF(MINUTE, @ShiftStart, CONVERT(TIME, @CheckIn)) / 60.0;

			IF @CheckOut IS NOT NULL AND CONVERT(TIME, @CheckOut) < @ShiftEnd       
				SET @EarlyTime = DATEDIFF(MINUTE, CONVERT(TIME, @CheckOut), @ShiftEnd) / 60.0;        
		END
      
        -- Calculate Over Timings	
        IF ISNULL(@AllowOT, 0) = 1
        BEGIN		
			IF (@IsWeekOff = 1 AND ISNULL(@AllowOTOnWeekday, 0) = 1) OR (@IsHoliday = 1 AND ISNULL(@AllowOTOnHoliday, 0) = 1) 
			BEGIN
				IF @ExtraHrs < @MinOTHoursAllowed SET @OTHrs = 0;
				ELSE IF @ExtraHrs = @MinOTHoursAllowed SET @OTHrs = @ExtraHrs;
				ELSE IF @ExtraHrs > @MinOTHoursAllowed AND @ExtraHrs >= @MaxOTHoursAllowed SET @OTHrs = @ExtraHrs;
				ELSE IF @ExtraHrs > @MaxOTHoursAllowed SET @OTHrs = @MaxOTHoursAllowed;
			END
			ELSE
			BEGIN
				IF @ExtraHrs < @MinOTHoursAllowed SET @OTHrs = 0;
				ELSE IF @ExtraHrs = @MinOTHoursAllowed SET @OTHrs = @ExtraHrs;
				ELSE IF @ExtraHrs > @MinOTHoursAllowed AND @ExtraHrs >= @MaxOTHoursAllowed SET @OTHrs = @ExtraHrs;
				ELSE IF @ExtraHrs > @MaxOTHoursAllowed SET @OTHrs = @MaxOTHoursAllowed;
			END
        END

        -- Status logic
		IF @IsLeave = 1 AND (@IsHoliday = 1 OR @IsWeekOff = 1) AND ISNULL(@RequestStatusApplicationID, 0) = 6 AND ISNULL(@AllowCompOff, 0) = 1 --For compoff
		BEGIN
			--PRINT 'compoff Block';
			Select @CompofWorkedHours = ISNULL(TotalHrs,0) from RequestForm Where ID = @RequestStatusID			
			IF @CompofWorkedHours >= @HalfDayCompoffHours 
			BEGIN
				SET @DayStatus = '^';
				SET @WorkingHrs = @CompofWorkedHours
			END
		END
        ELSE IF @IsHoliday = 1 
		BEGIN
			-- PRINT 'Holiday Block';
			SET @DayStatus = 'HH'; -- Holiday
		END
        ELSE IF @IsWeekOff = 1 
		BEGIN
			-- PRINT 'Week Off Block';
			SET @DayStatus = 'WO'; -- Week Off	
		END
        ELSE IF @IsLeave = 1 
		BEGIN
			-- PRINT 'Leave Block';
			SET @DayStatus =				
				CASE 
					WHEN @RequestStatusApplicationID = 1 THEN 'L' -- Leave
					WHEN @RequestStatusApplicationID = 2 THEN '%' -- Tour
					WHEN @RequestStatusApplicationID = 3 THEN '*' -- Manual Entry
					WHEN @RequestStatusApplicationID = 4 THEN '$' -- Out Door Entry
					WHEN @RequestStatusApplicationID = 5 THEN '#' -- Condone					
					WHEN @RequestStatusApplicationID = 7 THEN 'OT' -- OverTime					
				END ;
		END
        ELSE IF @CheckIn IS NULL AND @CheckOut IS NULL 
		BEGIN
			-- PRINT 'Absent Block';
			SET @DayStatus = 'AA'; -- Absent
		END
		ELSE IF (@CheckIn IS NOT NULL AND @CheckOut IS NULL) OR (@CheckIn IS NULL AND @CheckOut IS NOT NULL )
		BEGIN
			-- PRINT 'Irregular Punch Block';
			SET @DayStatus = 'XX'; -- Irregular Punch
		END
        ELSE IF @WorkingHrs >= ISNULL(@FullDayHours, 8) 
		BEGIN
			-- PRINT 'Late/Early Penalty OR Present Block';
			IF @DeductPenaltyCounter > @DeductPenaltyCount
				SET @DayStatus = '@'; -- Late/Early Penalty
			ELSE
				SET @DayStatus = 'PP'; -- Present
		END
        ELSE IF @WorkingHrs >= ISNULL(@HalfDayHours, 4) 
        BEGIN
			-- PRINT 'Late/Early Penalty OR 1st Half Absent OR 1st Half Present Block';
			IF @DeductPenaltyCounter > @DeductPenaltyCount
				SET @DayStatus = '@'; -- Late/Early Penalty
			ELSE
			BEGIN
				DECLARE @ShiftMid TIME;
				SET @ShiftMid = DATEADD(MINUTE, DATEDIFF(MINUTE, @ShiftStart, @ShiftEnd) / 2, @ShiftStart);

				-- Check 1st Half and 2nd Half presence
				IF (@CheckIn IS NOT NULL AND CONVERT(TIME, @CheckIn) > @ShiftMid)
					SET @DayStatus = 'AP'; -- 1st Half Absent
				ELSE IF (@CheckOut IS NOT NULL AND CONVERT(TIME, @CheckOut) < @ShiftMid)
					SET @DayStatus = 'PA'; -- 1st Half Present			
				ELSE
					SET @DayStatus = 'PA'; -- 1st Half Present
			END;
        END
        ELSE 
		BEGIN
			-- PRINT Full day Absent Block
			SET @DayStatus = 'AA'; -- Full day Absent
		END        

        -- Insert Attendance record for Employee for current calulation date
		

        -- Check if attendance record already exists for this employee and date
		IF EXISTS (SELECT 1 FROM Attendance WHERE EmployeeID = @EmployeeID AND [Date] = @AttendanceDate)
		BEGIN
			-- UPDATE existing record
			UPDATE Attendance 
			SET ShiftName = @ShiftName,
				DayStatus = @DayStatus,
				CheckIn = @CheckIn,
				CheckOut = @CheckOut,
				BreakTime = @BreakTime,
				WorkingHrs = @WorkingHrs,
				ExtraHrs = @ExtraHrs,
				LateTime = @LateTime,
				EarlyTime = @EarlyTime,
				OTHrs = @OTHrs,
				PenaltyCounter = @DeductPenaltyCounter
			WHERE EmployeeID = @EmployeeID 
			AND [Date] = @AttendanceDate;
		END
		ELSE
		BEGIN
			-- INSERT new record
			 INSERT INTO Attendance(EmployeeID, LocationID, DepartmentID, DivisionID, CompanyID, CustomerID, [Date], ShiftName, DayStatus, CheckIn, CheckOut, BreakTime, WorkingHrs, ExtraHrs, LateTime, EarlyTime, OTHrs, PenaltyCounter)
			VALUES(@EmployeeID, @LocationID, @DepartmentID, @DivisionID, @CompanyID, @CustomerID, @AttendanceDate, @ShiftName, @DayStatus, @CheckIn, @CheckOut, @BreakTime, @WorkingHrs, @ExtraHrs, @LateTime, @EarlyTime, @OTHrs, @DeductPenaltyCounter);
		END


END;

