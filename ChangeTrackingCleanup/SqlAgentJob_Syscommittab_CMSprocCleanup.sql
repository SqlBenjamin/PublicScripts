USE [msdb];
GO

/********************************************************************************************
    SET SESSION HANDLING INFO
********************************************************************************************/
SET NOCOUNT ON;
GO

IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'Syscommittab_CMSprocCleanup')
BEGIN
    EXECUTE msdb.dbo.sp_delete_job @job_name=N'Syscommittab_CMSprocCleanup', @delete_unused_schedule=1;
    PRINT 'Job "Syscommittab_CMSprocCleanup" Deleted.';
END;
GO

/********************************************************************************************
  Purpose:      This job uses the ConfigMgr sproc "spDiagChangeTracking" to cleanup syscommittab.

Modification History:
Date            Version    Who                    What

10/23/2018      ?.?        Benjamin Reynolds      Created.
03/05/2019      7.7        Benjamin Reynolds      Updating drive letter location based on latest 'standards'. Preferred order: D, H?, C.
03/11/2019      7.7        Benjamin Reynolds      Commented out the H drive check since that isn't the standard; it was only
                                                  discussed as a possible new standard for certain servers. Preferred drives: D then C.
05/29/2019      7.7        Benjamin Reynolds      Updated delete job portion to print the deletion.
                                                  Some formatting updated; Version added to Modification History.
                                                  (Changes not significant to iterate the version.)
********************************************************************************************/

BEGIN TRANSACTION;
DECLARE @ReturnCode int = 0;

/********************************************************************************************
    Create the SQL Statements to be used in the job steps
********************************************************************************************/
DECLARE  @CM_DB sysname
        ,@PowerShellCommand nvarchar(2000);
DECLARE  @FileExists TABLE ( isFile       int NOT NULL
                            ,isDirectory  int NOT NULL
                            ,ParentExists int NOT NULL
                            );
SELECT TOP 1 @CM_DB = name FROM sys.databases WHERE name LIKE N'CM[_]___';

IF @CM_DB IS NOT NULL
BEGIN
    PRINT N'CM database exists; checking for the powershell file...';
    -- Ensure the PowerShell script exists and create the PowerShell command to use:
    INSERT @FileExists
    EXECUTE master..xp_fileexist N'D:\DBA_Objects\CleanSyscommittabWithCMSproc.ps1';
    IF (SELECT isFile+ParentExists FROM @FileExists) = 2
    BEGIN
        SELECT @PowerShellCommand = N'cd "%SystemRoot%\system32\WindowsPowerShell\v1.0" && powershell.exe -Command "& ''D:\DBA_Objects\CleanSyscommittabWithCMSproc.ps1'' -SqlServerName '''+@@SERVERNAME+N''' -Verbose;"';
        PRINT 'File exists on D drive; proceeding...';
    END;
    ELSE
    BEGIN
        --PRINT 'File not found on D drive; looking on H drive...';
        --DELETE FROM @FileExists;
        --INSERT @FileExists
        --EXECUTE master..xp_fileexist N'H:\DBA_Objects\CleanSyscommittabWithCMSproc.ps1';
        --IF (SELECT isFile+ParentExists FROM @FileExists) = 2
        --BEGIN
        --    SELECT @PowerShellCommand = N'cd "%SystemRoot%\system32\WindowsPowerShell\v1.0" && powershell.exe -Command "& ''H:\DBA_Objects\CleanSyscommittabWithCMSproc.ps1'' -SqlServerName '''+@@SERVERNAME+N''' -Verbose;"';
        --    PRINT 'File exists on H drive; proceeding...';
        --END;
        --ELSE
        --BEGIN
        --    PRINT 'File not found on H drive; looking on C drive...';
            PRINT 'File not found on D drive; looking on C drive...';
            DELETE FROM @FileExists;
            INSERT @FileExists
            EXECUTE master..xp_fileexist N'C:\DBA_Objects\CleanSyscommittabWithCMSproc.ps1';
            IF (SELECT isFile+ParentExists FROM @FileExists) = 2
            BEGIN
                SELECT @PowerShellCommand = N'cd "%SystemRoot%\system32\WindowsPowerShell\v1.0" && powershell.exe -Command "& ''C:\DBA_Objects\CleanSyscommittabWithCMSproc.ps1'' -SqlServerName '''+@@SERVERNAME+N''' -Verbose;"';
                PRINT 'File exists on C drive; proceeding...';
            END;
            ELSE
            BEGIN            
                PRINT 'File not found on drives!; the script will stop!';
                GOTO QuitWithRollback;
            END;
        --END;
    END;
END;
ELSE
BEGIN
    PRINT N'CM database Does NOT exist; No need to install "Syscommittab_CMSprocCleanup" job!';
    GOTO QuitWithRollback;
END;

/********************************************************************************************
    Create the Job Category "Database Maintenance" if it doesn't exist
********************************************************************************************/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
    EXECUTE @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance';
    IF (@@ERROR != 0 OR @ReturnCode != 0)
    GOTO QuitWithRollback;
END

/********************************************************************************************
    Create the Job and pick up the job_id
********************************************************************************************/
DECLARE @jobId binary(16);
EXECUTE  @ReturnCode = msdb.dbo.sp_add_job 
         @job_name = N'Syscommittab_CMSprocCleanup'
        ,@enabled = 1
        ,@notify_level_eventlog = 2
        ,@notify_level_email = 0
        ,@notify_level_netsend = 0
        ,@notify_level_page = 0
        ,@delete_level = 0
        ,@description = N'This cleans up the syscommittab table and side tables using the ConfigMgr Sproc.'
        ,@category_name = N'Database Maintenance'
        ,@owner_login_name = N'sa'
        ,@job_id = @jobId OUTPUT;
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;

/********************************************************************************************
    Create the steps for the Job
********************************************************************************************/
-- Step One
EXECUTE @ReturnCode = msdb.dbo.sp_add_jobstep
        @job_id = @jobId
       ,@step_name = N'Run PowerShell script to cleanup'
       ,@step_id = 1
       ,@cmdexec_success_code = 0
       ,@on_success_action = 1
       ,@on_success_step_id = 0
       ,@on_fail_action = 2 --2=Quit the job reporting failure; 1=Quit the job reporting success
       ,@on_fail_step_id = 0
       ,@retry_attempts = 0
       ,@retry_interval = 0
       ,@os_run_priority = 0
       ,@subsystem = N'CmdExec'
       ,@command = @PowerShellCommand
       ,@flags = 40;
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;

/********************************************************************************************
    Update the Job to create settings
********************************************************************************************/
-- Set the start step
EXECUTE @ReturnCode = msdb.dbo.sp_update_job
        @job_id = @jobId
       ,@start_step_id = 1;
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;

-- Create/set the schedule
EXECUTE @ReturnCode = msdb.dbo.sp_add_jobschedule
        @job_id = @jobId
       ,@name = N'Daily at 6 PM'
       ,@enabled = 1
       ,@freq_type = 4 -- daily
       ,@freq_interval = 1 -- every 1 day
       ,@freq_subday_type = 1 -- at the specified time
       ,@freq_subday_interval = 0
       ,@freq_relative_interval = 0
       ,@freq_recurrence_factor = 0
       ,@active_start_date = 20081026
       ,@active_end_date = 99991231
       ,@active_start_time = 180000 -- 6 PM (HMMSS)
       ,@active_end_time = 235959;
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;

-- Set the server to run as the local server
EXECUTE @ReturnCode = msdb.dbo.sp_add_jobserver
        @job_id = @jobId
       ,@server_name = N'(local)';
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;

COMMIT TRANSACTION;
PRINT 'Job "Syscommittab_CMSprocCleanup" Created.';
GOTO EndSave;

QuitWithRollback:
IF (@@TRANCOUNT > 0)
ROLLBACK TRANSACTION;
PRINT 'Job "Syscommittab_CMSprocCleanup" NOT CREATED; Transaction Rolledback.';

EndSave:
GO
