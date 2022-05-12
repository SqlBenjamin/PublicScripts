<#
.SYNOPSIS
    This script is used to help cleanup syscommittab via the supported SQL internal stored procedure "sp_flush_commit_table_on_demand".
.DESCRIPTION
    This script will run the official SQL sproc for cleaning up Syscommittab in order to cleanup Syscommittab. It tries to run the sproc until the sproc is actually
    triggered as it should be and performs the cleanup. If the cleanup is deadlocked it will retry up to 5 times. This was created due to CSS's strong recommendation
    that we use this internal procedure rather than our own (unsupported) methods. It doesn't work as well as a custom solution due to the bug(s) in the internal
    procedure "sp_flush_commit_table_on_demand". If there is an issue with the auto cleanup this can help but in really bad situations it may not work at all due to
    the bugs (which this script tries to work around).
.PARAMETER ServerName
    The name of the SQL Server to connect to.
.PARAMETER DatabaseName
    The name of the database against which to run the sproc to try and cleanup sysycommittab.
.PARAMETER LogDirectory
    The path to the directory where the log file should be written. The default location is in the same folder as this script when it is called.
.PARAMETER LogFileName
    The name of the log file to write to. The file does need to end in ".log". The default is "SqlSysCommitTabCleanup.log".
.PARAMETER LogTableName
    The name of the log table to write to. The default is "DBA.dbo.CommandLog".
.PARAMETER MaxIterations
    This is the total number of times the stored procedure should be called to try and cleanup syscommittab. This is necessary due to a bug in the sproc; this script
    was written to work around the bug. The bug manifests sporadically (more frequent the worse off syscommittab is), so this script calls the sproc until the bug isn't
    hit or until this max number of tries. The default is 1800 tries; the script waits 1 second between tries so this is roughly 30 minutes of trying.
.PARAMETER ConnectionTimeout
    This is the time in seconds to wait for a connection to SQL before erroring out. The default is 120 seconds (2 minutes).
.PARAMETER CommandTimeout
    This is the time in seconds to let the SQL script execute before stopping/erroring out. The default is 172800 seconds (48 hours).
.PARAMETER TotalRecordsToDelete
    The total number of records to delete. Note: if this is not specified then all records will be deleted
.PARAMETER LogToFile
    This flag tells the script to write to a log file. The default is to write to a file.
.PARAMETER LogToTable
    This flag tells the script to write to a SQL table. The default is to write to a table.
.EXAMPLE
    .\CleanupSyscommittabWithInternalSproc.ps1 -ServerName "SomeSqlSrvr" -DatabaseName "CM_123" -LogTableName "SomeDB.dbo.CommandLog";
    This will run the internal procedure to cleanup syscommittab on the "SomeSqlSrvr" server for the database "CM_123". Information will be logged to the CommandLog table in the "SomeDB" database
    and to a file named "SqlSysCommitTabCleanup.log" in the same directory as this script. The script will call the sproc 1800 times or until it determines no cleanup is necessary or it performs
    the cleanup.
.NOTES
    NAME: CleanupSyscommittabWithInternalSproc.ps1
    HISTORY:
        Date         Version    Author                  Notes:
        06/15/2017   ?.?        Benjamin Reynolds       Initial Creation
        06/20/2017   ?.?        Sherry Kissinger        Added logging to a file in the same folder as the script.
        06/29/2017   ?.?        Benjamin Reynolds       Updated to contain script parameters and cmdletbinding;
                                                        Updated logging to file name/logic and the script invocation directory;
                                                        added better verbose and logging information when an iteration begins to cleanup;
                                                        NOTE: I could now remove the "VerboseLogging" variable since I could just use "Write-Verbose" instead...
                                                        but that is something I'll look at in a future iteration.
        07/01/2017   ?.?        Benjamin Reynolds       Added logging to a SQL table; cleaned up a bit and added some comments.
        07/03/2017   ?.?        Benjamin Reynolds       Added additional info to table logging (number of syscommittab records before and after cleanup);
                                                        Moved the final verbose logging lines before the final output per Sherry's feedback.
        07/07/2017   ?.?        Benjamin Reynolds       Added "CheckRunnability" to account for Availability Groups and databases not in certain 'read/write' conditions;
                                                        Started to add some additional 'features' but decided not to finish or go through with them for 
                                                        the time being ("CheckSql" for example); Hopefully fixed 'lastdeadlockerrmsg' logic for logging;
                                                        Updated output text color to try and be more consistent:
                                                        VerboseLogging=Cyan, Exceptions Caught=Magenta, Records Deleted=Yellow, Everything else=White.
        07/11/2017   7.6        Benjamin Reynolds       Added logging if the maxiterations is hit with no safe cleanup version found; Started to add "LogToErrorLog";
        05/29/2019   7.6        Benjamin Reynolds       Added "Version" to Modification History. (Changes not significant to iterage version)
        07/30/2019   8.1        Benjamin Reynolds       Updated the "CheckRunnabilitySql" to account for SQL Server version 2012; the "is_primary_replica" column
                                                        doesn't exist previous to 2014. Added official script comments; Removed "VerboseLogging" variable; Removed unfinished crap.
                                                        Script to be code signed.
                                                        NOTE: This whole script could use a big rewrite (change the functions to use the main multipurpose ones I use now, etc)
                                                        but it's just not that important so not going to spend the time doing that for something that is rarely used now.
#>
[cmdletbinding(PositionalBinding=$false)]
param (
    [Parameter(Mandatory=$true,HelpMessage="Provide the SQL Server name (and Instance Name if not a default instance)")][Alias("SqlServer")][String]$ServerName
   ,[Parameter(Mandatory=$true,HelpMessage="Provide the Database Name")][Alias("Database")][String]$DatabaseName
   ,[Parameter(Mandatory=$false,HelpMessage="The directory in which to write the log file")][ValidateScript({Test-Path $_})][String]$LogDirectory = ""
   ,[Parameter(Mandatory=$false,HelpMessage="The name of the log file. This must end with '.log'.")][ValidateScript({$_.EndsWith(".log")})][String]$LogFileName = "SqlSysCommitTabCleanup.log"
   ,[Parameter(Mandatory=$false,HelpMessage="The 3 part name of the CommandLog table (i.e.: database.schema.table)")][ValidateScript({($_.Length-$_.Replace(".","").Length) -eq 2})][String]$LogTableName = "DBA.dbo.CommandLog"
   ,[Parameter(Mandatory=$false,HelpMessage="The number of times to try to cleanup before stopping the script (waits a second between trials); 1800 = 30 min (if all trials don't find a safe cleanup)")][ValidateRange(1,2147483647)][int]$MaxIterations = 1800
   ,[Parameter(Mandatory=$false,HelpMessage="How long to wait for a connection timeout (in seconds). Note: The max is 20 minutes.")][ValidateRange(0,1200)][int]$ConnectionTimeout = 120
   ,[Parameter(Mandatory=$false,HelpMessage="How long to let the SQL script execute before stopping (in seconds); 28800 = 8 hours; 172800 = 48 hours;")][ValidateRange(1,2147483647)][int]$CommandTimeout = 172800
   ,[Parameter(Mandatory=$false,HelpMessage="The total number of records to delete. Note: if this is not specified then all records will be deleted")][int]$TotalRecordsToDelete = $null
   ,[Parameter(Mandatory=$false,HelpMessage="If this is set to true then a log file will be created and logged to")][Boolean]$LogToFile = $true
   ,[Parameter(Mandatory=$false,HelpMessage="If this is set to true then summary info will be logged to a table")][Boolean]$LogToTable = $true
)
Clear-Host

<#################################################################
    Variable Validation/Setting
#################################################################>
# Get the Start Time:
$ScriptStartTime = Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff";


# Check/Set the Log to File variables:
if ($LogDirectory -eq "") {
    $LogDirectory = Split-Path $MyInvocation.MyCommand.Path  #Place the log in the same folder this script exists when launched
}
else {
    # if a directory was provided make sure there isn't a "\" at the end.
    # there's no need to check for the directory existence since the parameter validates that already:
    if ($LogDirectory.EndsWith("\")) {
        $LogDirectory.TrimEnd("\");
    }
}
$logfile = $LogDirectory + "\" + $LogFileName

# Delete the log file if it already exists:
if ($LogToFile -and (Test-Path $logfile)) {
    Remove-Item $logfile
}

# Check/Set the SQL command(s):
if ($TotalRecordsToDelete) {
    $SqlToExecute = "SET NOCOUNT OFF;
SET DEADLOCK_PRIORITY LOW;
EXECUTE sp_flush_commit_table_on_demand $TotalRecordsToDelete;";
}
else {
    $SqlToExecute = "SET NOCOUNT OFF;
SET DEADLOCK_PRIORITY LOW;
EXECUTE sp_flush_commit_table_on_demand;";
}

[String]$CheckRunnabilitySql = "DECLARE @ReturnMessage varchar(2000);
/* This 'does not exist' check isn't truly necessary since the
   connection will fail to begin with anyway, but is here for anality
   reasons. :) */
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'{0}')
SELECT  @ReturnMessage = CONVERT(varchar(50),'Database Does Not Exist');
ELSE
SELECT @ReturnMessage = COALESCE(@ReturnMessage+',','')+msg.ErrorMessages
  FROM (
SELECT  dbs.name AS [DatabaseName]
       ,CONVERT(varchar(50),CASE WHEN dbs.state != 0 THEN 'Database Not ONLINE' END) AS [state]
       ,CONVERT(varchar(50),CASE WHEN dbs.is_read_only != 0 THEN 'Database Not READ_WRITE' END) AS [is_read_only]
       ,CONVERT(varchar(50),CASE WHEN dbs.user_access != 0 THEN 'Database Not MULTI_USER' END) AS [user_access]
       ,CONVERT(varchar(50),CASE WHEN ISNULL(sys.fn_hadr_is_primary_replica(N'{0}'),1) != 1 THEN 'Server Not PRIMARY in AOAG' END) AS [is_primary_replica]
  FROM sys.databases dbs
) dta
UNPIVOT (ErrorMessages FOR Col IN (state,is_read_only,user_access,is_primary_replica)) msg
 WHERE msg.DatabaseName = N'{0}';
SELECT ISNULL(@ReturnMessage,'Okay To Proceed') AS [CheckRunnabilityReturnMessage];" -f $DatabaseName;

<#################################################################
    Variables (aka Constants...don't change these)
#################################################################>
$ConnectionString = "Server={0};Database={1};Integrated Security=SSPI;Connection Timeout={2}" -f $ServerName,$DatabaseName,$ConnectionTimeout;
$Global:Output = @();
[int64]$Global:RowsAffected = $null;
[int]$Global:NumberOfIterations = 0; # this is required to be globally scoped to handle logging when the safe cleanup version is found on the first run/iteration
[int64]$safe_cleanup_version = 0;
[int]$DeadlockedCount = 0;
[Boolean]$WasDeadlocked = $false;

<#################################################################
    Functions
#################################################################>
# This function is for logging to a file
function LogToFile($StringToLog) {
    (Get-Date -format "dd-MM-yyy HH:mm:ss.mm") + "  " + $StringToLog | Out-File -Filepath $logfile -Append
}
##################################################################
# This function is for logging to a table
function LogToTable {
    param ($TblLoggingSqlCmd)

    # Setup the SQL Connection:
    $SqlConn = New-Object System.Data.SqlClient.SQLConnection;
    $SqlConn.ConnectionString = $ConnectionString;
    # Open the connection and create the command which we'll try in the try/catch:
    $SqlConn.Open();
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand($TblLoggingSqlCmd, $SqlConn);
    $SqlCmd.CommandText = $TblLoggingSqlCmd;
    $SqlCmd.CommandTimeout = $CommandTimeout;

    try {
        $CurScopeId = $SqlCmd.ExecuteScalar();
    }
    catch {
        Write-Host $_.Exception.Message -ForegroundColor Magenta;
        # this may not work but I'll try anyway:
        if ($LogToFile) {
            LogToFile $_.Exception.Message;
        }
    }
    finally { # Make sure to close the connection whether successful or not
        $SqlConn.Close();
    }

    # Return the ScopeId:
    $CurScopeId;
}
##################################################################
# This function is to get the rowcount of syscommittab:
function GetSyscommittabRowcount {
    #
    [String]$SyscommittabRowCountSql = "DECLARE @SpaceUsed table (name nvarchar(255),rows bigint,reserved nvarchar(255),data nvarchar(255),index_size nvarchar(255),unused nvarchar(255));
INSERT @SpaceUsed
EXECUTE sp_spaceused N'sys.syscommittab';
SELECT rows AS [SyscommittabRowCount] FROM @SpaceUsed;";

    # Setup the SQL Connection:
    $SqlConn = New-Object System.Data.SqlClient.SQLConnection;
    $SqlConn.ConnectionString = $ConnectionString;
    # Open the connection and create the command which we'll try in the try/catch:
    $SqlConn.Open();
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand($SyscommittabRowCountSql, $SqlConn);
    $SqlCmd.CommandText = $SyscommittabRowCountSql;
    $SqlCmd.CommandTimeout = $CommandTimeout;

    try {
        [int64]$SyscommittabRowCount = $SqlCmd.ExecuteScalar();
    }
    catch {
        Write-Host "Exception Caught trying to get the Syscommittab RowCount. Error Message:" -ForegroundColor Magenta;
        Write-Host $_.Exception.Message -ForegroundColor Magenta;
        # this may not work but I'll try anyway:
        if ($LogToFile) {
            LogToFile "Exception Caught trying to get the Syscommittab RowCount. Error Message:"
            LogToFile $_.Exception.Message;
        }
    }
    finally { # Make sure to close the connection whether successful or not
        $SqlConn.Close();
    }

    # Return the Syscommittab Row Count:
    $SyscommittabRowCount;
}
##################################################################
# This function captures the sproc's output into global variables
function ProcessOutput {
    param ($event)
    
    # if we've started cleaning up we want to return that in verbose mode for better logging:
    if ($event.RecordCount -gt 0 -and $Global:Output.Length -eq 2) { # the Length of 2 ensures that we only show this message once per iteration
        Write-Verbose "Iteration $Global:NumberOfIterations Returned a Safe Cleanup Version; Cleanup has started...";
        if ($LogToFile) {LogToFile "Iteration $Global:NumberOfIterations Returned a Safe Cleanup Version; Cleanup has started...";};
    };
    # add the captured output to the global variable
    if ($event.Message -ne $null) {
        $Global:Output += $event.Message;
    };
    if ($event.RecordCount -gt 0) {
        $Global:Output += "RecordCount Captured Event = $($event.RecordCount)";
        # if a RecordCount exists then rows were deleted; keep track of the number of rows deleted:
        [int]$CurRowsAffected = $event.RecordCount;
        $Global:RowsAffected += $CurRowsAffected;
        Write-Verbose "Rows Deleted : $CurRowsAffected    ||    Total Rows Deleted so far: $Global:RowsAffected";
        if ($LogToFile) {LogToFile "Rows Deleted : $CurRowsAffected    ||    Total Rows Deleted so far: $Global:RowsAffected"};
    };
}
##################################################################
# This function makes the connection to SQL and tries to run the sproc
function CleanSyscommittab {
    $Global:Output = @();

    # Setup the SQL Connection:
    $SqlConn = New-Object System.Data.SqlClient.SQLConnection;
    $SqlConn.ConnectionString = $ConnectionString;
    # Create an event handler since we need to capture the PRINT commands from the Sproc:
    $EventHandler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {param($sender,$event) ProcessOutput $event};
    $SqlConn.FireInfoMessageEventOnUserErrors = $true;
    $SqlConn.add_InfoMessage($EventHandler);
    # Open the connection and create the command which we'll try in the try/catch:
    $SqlConn.Open();
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand($SqlToExecute, $SqlConn); #$SqlConn.CreateCommand()
    $SqlCmd.CommandText = $SqlToExecute;
    $SqlCmd.CommandTimeout = $CommandTimeout;
    # Create an event handler since we need to capture the "rows affected" output from the Sproc:
    $StmtCmpltdEventHndlr = [System.Data.StatementCompletedEventHandler] {param($sender,$event) ProcessOutput $event};
    $SqlCmd.add_StatementCompleted($StmtCmpltdEventHndlr);

    # Run the sproc (capturing the messages)
    try {
        $Results = $SqlCmd.ExecuteNonQuery();
    }
    catch {
        # if there's an exception we'll display that as well as add it to the
        # global variable for handling/logging:
        Write-Host "Exception Caught in 'CleanSyscommittab' function. Error Message:" -ForegroundColor Magenta;
        Write-Host $_.Exception.Message -ForegroundColor Magenta;
        $Global:Output += $_.Exception.Message;
        
        # I don't think this will work here...hence, the need to handle it outside this function...
        if ($LogToFile) {
            LogToFile "Exception Caught in 'CleanSyscommittab' function. Error Message:";
            LogToFile $_.Exception.Message;
        }
    }
    finally { # Make sure to close the connection whether successful or not
        $SqlConn.Close();
    }
    # Return the messages captured:
    $Global:Output;
}
##################################################################
# This function makes sure the server/database is in a state to be able to perform the activities
# We will assume the server[\instance] and database are correct and just check the "updateability":
function CheckRunnability {
    # Setup the SQL Connection:
    $SqlConn = New-Object System.Data.SqlClient.SQLConnection;
    $SqlConn.ConnectionString = $ConnectionString;
    #$SqlConn.Open();

    ## I don't know if I want to leave this try/catch since it could error due to a timeout
    ## in making the connection...and I don't know if I want to stop on that or not...
    try {
        $SqlConn.Open();
    }
    catch {
        Write-Verbose "Exception Trying to Open the Connection (in 'CheckRunnability' function). Error Message:" -ForegroundColor Magenta;
        Write-Verbose $_.Exception.Message -ForegroundColor Magenta;
        # Log the exception
        if ($LogToFile) {
            LogToFile "Exception Trying to Open the Connection (in 'CheckRunnability' function). Error Message:"
            LogToFile $_.Exception.Message;
        }
        # stop executing the function:
        [String]$CheckRunnabilityReturnMessage = $_.Exception.Message;
        return $CheckRunnabilityReturnMessage;
    }##>

    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand($CheckRunnabilitySql, $SqlConn);
    $SqlCmd.CommandText = $CheckRunnabilitySql;
    $SqlCmd.CommandTimeout = $CommandTimeout;
            
    # Run the SQL command to see if we should try to update it:
    try {
        [String]$CheckRunnabilityReturnMessage = $SqlCmd.ExecuteScalar();
    }
    catch {
        Write-Verbose "Exception Caught in 'CheckRunnability' function. Error Message:" -ForegroundColor Magenta;
        Write-Verbose $_.Exception.Message -ForegroundColor Magenta;
        if ($LogToFile) {
            LogToFile "Exception Caught in 'CheckRunnability' function. Error Message:"
            LogToFile $_.Exception.Message;
        }
        [String]$CheckRunnabilityReturnMessage = $_.Exception.Message;
    }
    finally { # Make sure to close the connection whether successful or not
        $SqlConn.Close();
    }

    # Return the Return Message:
    $CheckRunnabilityReturnMessage;
}
<#################################################################
    End Functions
#################################################################>

# Capture all the input parameters for logging:
<#
Write-Verbose "";
Write-Verbose " Input Parameter Values:";
Write-Verbose "ServerName           : $ServerName";
Write-Verbose "DatabaseName         : $DatabaseName";
Write-Verbose "LogToFile            : $LogToFile";
Write-Verbose "LogToTable           : $LogToTable";
Write-Verbose "MaxIterations        : $MaxIterations";
Write-Verbose "ConnectionTimeout    : $ConnectionTimeout";
Write-Verbose "CommandTimeout       : $CommandTimeout";
Write-Verbose "LogDirectory         : $LogDirectory";
Write-Verbose "LogFileName          : $LogFileName";
Write-Verbose "LogTableName         : $LogTableName";
Write-Verbose "TotalRecordsToDelete : $TotalRecordsToDelete";
Write-Verbose "logfile              : $logfile";
Write-Verbose "SqlToExecute         : $SqlToExecute";
Write-Verbose "ConnectionString     : $ConnectionString";
Write-Verbose "Global:RowsAffected  : $Global:RowsAffected";
Write-Verbose "safe_cleanup_version : $safe_cleanup_version";
Write-Verbose "NumberOfIterations   : $Global:NumberOfIterations";
Write-Verbose "DeadlockedCount      : $DeadlockedCount";
Write-Verbose "WasDeadlocked        : $WasDeadlocked";
#>
# This is in xml style for table logging:
[String]$InputParamInfo = "  <InputParameterValues>
    <ServerName>{0}</ServerName>
    <DatabaseName>{1}</DatabaseName>
    <LogToFile>{2}</LogToFile>
    <LogToTable>{3}</LogToTable>
    <MaxIterations>{5}</MaxIterations>
    <ConnectionTimeout>{6}</ConnectionTimeout>
    <CommandTimeout>{7}</CommandTimeout>
    <LogDirectory>{8}</LogDirectory>
    <LogFileName>{9}</LogFileName>
    <LogTableName>{10}</LogTableName>
    <TotalRecordsToDelete>{11}</TotalRecordsToDelete>
    <logfile>{12}</logfile>
    <SqlToExecute>{13}</SqlToExecute>
    <ConnectionString>{14}</ConnectionString>
  </InputParameterValues>" -f $ServerName,$DatabaseName,$LogToFile,$LogToTable,$MaxIterations,$ConnectionTimeout,$CommandTimeout,$LogDirectory,$LogFileName,$LogTableName,$TotalRecordsToDelete,$logfile,$SqlToExecute,$ConnectionString;

<#################################################################
    Script Start
#################################################################>
# Log start of script:
Write-Host "Script Starting : $(Get-Date)";
Write-Host "**************************************";

if ($LogToFile) {
    LogToFile "Script Starting";
}

# Make sure we can work on the given server/database:
$CheckRunnabilityReturnMessage = CheckRunnability;
if ($CheckRunnabilityReturnMessage -ne "Okay To Proceed") {
    $ScriptEndTime = Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff";
    
    if ($CheckRunnabilityReturnMessage -like "*Cannot open database*") {
        #Don't try to open a connection again by trying to log to the table:
        $LogToTable = $false;
    }
    
    Write-Host "Server/Database not 'runnable' for the following reason(s): $CheckRunnabilityReturnMessage";
    if ($LogToFile) {
        LogToFile "Server/Database not 'runnable' for the following reason(s): $CheckRunnabilityReturnMessage";
        if ($LogToTable) {
            LogToFile "Will try to log to table now";
        }
    }
    
    if ($LogToTable) {
        Write-Verbose "Will try to log to table now";
        [String]$InsExtInfo = "<CleanupInfo>
  <ScriptStart>{0}</ScriptStart>
  <EndTime>{1}</EndTime>
{2}
  <CheckRunnabilityIssues>{3}</CheckRunnabilityIssues>
</CleanupInfo>" -f $ScriptStartTime,$ScriptEndTime,$InputParamInfo,$CheckRunnabilityReturnMessage;
        
        [String]$InsertSql = "IF OBJECT_ID(N'{0}') IS NOT NULL
BEGIN
    INSERT INTO {0} (Command,CommandType,StartTime,ExtendedInfo,EndTime,ErrorNumber,ErrorMessage)
    VALUES ( N'Cleanup syscommittab via sp_flush_commit_table_on_demand'
            ,N'SYSCOMMITTAB_POSH_MAINTAINER'
            ,'{1}'
            ,'{2}'
            ,'{3}'
            ,0
            ,N'Server/Database not runnable for the following reasons so script stopping: {4}'
            );
    SELECT SCOPE_IDENTITY() AS [CurScopeId];
END;" -f $LogTableName,$ScriptStartTime,$InsExtInfo,$ScriptEndTime,$CheckRunnabilityReturnMessage;
    
        $CurScopeId = LogToTable $InsertSql;
    
        if ($CurScopeId -lt 1) {
            Write-Host "No ScopeId returned; Not able to log to table!" -ForegroundColor Magenta;
            if ($LogToFile) {LogToFile "No ScopeId returned; Not able to log to table!";};
        }
        else {
            Write-Verbose "Logged to table using ID $CurScopeId";
            if ($LogToFile) {LogToFile "Logged to table using ID $CurScopeId";};
        }
    }
    
    Write-Host "**************************************";
    Write-Host "Script Ending   : $(Get-Date)";
    if ($LogToFile) {
        LogToFile "Script Ending";
    }
    # Stop execution of the script
    exit;
}
else {
    Write-Verbose "Server/Database are 'runnable' so we will proceed.";
}

#Get the row count of syscommittab at the start of the script:
[int64]$SyscommittabRowCountStart = GetSyscommittabRowcount;
Write-Verbose "Current Total Syscommittab Records = $SyscommittabRowCountStart";
if ($LogToFile) {
    LogToFile "Current Total Syscommittab Records = $SyscommittabRowCountStart";
}

# Log the script start to the table if LogToTable is set:
if ($LogToTable) {
    
    [String]$InsExtInfo = "<CleanupInfo>
  <ScriptStart>{0}</ScriptStart>
  <SysCommitTabRecordsAtStart>{1}</SysCommitTabRecordsAtStart>
{2}
</CleanupInfo>" -f $ScriptStartTime,$SyscommittabRowCountStart,$InputParamInfo;

    [String]$InsertSql = "IF OBJECT_ID(N'{0}') IS NOT NULL
BEGIN
    INSERT INTO {0} (DatabaseName,Command,CommandType,StartTime,ExtendedInfo)
    VALUES ( DB_NAME()
            ,N'Cleanup syscommittab via sp_flush_commit_table_on_demand'
            ,N'SYSCOMMITTAB_POSH_MAINTAINER'
            ,'{1}'
            ,N'{2}'
            );
    SELECT SCOPE_IDENTITY() AS [CurScopeId];
END;" -f $LogTableName,$ScriptStartTime,$InsExtInfo;

    $CurScopeId = LogToTable $InsertSql;

    if ($CurScopeId -lt 1) {
        Write-Host "No ScopeId returned so we can't continue to try and log to a table any more!" -ForegroundColor Magenta;
        $LogToTable = $false;
        if ($LogToFile) {LogToFile "No ScopeId returned so we can't continue to try and log to a table any more!";};
    }
    else {
        Write-Verbose "Logged to table and using ID $CurScopeId";
        if ($LogToFile) {LogToFile "Logged to table and using ID $CurScopeId";};
    }
}

<#################################################################
    Try to Cleanup
#################################################################>
# We'll keep trying until we get the real safe cleanup version
# unless we were deadlocked more than 5 times (this is handled
# inside the loop so probably not necessary here; but this is
# a failsafe just in case).
while ($safe_cleanup_version -eq 0 -and $DeadlockedCount -lt 6) {
    
    $Global:NumberOfIterations += 1;
    
    # if we've been trying and have reached the maximum number of trials then we'll stop
    if ($Global:NumberOfIterations -gt $MaxIterations) {
        $Global:NumberOfIterations -= 1; # if we break we will show the wrong number of iterations so roll the count back
        break;
    }

    # Run the sproc:
    $CleanSyscommittabOutput = CleanSyscommittab;
    
    # Determine the safe cleanup version:
    $safe_cleanup_version = $CleanSyscommittabOutput[1].Substring(48,$CleanSyscommittabOutput[1].Length-49)
    
    # Capture any deadlock information:
    $DeadlockErrMsg = ($CleanSyscommittabOutput -like "Transaction*was deadlocked*");
    if ($DeadlockErrMsg) {
        $DeadlockTime = Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff";
        $DeadlockedCount += 1;
        $WasDeadlocked = $true;
        [String]$LastDeadlockErrMsg = $DeadlockErrMsg[0];

        # Update Log table Info:
        if ($LogToTable -and $CurScopeId) {
            [String]$UpdExtInfo = "<CleanupInfo>
  <ScriptStart>{0}</ScriptStart>
  <SysCommitTabRecordsAtStart>{1}</SysCommitTabRecordsAtStart>
  <TotalSysCommitTabRecordsDeleted>{2}</TotalSysCommitTabRecordsDeleted>
  <RowsToDeletePerIteration>10000</RowsToDeletePerIteration>
  <SafeCleanupVersion>{3}</SafeCleanupVersion>
  <NumberOfDeadlocks>{4}</NumberOfDeadlocks>
  <NumberOfIterations>{5}</NumberOfIterations>
  <DeadlockMessage>{6}</DeadlockMessage>
  <DeadlockTime>{7}</DeadlockTime>
{8}
</CleanupInfo>" -f $ScriptStartTime,$SyscommittabRowCountStart,$Global:RowsAffected,$safe_cleanup_version,$DeadlockedCount,$($Global:NumberOfIterations-1),$LastDeadlockErrMsg,$DeadlockTime,$InputParamInfo;

            [String]$UpdateSql = "IF OBJECT_ID(N'{0}') IS NOT NULL
BEGIN
    UPDATE {0}
       SET  ErrorNumber = 1205
           ,ErrorMessage = N'{1}'
           ,ExtendedInfo = '{2}'
     WHERE ID = {3};
    SELECT {3} AS [CurScopeId];
END;" -f $LogTableName,$LastDeadlockErrMsg,$UpdExtInfo,$CurScopeId;

            $ScopeId = LogToTable $UpdateSql;
        }
    }# End of Deadlock info capturing

    ## Verbose/Debugging output:
    $VerboseIterationLine = "Iteration $Global:NumberOfIterations; Safe Cleanup Version = $safe_cleanup_version"
    if ($safe_cleanup_version -gt 0) {
        if ($WasDeadlocked) {
            $VerboseIterationLine += "; Rows Cleaned Up = $Global:RowsAffected; Trial was DEADLOCKED so may try again"
        }
        else {
            $VerboseIterationLine += "; Rows Cleaned Up = $Global:RowsAffected; script will stop now"
        }
    }
    else {
        if ($Global:NumberOfIterations -eq $MaxIterations) {
            $VerboseIterationLine += "; Max Iterations reached; script will stop now"
        }
        else {
            $VerboseIterationLine += "; will retry in one second since safe cleanup version didn't return"
        }
    }
    Write-Verbose $VerboseIterationLine;
    if ($LogToFile) {LogToFile $VerboseIterationLine;}
    ## End of verbose/debugging output

    # If we were deadlocked (and haven't reached the 5 deadlock limit) reset the safe cleanup version so we try again:
    if ($WasDeadlocked -eq $true) {
        Write-Verbose "Iteration $Global:NumberOfIterations found a safe cleanup version but was deadlocked";
        if ($LogToFile) {LogToFile "Iteration $Global:NumberOfIterations found a safe cleanup version but was deadlocked"}

        if ($DeadlockedCount -le 5) {
            $safe_cleanup_version = 0;
            $WasDeadlocked = $false;
        }
        else {
            break;
        }
    } # End of if deadlocked and less than deadlock limit
    
    # Wait a second before retrying:
    Start-Sleep -Seconds 1;
}

<#################################################################
    Final Output
#################################################################>
[int64]$SyscommittabRowCountEnd = GetSyscommittabRowcount;
$SriptEndTime = Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff";

if ($WasDeadlocked) {
    ## if verbose logging add the verbose info before the final output:
    Write-Verbose "";
    Write-Verbose "************************************************************************************************************";
    Write-Verbose "************************************************************************************************************";
    Write-Verbose "  Last Messages Captured :";

    if ($LogToFile) {
        LogToFile "************************************************************************************************************";
        LogToFile "Last Messages Captured :";
    }

    foreach ($OutputLine in $CleanSyscommittabOutput) {
        Write-Verbose $OutputLine;
        if ($LogToFile) {
            LogToFile $OutputLine;
        }
    }
    Write-Verbose "************************************************************************************************************";
    if ($LogToFile) {
        LogToFile "************************************************************************************************************";
    }
    ## End Verbose Logging
    
    # Display/Log Final output Info:
    Write-Host "";
    Write-Host "Script Ended with a deadlock!" -ForegroundColor Red;
    Write-Host "        Last Values        :";
    Write-Host "Safe Cleanup Version       : $safe_cleanup_version";
    Write-Host "Syscommittab Rows deleted  : $Global:RowsAffected";
    Write-Host "Deadlocks Encountered      : $DeadlockedCount";
    Write-Host "Number of Iterations       : $Global:NumberOfIterations";
    Write-Host "Syscommittab Records Start : $SyscommittabRowCountStart";
    Write-Host "Syscommittab Records End   : $SyscommittabRowCountEnd";

    if ($LogToFile) {
        LogToFile "Script Ended with a deadlock!";
        LogToFile "Last Values:";
        LogToFile "Safe Cleanup Version: $safe_cleanup_version";
        LogToFile "Syscommittab Rows deleted: $Global:RowsAffected";
        LogToFile "Deadlocks Encountered: $DeadlockedCount";
        LogToFile "Number of Iterations: $Global:NumberOfIterations";
        LogToFile "Syscommittab Records Start: $SyscommittabRowCountStart";
        LogToFile "Syscommittab Records End: $SyscommittabRowCountEnd";
    }
    
    # if LogToTable set finish the table logging:
    if ($LogToTable -and $CurScopeId) {
        [String]$UpdExtInfo = "<CleanupInfo>
  <ScriptStart>{0}</ScriptStart>
  <SysCommitTabRecordsAtStart>{1}</SysCommitTabRecordsAtStart>
  <TotalSysCommitTabRecordsDeleted>{2}</TotalSysCommitTabRecordsDeleted>
  <RowsToDeletePerIteration>10000</RowsToDeletePerIteration>
  <SafeCleanupVersion>{3}</SafeCleanupVersion>
  <NumberOfDeadlocks>{4}</NumberOfDeadlocks>
  <NumberOfIterations>{5}</NumberOfIterations>
  <DeadlockMessage>{6}</DeadlockMessage>
  <DeadlockTime>{7}</DeadlockTime>
{8}
  <SysCommitTabRecordsAtEnd>{9}</SysCommitTabRecordsAtEnd>
  <EndTime>{10}</EndTime>
</CleanupInfo>" -f $ScriptStartTime,$SyscommittabRowCountStart,$Global:RowsAffected,$safe_cleanup_version,$DeadlockedCount,$Global:NumberOfIterations,$LastDeadlockErrMsg,$DeadlockTime,$InputParamInfo,$SyscommittabRowCountEnd,$SriptEndTime;

        [String]$UpdateSql = "IF OBJECT_ID(N'{0}') IS NOT NULL
BEGIN
    UPDATE {0}
       SET  EndTime = '{1}'
           ,ErrorNumber = 1205
           ,ErrorMessage = N'Deadlock Threshold Reached; 5 Concurrent Deadlocks have been hit so procedure stopping: {2}'
           ,ExtendedInfo = '{3}'
     WHERE ID = {4};
    SELECT {4} AS [CurScopeId];
END;" -f $LogTableName,$SriptEndTime,$LastDeadlockErrMsg,$UpdExtInfo,$CurScopeId;
    
        $ScopeId = LogToTable $UpdateSql;
    }
} # end of "WasDeadlocked" if
else {
    ## if verbose logging add the verbose info before the final output:
    Write-Verbose "";
    Write-Verbose "************************************************************************************************************";
    Write-Verbose "************************************************************************************************************";
    Write-Verbose "  Last Messages Captured :";

    if ($LogToFile) {
        LogToFile "************************************************************************************************************";
        LogToFile "Last Messages Captured:";
    }

    foreach ($OutputLine in $CleanSyscommittabOutput) {
        Write-Verbose $OutputLine;
        if ($LogToFile) {
            LogToFile $OutputLine;
        }
    }
    Write-Verbose "************************************************************************************************************";
    if ($LogToFile) {
        LogToFile "************************************************************************************************************";
    }
    ## End Verbose Logging
    
    # Display/Log Final output Info:
    if ($Global:NumberOfIterations -eq $MaxIterations) {
        # The script ran the max iterations and never got a chance to cleanup!
        Write-Host "";
        Write-Host "Script Completed; But never got to cleanup records because the safe cleanup version was never found!";
        Write-Host "        Last Values      :";
        Write-Host "Safe Cleanup Version     : $safe_cleanup_version";
        Write-Host "Syscommittab Rows deleted: $Global:RowsAffected";
        Write-Host "Deadlocks Encountered    : $DeadlockedCount";
        Write-Host "Number of Iterations     : $Global:NumberOfIterations";
        Write-Host "Syscommittab Records Start : $SyscommittabRowCountStart";
        Write-Host "Syscommittab Records End   : $SyscommittabRowCountEnd";

        if ($LogToFile) {
            LogToFile "Script Completed; But never got to cleanup records because the safe cleanup version was never found!";
            LogToFile "Last Values:";
            LogToFile "Safe Cleanup Version: $safe_cleanup_version";
            LogToFile "Syscommittab Rows deleted: $Global:RowsAffected";
            LogToFile "Deadlocks Encountered: $DeadlockedCount";
            LogToFile "Number of Iterations: $Global:NumberOfIterations";
            LogToFile "Syscommittab Records Start: $SyscommittabRowCountStart";
            LogToFile "Syscommittab Records End: $SyscommittabRowCountEnd";
        }
    }
    else {
        Write-Host "";
        Write-Host "Script Completed!";
        Write-Host "        Last Values      :";
        Write-Host "Safe Cleanup Version     : $safe_cleanup_version";
        Write-Host "Syscommittab Rows deleted: $Global:RowsAffected";
        Write-Host "Deadlocks Encountered    : $DeadlockedCount";
        Write-Host "Number of Iterations     : $Global:NumberOfIterations";
        Write-Host "Syscommittab Records Start : $SyscommittabRowCountStart";
        Write-Host "Syscommittab Records End   : $SyscommittabRowCountEnd";

        if ($LogToFile) {
            LogToFile "Script Completed!";
            LogToFile "Last Values:";
            LogToFile "Safe Cleanup Version: $safe_cleanup_version";
            LogToFile "Syscommittab Rows deleted: $Global:RowsAffected";
            LogToFile "Deadlocks Encountered: $DeadlockedCount";
            LogToFile "Number of Iterations: $Global:NumberOfIterations";
            LogToFile "Syscommittab Records Start: $SyscommittabRowCountStart";
            LogToFile "Syscommittab Records End: $SyscommittabRowCountEnd";
        }
    }
    
    
    
    # if LogToTable set finish the table logging:
    if ($LogToTable -and $CurScopeId) {
        if ($DeadlockedCount -eq 0) {
            if ($Global:NumberOfIterations -eq $MaxIterations -and $safe_cleanup_version -eq 0) {
                [String]$UpdExtInfo = "<CleanupInfo>
  <ScriptStart>{0}</ScriptStart>
  <SysCommitTabRecordsAtStart>{1}</SysCommitTabRecordsAtStart>
  <TotalSysCommitTabRecordsDeleted>{2}</TotalSysCommitTabRecordsDeleted>
  <RowsToDeletePerIteration>10000</RowsToDeletePerIteration>
  <SafeCleanupVersion>{3}</SafeCleanupVersion>
  <NumberOfDeadlocks>{4}</NumberOfDeadlocks>
  <NumberOfIterations>{5}</NumberOfIterations>
{6}
  <SysCommitTabRecordsAtEnd>{7}</SysCommitTabRecordsAtEnd>
  <EndTime>{8}</EndTime>
</CleanupInfo>" -f $ScriptStartTime,$SyscommittabRowCountStart,$Global:RowsAffected,$safe_cleanup_version,$DeadlockedCount,$Global:NumberOfIterations,$InputParamInfo,$SyscommittabRowCountEnd,$SriptEndTime;
        
                [String]$UpdateSql = "IF OBJECT_ID(N'{0}') IS NOT NULL
BEGIN
    UPDATE {0}
       SET  EndTime = '{1}'
           ,ExtendedInfo = '{2}'
           ,ErrorNumber = 1
           ,ErrorMessage = N'No Cleanup Version found within the provided MaxIterations!'
     WHERE ID = {3};
    SELECT {3} AS [CurScopeId];
END;" -f $LogTableName,$SriptEndTime,$UpdExtInfo,$CurScopeId;
            
                <# Do I want to do this???
                # Trigger an Error so we know to re-run the script (if automated in a job):
                $LogIssueResult = LogToErrorLog 10261975,"CleanupSyscommittabWithInternalSproc ran the max iterations and never found a safe cleanup version","WARNING";
                #>
            
            } # end if for never finding a safe cleanup version
            else {
                [String]$UpdExtInfo = "<CleanupInfo>
  <ScriptStart>{0}</ScriptStart>
  <SysCommitTabRecordsAtStart>{1}</SysCommitTabRecordsAtStart>
  <TotalSysCommitTabRecordsDeleted>{2}</TotalSysCommitTabRecordsDeleted>
  <RowsToDeletePerIteration>10000</RowsToDeletePerIteration>
  <SafeCleanupVersion>{3}</SafeCleanupVersion>
  <NumberOfDeadlocks>{4}</NumberOfDeadlocks>
  <NumberOfIterations>{5}</NumberOfIterations>
{6}
  <SysCommitTabRecordsAtEnd>{7}</SysCommitTabRecordsAtEnd>
  <EndTime>{8}</EndTime>
</CleanupInfo>" -f $ScriptStartTime,$SyscommittabRowCountStart,$Global:RowsAffected,$safe_cleanup_version,$DeadlockedCount,$Global:NumberOfIterations,$InputParamInfo,$SyscommittabRowCountEnd,$SriptEndTime;
        
                [String]$UpdateSql = "IF OBJECT_ID(N'{0}') IS NOT NULL
BEGIN
    UPDATE {0}
       SET  EndTime = '{1}'
           ,ExtendedInfo = '{2}'
     WHERE ID = {3};
    SELECT {3} AS [CurScopeId];
END;" -f $LogTableName,$SriptEndTime,$UpdExtInfo,$CurScopeId;
            } # end else for never finding a safe cleanup version (we're in the else if we did find a safe cleanup version)
        } # end if there were no deadlocks during the run
        else {
            [String]$UpdExtInfo = "<CleanupInfo>
  <ScriptStart>{0}</ScriptStart>
  <SysCommitTabRecordsAtStart>{1}</SysCommitTabRecordsAtStart>
  <TotalSysCommitTabRecordsDeleted>{2}</TotalSysCommitTabRecordsDeleted>
  <RowsToDeletePerIteration>10000</RowsToDeletePerIteration>
  <SafeCleanupVersion>{3}</SafeCleanupVersion>
  <NumberOfDeadlocks>{4}</NumberOfDeadlocks>
  <NumberOfIterations>{5}</NumberOfIterations>
  <DeadlockMessage>{6}</DeadlockMessage>
  <DeadlockTime>{7}</DeadlockTime>
{8}
  <SysCommitTabRecordsAtEnd>{9}</SysCommitTabRecordsAtEnd>
  <EndTime>{10}</EndTime>
</CleanupInfo>" -f $ScriptStartTime,$SyscommittabRowCountStart,$Global:RowsAffected,$safe_cleanup_version,$DeadlockedCount,$Global:NumberOfIterations,$LastDeadlockErrMsg,$DeadlockTime,$InputParamInfo,$SyscommittabRowCountEnd,$SriptEndTime;
        
            [String]$UpdateSql = "IF OBJECT_ID(N'{0}') IS NOT NULL
BEGIN
    UPDATE {0}
       SET  EndTime = '{1}'
           ,ErrorNumber = 0
           ,ErrorMessage = N'Deadlocks encountered but script finished; last deadlock message: {2}'
           ,ExtendedInfo = '{3}'
     WHERE ID = {4};
    SELECT {4} AS [CurScopeId];
END;" -f $LogTableName,$SriptEndTime,$LastDeadlockErrMsg,$UpdExtInfo,$CurScopeId;
        }
        
        $ScopeId = LogToTable $UpdateSql;
    }
} # end of else for "WasDeadlocked"
# EndScript
Write-Host "**************************************";
Write-Host "Script Ending   : $(Get-Date)";
# SIG # Begin signature block
# MIIdcAYJKoZIhvcNAQcCoIIdYTCCHV0CAQExCzAJBgUrDgMCGgUAMGkGCisGAQQB
# gjcCAQSgWzBZMDQGCisGAQQBgjcCAR4wJgIDAQAABBAfzDtgWUsITrck0sYpfvNR
# AgEAAgEAAgEAAgEAAgEAMCEwCQYFKw4DAhoFAAQUpvdAYquP3+TPK+VPnoDH83/l
# eY6gghicMIIE2jCCA8KgAwIBAgITMwAAAQWOyikiHmo0WwAAAAABBTANBgkqhkiG
# 9w0BAQUFADB3MQswCQYDVQQGEwJVUzETMBEGA1UECBMKV2FzaGluZ3RvbjEQMA4G
# A1UEBxMHUmVkbW9uZDEeMBwGA1UEChMVTWljcm9zb2Z0IENvcnBvcmF0aW9uMSEw
# HwYDVQQDExhNaWNyb3NvZnQgVGltZS1TdGFtcCBQQ0EwHhcNMTgwODIzMjAyMDI0
# WhcNMTkxMTIzMjAyMDI0WjCByjELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAldBMRAw
# DgYDVQQHEwdSZWRtb25kMR4wHAYDVQQKExVNaWNyb3NvZnQgQ29ycG9yYXRpb24x
# LTArBgNVBAsTJE1pY3Jvc29mdCBJcmVsYW5kIE9wZXJhdGlvbnMgTGltaXRlZDEm
# MCQGA1UECxMdVGhhbGVzIFRTUyBFU046M0JENC00QjgwLTY5QzMxJTAjBgNVBAMT
# HE1pY3Jvc29mdCBUaW1lLVN0YW1wIHNlcnZpY2UwggEiMA0GCSqGSIb3DQEBAQUA
# A4IBDwAwggEKAoIBAQCffZs9uGatv9jfpb3g0Q0muReKdfyO+ND1cMPAHg/+ltXc
# 1XcUSSvbtE2sQOpyzJ6lAdbDHTouZnya8uI0AYAipfNXEnp0eB1l5b5mnVvKumye
# nWxzU1YLanf9rzp4HKHxuhl8kP8VlcJd0x0zBxj1JAHHO8jVI35U3v08cVReLMw5
# QWdlWQz/Swutiuhde2k613yzR4I5M7gsm4S0xcuC+vB1SzjwqSoYXCnRfhXvz+wB
# FvXlUycvp+9dnjfQFuoJdy/9yppx9EGLW86fsLqnkEZO9kKACU22tZusBpioC3+w
# jd96i5SkflDjVjLxHbMKFIKD3XIgx1oxrBVO4Yl/AgMBAAGjggEJMIIBBTAdBgNV
# HQ4EFgQUS/krKiFv0JlX9HMQH8enXOKF3c0wHwYDVR0jBBgwFoAUIzT42VJGcArt
# QPt2+7MrsMM1sw8wVAYDVR0fBE0wSzBJoEegRYZDaHR0cDovL2NybC5taWNyb3Nv
# ZnQuY29tL3BraS9jcmwvcHJvZHVjdHMvTWljcm9zb2Z0VGltZVN0YW1wUENBLmNy
# bDBYBggrBgEFBQcBAQRMMEowSAYIKwYBBQUHMAKGPGh0dHA6Ly93d3cubWljcm9z
# b2Z0LmNvbS9wa2kvY2VydHMvTWljcm9zb2Z0VGltZVN0YW1wUENBLmNydDATBgNV
# HSUEDDAKBggrBgEFBQcDCDANBgkqhkiG9w0BAQUFAAOCAQEAcLcxL0JQzfHT3vPE
# OVH1qIuPJjuI+CmWyxzaqMn9K8XLFjBEguHUo818JoDzFujQTVYHFnB+Me4EQBj3
# eAKz4WIOndt6nEtyZq8w/k1iJCJfR+r36dRZjkbpBpyezdPAUAVwzrzuKYvsYlT8
# xb9EyItAsLIog5zxfixxaJFD9lWLytcMOV1if3T3M4ASsV/UcakF2RtaSyav9i8d
# Du9xMWM9OxQjzWNOUEtbuditPvUG7y3dLYBsTfG3EzlbKxd0fp5a/Kq4OhQosnbF
# 7mxNnsCc7QDMVYiM5bpv7AJsxMUC9/5upsjhATVvG1COGLlY07O+w7Yp8f+cP/7e
# 6Y30xDCCBgcwggPvoAMCAQICCmEWaDQAAAAAABwwDQYJKoZIhvcNAQEFBQAwXzET
# MBEGCgmSJomT8ixkARkWA2NvbTEZMBcGCgmSJomT8ixkARkWCW1pY3Jvc29mdDEt
# MCsGA1UEAxMkTWljcm9zb2Z0IFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9yaXR5MB4X
# DTA3MDQwMzEyNTMwOVoXDTIxMDQwMzEzMDMwOVowdzELMAkGA1UEBhMCVVMxEzAR
# BgNVBAgTCldhc2hpbmd0b24xEDAOBgNVBAcTB1JlZG1vbmQxHjAcBgNVBAoTFU1p
# Y3Jvc29mdCBDb3Jwb3JhdGlvbjEhMB8GA1UEAxMYTWljcm9zb2Z0IFRpbWUtU3Rh
# bXAgUENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAn6Fssd/bSJIq
# fGsuGeG94uPFmVEjUK3O3RhOJA/u0afRTK10MCAR6wfVVJUVSZQbQpKumFwwJtoA
# a+h7veyJBw/3DgSY8InMH8szJIed8vRnHCz8e+eIHernTqOhwSNTyo36Rc8J0F6v
# 0LBCBKL5pmyTZ9co3EZTsIbQ5ShGLieshk9VUgzkAyz7apCQMG6H81kwnfp+1pez
# 6CGXfvjSE/MIt1NtUrRFkJ9IAEpHZhEnKWaol+TTBoFKovmEpxFHFAmCn4TtVXj+
# AZodUAiFABAwRu233iNGu8QtVJ+vHnhBMXfMm987g5OhYQK1HQ2x/PebsgHOIktU
# //kFw8IgCwIDAQABo4IBqzCCAacwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU
# IzT42VJGcArtQPt2+7MrsMM1sw8wCwYDVR0PBAQDAgGGMBAGCSsGAQQBgjcVAQQD
# AgEAMIGYBgNVHSMEgZAwgY2AFA6sgmBAVieX5SUT/CrhClOVWeSkoWOkYTBfMRMw
# EQYKCZImiZPyLGQBGRYDY29tMRkwFwYKCZImiZPyLGQBGRYJbWljcm9zb2Z0MS0w
# KwYDVQQDEyRNaWNyb3NvZnQgUm9vdCBDZXJ0aWZpY2F0ZSBBdXRob3JpdHmCEHmt
# FqFKoKWtTHNY9AcTLmUwUAYDVR0fBEkwRzBFoEOgQYY/aHR0cDovL2NybC5taWNy
# b3NvZnQuY29tL3BraS9jcmwvcHJvZHVjdHMvbWljcm9zb2Z0cm9vdGNlcnQuY3Js
# MFQGCCsGAQUFBwEBBEgwRjBEBggrBgEFBQcwAoY4aHR0cDovL3d3dy5taWNyb3Nv
# ZnQuY29tL3BraS9jZXJ0cy9NaWNyb3NvZnRSb290Q2VydC5jcnQwEwYDVR0lBAww
# CgYIKwYBBQUHAwgwDQYJKoZIhvcNAQEFBQADggIBABCXisNcA0Q23em0rXfbznlR
# TQGxLnRxW20ME6vOvnuPuC7UEqKMbWK4VwLLTiATUJndekDiV7uvWJoc4R0Bhqy7
# ePKL0Ow7Ae7ivo8KBciNSOLwUxXdT6uS5OeNatWAweaU8gYvhQPpkSokInD79vzk
# eJkuDfcH4nC8GE6djmsKcpW4oTmcZy3FUQ7qYlw/FpiLID/iBxoy+cwxSnYxPSty
# C8jqcD3/hQoT38IKYY7w17gX606Lf8U1K16jv+u8fQtCe9RTciHuMMq7eGVcWwEX
# ChQO0toUmPU8uWZYsy0v5/mFhsxRVuidcJRsrDlM1PZ5v6oYemIp76KbKTQGdxpi
# yT0ebR+C8AvHLLvPQ7Pl+ex9teOkqHQ1uE7FcSMSJnYLPFKMcVpGQxS8s7OwTWfI
# n0L/gHkhgJ4VMGboQhJeGsieIiHQQ+kr6bv0SMws1NgygEwmKkgkX1rqVu+m3pmd
# yjpvvYEndAYR7nYhv5uCwSdUtrFqPYmhdmG0bqETpr+qR/ASb/2KMmyy/t9RyIwj
# yWa9nR2HEmQCPS2vWY+45CHltbDKY7R4VAXUQS5QrJSwpXirs6CWdRrZkocTdSIv
# MqgIbqBbjCW/oO+EyiHW6x5PyZruSeD3AWVviQt9yGnI5m7qp5fOMSn/DsVbXNhN
# G6HY+i+ePy5VFmvJE6P9MIIGkTCCBHmgAwIBAgITVwCErVHzF6lNzY21SAABAISt
# UTANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDEwpNU0lUIENBIFoxMB4XDTE5MDYw
# NTIwMzYyMVoXDTIwMDYwNDIwMzYyMVowgYgxCzAJBgNVBAYTAlVTMRMwEQYDVQQI
# EwpXYXNoaW5ndG9uMRAwDgYDVQQHEwdSZWRtb25kMR4wHAYDVQQKExVNaWNyb3Nv
# ZnQgQ29ycG9yYXRpb24xMjAwBgNVBAMTKU1pY3Jvc29mdCBDb3Jwb3JhdGlvbiAo
# SW50ZXJuYWwgVXNlIE9ubHkpMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
# AQEAkDRtn5NhCb0MiQYKuThcrEhcKk6wGr17Tadv2cH4J/t15JO2J2ocpZrlZVKI
# k5RCsEvvXEqyoXFZ1K0UvXRZ/bUw5cr+W3vvgMKTJjY0iawTRBrGrNb5C/JNRMro
# aD5RYtdgMYEsY7bt8owJcuvAZtfB5UeSGp++44Mx7qgm6K0OJEGrRU0MYY92V3je
# AWVkFO2eejePsYKIlr/FnH8Io3fuvvp1bRaV4HlGtIDOS1M/KISKY9wggqMRw0+R
# nvYBRVtCE1bz5AY6HWRG8ezmnzh4sWSA05cy6S5A0DQu0EuZyGkzl7KQ6yF3XlT9
# galqN1P5x/j7C+KMG53tyGnPswIDAQABo4ICZDCCAmAwCwYDVR0PBAQDAgeAMBMG
# A1UdJQQMMAoGCCsGAQUFBwMDMB0GA1UdDgQWBBRoMO5QfbZGfvwPrHCNoplQahPC
# BzBQBgNVHREESTBHpEUwQzEpMCcGA1UECxMgTWljcm9zb2Z0IE9wZXJhdGlvbnMg
# UHVlcnRvIFJpY28xFjAUBgNVBAUTDTIzMDg1Nis0NTU0MjIwHwYDVR0jBBgwFoAU
# dhoBm1bWeGQebVPhA4kcJn/UsYMwgb4GA1UdHwSBtjCBszCBsKCBraCBqoYoaHR0
# cDovL2NvcnBwa2kvY3JsL01TSVQlMjBDQSUyMFoxKDEpLmNybIY/aHR0cDovL21z
# Y3JsLm1pY3Jvc29mdC5jb20vcGtpL21zY29ycC9jcmwvTVNJVCUyMENBJTIwWjEo
# MSkuY3Jshj1odHRwOi8vY3JsLm1pY3Jvc29mdC5jb20vcGtpL21zY29ycC9jcmwv
# TVNJVCUyMENBJTIwWjEoMSkuY3JsMIGLBggrBgEFBQcBAQR/MH0wNAYIKwYBBQUH
# MAKGKGh0dHA6Ly9jb3JwcGtpL2FpYS9NU0lUJTIwQ0ElMjBaMSgxKS5jcnQwRQYI
# KwYBBQUHMAKGOWh0dHA6Ly93d3cubWljcm9zb2Z0LmNvbS9wa2kvbXNjb3JwL01T
# SVQlMjBDQSUyMFoxKDEpLmNydDA+BgkrBgEEAYI3FQcEMTAvBicrBgEEAYI3FQiH
# 2oZ1g+7ZAYLJhRuBtZ5hhfTrYIFdgZvHJ4aMmUcCAWQCAQkwGwYJKwYBBAGCNxUK
# BA4wDDAKBggrBgEFBQcDAzANBgkqhkiG9w0BAQsFAAOCAgEAOGpvjDZErPlnp8OO
# 9EJ8iEWgasFHtnp7jNCY3P4mKXPl/dsZopAHu2kXJ56UWIPQpzNUyQcvSyUNZNQp
# uXTLloTIXhE+uDo0BrKAKQnAvHwfUA+26nSsuT61NBHNgz96RtZ9YQvkfWKCUFnw
# wn7zgl7DqPnITPwOBPQhQp7W44I46PCOGI+jpcr/788oHbt/nnuH7mgHpbriHQNE
# 1g6PMIkHxYTUV/aZLgQNolH/Jo72a6p7PLv+bTXCs4eWrZDjVCBDec98GdjpbezG
# 74D67irrGndqvI1rYfH7VaLgkyccmFoulFPHSTl5Vhc8knYfKP0BeSE5yX63DKTn
# wqAi+5O4yHwuFfRfV25/fXb+pYm5q8eAJe71wh2EGIwf3+/sOvgC02Y/3xRVp7BL
# wSvtuCe6zyCcVx2FvmV2n62mB+wJ36DhkxlLk7lZ99N3gtB6rhavo5fNkV+G42xo
# MxlGP5JfHzSbBEUcJ98/1ueBDzHdxeYpzaW9jPzL5ASeEkJ9LpDJa1kOsNXgSMg0
# TqX3GH0e5NQGTrn5LiTa7Wl14LYrrLwtSuJ8m6Q64KRYGSEEi965wihOkIcrpwEi
# r1pjg4BH2mIWDVVQYy3rpDuHyOAcJnIUCnqOO5NkM8bX/h+pCkouCrYaP+Xu39x5
# hn2CO3DEwxPzf8LZ+RD+n4qPPQMwggcaMIIFAqADAgECAhNlAAAATfY2VSpH1lWc
# AAAAAABNMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIU1pY3Jvc29mdCBJbnRl
# cm5hbCBDb3Jwb3JhdGUgUm9vdDAeFw0xODA2MjgxNzUxNTBaFw0yMjA0MDYxNzUx
# NTBaMBUxEzARBgNVBAMTCk1TSVQgQ0EgWjEwggIiMA0GCSqGSIb3DQEBAQUAA4IC
# DwAwggIKAoICAQCZr03iYzNu0wZN8KYObZw4MCFKrelBmnJkC34+7as93SPMOWms
# rcRYVXuPBDmaR5jF5el10t2AoFNJ18XMKU+0QUd99/2EmmXJk177xpjZ5ac+bS3L
# iVQoJC1ABw7sIUcTQItb/Duvmgr4jMLqu6l3IxG8ngYEzemA58tkDpGN4IQfGrhD
# jsOAVCM4A2BCUD/1Gs08deYq64qj6BC4UxHXlxtF21qUQp2RIZRfTrEXGMCmeGJo
# n5+XLj6IH+i0w5SpIsnMeTq4sPXQbbCxuADDNaC6GWcguj+VZAKoaicBW2zmcYFu
# hI+Xgifmbtv8Wv8BVCcYA4tNDSxkQp+o0aW+T9yM9ZFbZAQliJemkltv25F96h7e
# kihjpfj2/chX+gw0fHcEJbZ38LonH0w+YPqCpwPwlkyKuFHeasfPO44zNaQYIy6l
# wTfLqAoqITlANvRZCI9Io1UbHLAL2fDHH5BgLIKzwHatGzcwolHow1/2ieBnJS0m
# CyQ/haMQR/gC5c3Sk+OYZLjd0XnRFhkHAY1fCYQsrf1p5QJclVFP1U6NEghPfTPi
# YbE3EVbLK+yGuymia9PsL4tpJJos7eXbVVyVQE84GU6a/18BbQEsbeTQ5HOu4wnJ
# Uh+BCmPf3nWY+npJSlCTbrJod/TCj2lpkBaCJgcjpyKMBfzGtSok3mYqBQIDAQAB
# o4ICSjCCAkYwEgYJKwYBBAGCNxUBBAUCAwEAATAjBgkrBgEEAYI3FQIEFgQUPjPr
# r3hi5vFZP4LFzxA0+Yk1nQkwHQYDVR0OBBYEFHYaAZtW1nhkHm1T4QOJHCZ/1LGD
# MGkGA1UdJQRiMGAGCCsGAQUFBwMDBggrBgEFBQcDDgYIKwYBBQUHAwEGBysGAQUC
# AwUGCisGAQQBgjcUAgIGCSsGAQQBgjcVBQYIKwYBBQUHAwIGCisGAQQBgjcqAgUG
# CisGAQQBgjcqAgYwGQYJKwYBBAGCNxQCBAweCgBTAHUAYgBDAEEwCwYDVR0PBAQD
# AgGGMBIGA1UdEwEB/wQIMAYBAf8CAQAwHwYDVR0jBBgwFoAU2+wKZKjOwA7piFQO
# 6cjexHhLemEwgaYGA1UdHwSBnjCBmzCBmKCBlaCBkoYgaHR0cDovL2NvcnBwa2kv
# Y3JsL21zaW50Y3JjYS5jcmyGN2h0dHA6Ly9tc2NybC5taWNyb3NvZnQuY29tL3Br
# aS9tc2NvcnAvY3JsL21zaW50Y3JjYS5jcmyGNWh0dHA6Ly9jcmwubWljcm9zb2Z0
# LmNvbS9wa2kvbXNjb3JwL2NybC9tc2ludGNyY2EuY3JsMHsGCCsGAQUFBwEBBG8w
# bTAsBggrBgEFBQcwAoYgaHR0cDovL2NvcnBwa2kvYWlhL21zaW50Y3JjYS5jcnQw
# PQYIKwYBBQUHMAKGMWh0dHA6Ly93d3cubWljcm9zb2Z0LmNvbS9wa2kvbXNjb3Jw
# L21zaW50Y3JjYS5jcnQwDQYJKoZIhvcNAQELBQADggIBAI5uLH3zC3JmyQLHaOFq
# cKpNMPeM5X40rgEi82eIR806yEt0SonUT4kS4FYgoI8xrRP7QA8wUbf0nhEJd96E
# 8pzJZxXtZVDVB5PMzVXEextiy0tBS55JIqZ/W8FadBG0Xozb9peYuFS3jXVV/jmf
# UFC5w72xhJMuaH8h+wQu9NvQutqriYTArwsBdeaOeF+JUYEnC1HEpl2seU8TUUxN
# sgn4uBI/5N1i4D/4Lo8SAJ89CBol0QdQ1l4W6qlTMKMebL6vrPof2o8eCEdnNzAI
# 1bvk75utuoF3X8yMJAkDQdwm+QaqiLxkvM2AAIQsUS7rt+6Ea46fNqmT2T2qc5va
# AbkNIchzrsPts2k1HOQkOqsfkJeCRkr52Ll6N4ZRxu9fPtLKAmy9rsYvYiZ58naf
# o/JH/93M6YE+0Acls4yiFdY8o9bn0oKglZ7/Bx+Kl4Hrbt+jiFmGxiASVHcYoHDk
# e/X0GzFwsXj16UdV130Gn0oOhZ1aSfA33hp4QjUP4B60ePAtazwdNuHAPItI6iDz
# azHGbmsBMdQ8ppiLple0kC+c40YTub1bDMdpxkK4LFb0djhDDfb1tQmdHzG2aP54
# qxsgf3/vc0TOK/R4o3JjbwL37r6UWJ59F6ORkSjghliwTZi3gv/SEFGrA4T8KcIP
# wzEKZBJoTvXafYF+bXtDRuEjMYIEPjCCBDoCAQEwLDAVMRMwEQYDVQQDEwpNU0lU
# IENBIFoxAhNXAIStUfMXqU3NjbVIAAEAhK1RMAkGBSsOAwIaBQCggbwwGQYJKoZI
# hvcNAQkDMQwGCisGAQQBgjcCAQQwHAYKKwYBBAGCNwIBCzEOMAwGCisGAQQBgjcC
# ARUwIwYJKoZIhvcNAQkEMRYEFJZrcqa4zmtoXJMU8Z3tT9GxsI4QMFwGCisGAQQB
# gjcCAQwxTjBMoDCALgBDAE0AUwBFACAARABCAEEAIAA4AGQAbwB0ADEAIABQAFMA
# IABKAG8AYgAgADKhGIAWaHR0cHM6Ly9taWNyb3NvZnQuY29tIDANBgkqhkiG9w0B
# AQEFAASCAQCOwss7cEQqjjNZEDMvuPSq5nu5nXdtvy3nrFA6MokDs/smF3DGUV7g
# opQUOa59CnzG9Dna8s3SaLmpx0cCIhverbbO01R/fM5s4D/qZSMMw3WYTrT47p+N
# 6QcesgeQjXizo6rvu9Zq1TCWUY8B81zlAcWlY8Xp9BB7sCXmIOgs0IUFz6sLoktj
# bVWLL6iL66Ltfuvmm3cAQyjBF+H+C22qqN0HheVlaXVOvg/gfAJUcTvhB2VV3hi0
# /txwoBapRRJIs9gvWKjSXw4EP2QTjeWMjQXkcVSmLTkIVB3MWoFWVi+4/TD/nwEU
# Lx+9npPrE1nzWcUkl23b2iE0mah0dL2BoYICKDCCAiQGCSqGSIb3DQEJBjGCAhUw
# ggIRAgEBMIGOMHcxCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpXYXNoaW5ndG9uMRAw
# DgYDVQQHEwdSZWRtb25kMR4wHAYDVQQKExVNaWNyb3NvZnQgQ29ycG9yYXRpb24x
# ITAfBgNVBAMTGE1pY3Jvc29mdCBUaW1lLVN0YW1wIFBDQQITMwAAAQWOyikiHmo0
# WwAAAAABBTAJBgUrDgMCGgUAoF0wGAYJKoZIhvcNAQkDMQsGCSqGSIb3DQEHATAc
# BgkqhkiG9w0BCQUxDxcNMTkwNzMwMjIxMjQ1WjAjBgkqhkiG9w0BCQQxFgQUfwHF
# YPexZVYdKJRPXc/gaEEExx8wDQYJKoZIhvcNAQEFBQAEggEAZyAZGTORmHP7oIfB
# i0TpHZvZ7nHpDwDH/W7g5zW4dsdqLhf9PenDdQBt45C3F/QAlwixlWZvlzfpTQj/
# RVJBzhEUzZTbbw4D3ZFm22VzaajLzwT/dgME9vKMO2uEv1ocFK+zGGKRRSLI1Qtc
# /PCHSK7btzx5mCBOupmnBLC4MKIJv32yHVjP6xeQPTVkrwY8F8Yw5zMgDSRRoASY
# tOQUqIUflfOX/ns+OtS714pnShkw8bfC3Gt6lmE+EfnKz7PMT44M6zItmagc7bha
# Lp4V2WR+mO9J5oLZ1IG6pZsZoh9mv/wzQIPQE75D7p+/neBaLws4r2ECAGgu0Ut+
# 6D/HMg==
# SIG # End signature block
