<#
.SYNOPSIS
    This script is used to help cleanup syscommittab via the supported SQL internal stored procedure "sp_flush_commit_table_on_demand".
.DESCRIPTION
    This script will run the official SQL sproc for cleaning up Syscommittab in order to cleanup Syscommittab. It tries to run the sproc until the sproc is actually
    triggered as it should be and performs the cleanup. If the cleanup is deadlocked it will retry up to 5 times. This was created due to CSS's strong recommendation
    that we use this internal procedure rather than our own (unsupported) methods. It doesn't work as well as a custom solution due to the bug(s) in the internal
    procedure "sp_flush_commit_table_on_demand". If there is an issue with the auto cleanup this can help but in really bad situations it may not work at all due to
    the bugs (which this script tries to work around).
.PARAMETER SqlServerName
    The name of the SQL Server to connect to. If this is defined the SqlConnString is not required.
.PARAMETER SqlDatabaseName
    The name of the Database to connect to. If this is defined the SqlConnString is not required.
.PARAMETER SqlConnString
    The entire connection string used to make the connection to SQL. If this is defined neither SqlServerName nor SqlDatabaseName are required.
.PARAMETER SqlCredentials
    This is a SqlClient.SqlCredential containing the user/password to use to make the connection to SQL. If this is not passed in the function will try to use the current user's credentials.
.PARAMETER DoNotLogToTable
    If this is passed in the script will not log to the CommandLog table.
.PARAMETER CleanupChangeTracking
    This correlates to the variable in "spDiagChangeTracking"; the default is "1" which means to delete the records rather than just check the status.
.PARAMETER CheckSideTableMinTime
    This correlates to the variable in "spDiagChangeTracking"; the default is "0" which means don't get the MinTime for the side tables. (This can take a long time and is unnecessary as part of the cleanup).
.PARAMETER RowsToDeletePerIteration
    This correlates to the variable in "spDiagChangeTracking"; the default is 500,000.
.EXAMPLE
    .\CleanSyscommittabWithCMSproc.ps1 -SqlServerName "SomeSqlSrvr" -SqlDatabaseName "CM_123";
    This will run the CM stored procedure against the "CM_123" database on "SomeSqlSrvr".
.EXAMPLE
    .\CleanSyscommittabWithCMSproc.ps1 -SqlConnString "Server=MySqlServer;Database=MyDb;Integrated Security=SSPI";
    This will run the CM stored procedure against the "MyDb" database on "MySqlServer".
.NOTES
    NAME: CleanSyscommittabWithCMSproc.ps1
    HISTORY:
        Date         Version    Author                  Notes:
        10/24/2018   ?.?        Benjamin Reynolds       Created more full script to log to CommandLog.
        01/24/2019   7.7        Benjamin Reynolds       Updated ErrorNumber if logic to properly capture the info.
        05/29/2019   7.7        Benjamin Reynolds       Added "Version" to Modification History. (Changes not significant to iterage version)
        07/30/2019   8.1        Benjamin Reynolds       Updated the AG 'is primary' check to use "fn_hadr_is_primary_replica" to be sure this script works with older versions of SQL.
                                                        Added official script comments; Script to be code signed.
#>
[cmdletbinding(PositionalBinding=$false)]
param (
    [Parameter(Mandatory=$true,ParameterSetName='NoConnString')][string]$SqlServerName
   ,[Parameter(Mandatory=$false,ParameterSetName='NoConnString')][string]$SqlDatabaseName = "master"
   ,[Parameter(Mandatory=$true,ParameterSetName='ConnString')][string]$SqlConnString
   ,[Parameter(Mandatory=$false,ParameterSetName='ConnString')][System.Data.SqlClient.SqlCredential]$SqlCredentials
   ,[Parameter(Mandatory=$false)][switch]$DoNotLogToTable
   ,[Parameter(Mandatory=$false)][ValidateSet(0,1)][byte]$CleanupChangeTracking = 1
   ,[Parameter(Mandatory=$false)][ValidateSet(0,1)][byte]$CheckSideTableMinTime = 0
   ,[Parameter(Mandatory=$false)][int]$RowsToDeletePerIteration = 500000
)

<##############################################################################################################################
    Function Declarations
##############################################################################################################################>
# Invoke-SqlCommand
function Invoke-SqlCommand {
<#
.SYNOPSIS
    This function executes a SQL command against a server/db and returns an object used to determine if it was successful or not.
    If the command is getting data (multiple columns/rows) then the data is returned as an object to the caller.
.DESCRIPTION
    The function executes a provided SQL command and returns whether it was successful or not along with any error that was captured. If the
    "ReturnTableData" switch is defined then all the data returned by the command will be returned as a data object to the caller.
    To determine the SQL Server and database to run the command against either the Server/DB need to be passed in or a SQL Connection String.
    If no "SqlCredentials" are passed in then the current user's credentials will be used to try and create the connection; otherwise the
    credentials securely stored in the SqlCredential will be used to make the connection.
.PARAMETER SqlServerName
    The name of the SQL Server to connect to. If this is defined the SqlConnString is not required
.PARAMETER SqlDatabaseName
    The name of the Database to connect to. If this is defined the SqlConnString is not required
.PARAMETER SqlConnString
    The entire connection string used to make the connection to SQL. If this is defined neither SqlServerName nor SqlDatabaseName are required.
.PARAMETER SqlCredentials
    This is a SqlClient.SqlCredential containing the user/password to use to make the connection to SQL. If this is not passed in the function will try to use the current user's credentials
.PARAMETER SqlCommandText
    This is the SQL DML/DDL desired to run against the SQL Server.
.PARAMETER ReturnTableData
    This is a switch that controls whether all the data from the DML command should be returned to the caller. If this is not passed in only the first row/column value will be returned rather than all the rows/columns.
.EXAMPLE
    Invoke-SqlCommand -SqlConnString "Server=MySqlServer;Database=MyDb;Integrated Security=SSPI" -SqlCommandText "SELECT @@VERSION;";
    This will run "SELECT @@VERSION;" against the SQL Server "MySqlServer" and Database "MyDb". The value returned will be in the return object's property "SqlColVal"
.EXAMPLE
    Invoke-SqlCommand -SqlServerName "MySqlServer" -SqlDatabaseName "MyDb" -SqlCommandText "SELECT @@VERSION;";
    This will run "SELECT @@VERSION;" against the SQL Server "MySqlServer" and Database "MyDb". The value returned will be in the return object's property "SqlColVal"
.EXAMPLE
    Invoke-SqlCommand -SqlConnString "Server=MySqlServer;Database=MyDb;Integrated Security=SSPI" -SqlCommandText "SELECT TOP 5 * FROM sys.objects;" -ReturnTableData;
    This will run "SELECT TOP 5 * FROM sys.objects;" against the SQL Server "MySqlServer" and Database "MyDb".
    The rows and columns returned will be in the return object's property "SqlTableData".
.EXAMPLE
    Invoke-SqlCommand -SqlConnString "Server=MySqlServer;Database=MyDb;Integrated Security=SSPI" -SqlCommandText "SELECT TOP 5 * FROM sys.objects;";
    This will run "SELECT TOP 5 * FROM sys.objects;" against the SQL Server "MySqlServer" and Database "MyDb". Although multiple rows and columns are returned by the command,
    since the "ReturnTableData" switch was not turned on, only the value from the first row and column will be returned. It will be in the return object's property "SqlColVal".
.OUTPUTS
    An object (ArrayList) with the following properties:
    -Value = either -1 (failure) or 0 (success) 
    -ErrorCaptured = this property contains the information if an error was caught  
    One of the following:
     -SqlColVal = if ReturnTableData is not passed in the command is run and if there is anything returned this property will contain the first column/row value
     -SqlTableData = if the ReturnTableData switch is used the data captured is returned in this property as an ArrayList of Hashtables
.NOTES
    NAME: Invoke-SqlCommand
    NOTE: Original function created for module "IntuneOperationalStoreFunctions"
    HISTORY:
        Date                Author                                         Notes:
        09/05/2018          Benjamin Reynolds (breynol@microsoft.com)      Initial Creation
        09/10/2018          Benjamin Reynolds (breynol@microsoft.com)      Adding Reader switch/capability.
        10/24/2018          Benjamin Reynolds (breynol@microsoft.com)      Added SqlAdapter capability.
#>
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true,ParameterSetName='NoConnString')][string]$SqlServerName
       ,[Parameter(Mandatory=$true,ParameterSetName='NoConnString')][string]$SqlDatabaseName
       ,[Parameter(Mandatory=$true,ParameterSetName='ConnString')][string]$SqlConnString
       ,[Parameter(Mandatory=$false,ParameterSetName='ConnString')][Alias("SqlCreds")][System.Data.SqlClient.SqlCredential]$SqlCredentials
       ,[Parameter(Mandatory=$true)][String]$SqlCommandText
       ,[Parameter(Mandatory=$false)][ValidateSet("MultipleDataSets","OneDataSet","OneValue")][string]$ReturnDataType = "OneValue"
       ,[Parameter(Mandatory=$false)][int]$SqlCommandTimeout
    )

    $ReturnObj = New-Object System.Collections.ArrayList;

    ## Create the connection to SQL:
    if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
        ## Use SqlConnectionStringBuilder to be able to validate the connection string:
        #$SqlConnBuilder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder -ArgumentList $SqlConnString;
        ## Reset the Connection String now that it's been validated:
        #$SqlConnString = $SqlConnBuilder.ConnectionString;
        #Remove-Variable -Name SqlConnBuilder -ErrorAction SilentlyContinue;
        
        # Determine whether we're using SqlCredentials or the current user:
        if ($SqlCredentials) {
            # Setup the SQL Connection using the Credentials passed in:
            $SqlConn = New-Object -TypeName System.Data.SqlClient.SqlConnection -ArgumentList $SqlConnString, $SqlCredentials;
        }
        else {
            # Setup the SQL Connection using the callers credentials:
            $SqlConn = New-Object -TypeName System.Data.SqlClient.SqlConnection -ArgumentList $SqlConnString;
        }
    } # End: Using "ConnString" ParameterSet
    else {
        ### This is for backward compatability stuff but could be removed later....
        ## Use SqlConnectionStringBuilder to be able to validate the connection string:
        #$SqlConnBuilder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder;
        ## Build the connection string with what was sent:
        #$SqlConnBuilder.'Data Source' = $SqlServerName;
        #$SqlConnBuilder.'Initial Catalog' = $SqlDatabaseName;
        #$SqlConnBuilder.'Integrated Security' = $true;
        #$SqlConnString = $SqlConnBuilder.ConnectionString;
        #Remove-Variable -Name SqlConnBuilder -ErrorAction SilentlyContinue;
        
        $SqlConnString = "Server={0};Database={1};Integrated Security=SSPI;" -f $SqlServerName,$SqlDatabaseName;
        
        # Setup the SQL Connection using the callers credentials:
        $SqlConn = New-Object -TypeName System.Data.SqlClient.SqlConnection -ArgumentList $SqlConnString;
    } # End: Using "NoConnString" ParameterSet
    
    
    ## Connect to SQL and get the data:
    $SqlConn.Open();
    $SqlCmd = New-Object -TypeName System.Data.SqlClient.SqlCommand -ArgumentList $SqlCommandText, $SqlConn;
    if ($SqlCommandTimeout) {
        $SqlCmd.CommandTimeout = $SqlCommandTimeout;
    }
    
    try {
        if ($ReturnDataType -eq "MultipleDataSets") {
            ## The sproc returns multiple result sets so let's capture all the data:
            $SqlTableData = New-Object -TypeName System.Data.DataSet;

            $SqlAdapter = New-Object -TypeName System.Data.SqlClient.SqlDataAdapter -ArgumentList $SqlCmd
            $SqlAdapter.Fill($SqlTableData);
        } # end using adapter (for returning all datasets returned)
        elseif ($ReturnDataType -eq "OneDataSet") {
            $SqlTableData = New-Object System.Collections.ArrayList;
            
            $SqlReader = $SqlCmd.ExecuteReader()
            while ($SqlReader.Read()) {
                $RowData = New-Object -TypeName System.Collections.Hashtable;
                for ($i = 0; $i -lt $SqlReader.FieldCount; $i++) {
                    $RowData[$SqlReader.GetName($i)] = $SqlReader.GetValue($i);
                }
                [void]$SqlTableData.Add($RowData);
                Remove-Variable -Name RowData,i -ErrorAction SilentlyContinue;
            }
            $SqlReader.Close()
        } # end using reader (if returning all data returned by the command)
        else {
            $SqlColVal = $SqlCmd.ExecuteScalar();
        }
    }
    catch {
        $SqlErrorCaptured = $PSItem;
    }
    finally { # Make sure to close the connection whether successful or not
        $SqlConn.Close();
    }

    ## Create the return object (include the error if one was caught):
    if ($SqlErrorCaptured) {
        if ($SqlColVal) {
            $TmpRtnObj = New-Object -TypeName PSObject -Property @{"SqlColVal"=$SqlColVal;"ErrorCaptured" = $SqlErrorCaptured;"Value" = -1};
        }
        elseif ($SqlTableData) {
            $TmpRtnObj = New-Object -TypeName PSObject -Property @{"SqlTableData"=$SqlTableData;"ErrorCaptured" = $SqlErrorCaptured;"Value" = -1};
        }
        else {
            $TmpRtnObj = New-Object -TypeName PSObject -Property @{"ErrorCaptured" = $SqlErrorCaptured;"Value" = -1};
        }
        [void]$ReturnObj.Add($TmpRtnObj);
        return $ReturnObj;
    }
    else {
        if ($SqlColVal) {
            $TmpRtnObj = New-Object -TypeName PSObject -Property @{"SqlColVal"=$SqlColVal;"Value" = 0};
        }
        elseif ($SqlTableData) {
            $TmpRtnObj = New-Object -TypeName PSObject -Property @{"SqlTableData"=$SqlTableData;"Value" = 0};
        }
        else {
            $TmpRtnObj = New-Object -TypeName PSObject -Property @{"Value" = 0};
        }
        [void]$ReturnObj.Add($TmpRtnObj);
        return $ReturnObj;
    }
} # End: Invoke-SqlCommand
<##############################################################################################################################
    End Function Declarations
##############################################################################################################################>

#################################################### Start of Script: ########################################################
Write-Verbose "Script Starting; creating/validating connection string...";

## Validate the connection string for SQL and/or build it:
if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
    ## Use SqlConnectionStringBuilder to be able to validate the connection string:
    $SqlConnBuilder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder -ArgumentList $SqlConnString;
}
else {
    ## Use SqlConnectionStringBuilder to build and validate the connection string:
    $SqlConnBuilder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder;
    $SqlConnBuilder.'Data Source' = $SqlServerName;
    $SqlConnBuilder.'Initial Catalog' = $SqlDatabaseName;
    $SqlConnBuilder.'Integrated Security' = $true;
}
## Create a non DAC and a DAC connection string to use for various calls to SQL:
if ($SqlConnBuilder.DataSource.StartsWith("admin:")) {
    [string]$SqlConnStringDAC = $SqlConnBuilder.ConnectionString;
    $SqlConnBuilder.'Data Source' = $SqlConnBuilder.DataSource.Replace('admin:','');
    [string]$SqlConnStringNonDAC = $SqlConnBuilder.ConnectionString;
    Remove-Variable -Name SqlConnBuilder -ErrorAction SilentlyContinue;
}
else {
    [string]$SqlConnStringNonDAC = $SqlConnBuilder.ConnectionString;
    $SqlConnBuilder.'Data Source' = "admin:$($SqlConnBuilder.DataSource)";
    [string]$SqlConnStringDAC = $SqlConnBuilder.ConnectionString;
    Remove-Variable -Name SqlConnBuilder -ErrorAction SilentlyContinue;
}

## Log to SQL Table - Start of Script:
if (-not $DoNotLogToTable) {
    # Build the insert command to run:
    [string]$SqlLogCmd = "IF OBJECT_ID(N'DBA.dbo.CommandLog') IS NOT NULL BEGIN INSERT INTO DBA.dbo.CommandLog (DatabaseName, Command, CommandType, StartTime) VALUES (DB_NAME(),N'EXECUTE dbo.spDiagChangeTracking $CleanupChangeTracking, $CheckSideTableMinTime, $RowsToDeletePerIteration;',N'SYSCOMMITTAB_CLEANUP_WITHCMSPROC','$((Get-Date).ToString("yyyy-MM-dd HH:mm:ss.fff"))'); SELECT SCOPE_IDENTITY() AS [ID]; END;";
    # Run the insert command:
    if ($SqlCredentials) {
        $SqlLogTblIdObj = Invoke-SqlCommand -SqlConnString $SqlConnStringNonDAC -SqlCredentials $SqlCredentials -SqlCommandText $SqlLogCmd;
    }
    elseif ($SqlConnStringNonDAC.Length -gt 1) { # check length just in case it's empty?
        $SqlLogTblIdObj = Invoke-SqlCommand -SqlConnString $SqlConnStringNonDAC -SqlCommandText $SqlLogCmd;
    }
    # Check if logging to SQL was successful or not:
    if ($SqlLogTblIdObj.Value -eq 0) {#successful
        $SqlLogTblId = $SqlLogTblIdObj.SqlColVal;
        Write-Verbose "We are logging to SQL with the ID of $SqlLogTblId";
    }
    Remove-Variable -Name SqlLogTblIdObj,SqlLogCmd,DoNotLogToTable -ErrorAction SilentlyContinue;
}

## Create the "cleanup" query that calls the sproc when the CM db exists and is not a SECONDARY in an AG:
$ExecCmd = "DECLARE @CMDB sysname;
SELECT @CMDB = name
  FROM sys.databases
 WHERE name LIKE N'CM[_]___'
   AND is_read_only = 0
   AND state_desc = N'ONLINE';
IF @CMDB IS NULL
BEGIN
    SELECT  GETDATE() AS [LocalTime]
           ,@@SERVERNAME AS [ServerName]
           ,-1 AS [ErrorNumber]
           ,'Aborting Procedure: No ConfigMgr database was found on this server!' AS [ErrorMessage]
END;
ELSE IF (SELECT ISNULL(sys.fn_hadr_is_primary_replica(@CMDB),1)) = 1
BEGIN
EXECUTE (N'
USE ['+@CMDB+']; 
SELECT GETUTCDATE() AS [StartDateTimeUTC];
EXECUTE dbo.spDiagChangeTracking $CleanupChangeTracking, $CheckSideTableMinTime, $RowsToDeletePerIteration;
SELECT GETUTCDATE() AS [EndDateTimeUTC];
');
END;
ELSE
SELECT  GETDATE() AS [LocalTime]
       ,@@SERVERNAME AS [ServerName]
       ,@CMDB AS [DatabaseName]
       ,0 AS [ErrorNumber]
       ,'Aborting Procedure: The ConfigMgr database is in an Availability Group and is not the PRIMARY so we''ll skip running the procedure.' AS [ErrorMessage];
";

Write-Verbose "Going to try and run the cleanup sproc...";

## Run the Cleanup command:
if ($SqlCredentials) {
    $SyscommittabInfo = Invoke-SqlCommand -SqlConnString $SqlConnStringDAC -SqlCredentials $SqlCredentials -SqlCommandText $ExecCmd -ReturnDataType MultipleDataSets -SqlCommandTimeout 86400;
}
elseif ($SqlConnStringDAC.Length -gt 6) { # check length just in case it's empty?
    $SyscommittabInfo = Invoke-SqlCommand -SqlConnString $SqlConnStringDAC -SqlCommandText $ExecCmd -ReturnDataType MultipleDataSets -SqlCommandTimeout 86400;
}

Write-Verbose "Now we'll check the information we received from calling the sproc...";

## Parse the information returned so we can properly log that to SQL and/or let the job know if we were successful or not:
if ($SyscommittabInfo.Value -eq 0) {#success
    ## if we got all tables then we successfully ran the sproc; else we hit one of the other conditions:
    if ($SyscommittabInfo.SqlTableData.Tables.Count -eq 6) {
        ## info from the 1st and 6th result sets (the start and end time we specifically created):
        $SprocStartTime = ($SyscommittabInfo.SqlTableData.Tables[0].StartDateTimeUTC).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fff");
        $SprocEndTime = ($SyscommittabInfo.SqlTableData.Tables[5].EndDateTimeUTC).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fff");
        
        ## db name from the 2nd result set (1st from sproc):
        $DatabaseName = $SyscommittabInfo.SqlTableData.Tables[1].DBName;

        ## info from the 3rd result set:
        $SyscommittabTotalRowCount = $SyscommittabInfo.SqlTableData.Tables[2].Syscommittab_RowCount;
        #$CTMinTimeLocal = $SyscommittabInfo.SqlTableData.Tables[2].'CT_Min_Time_(Local)';
        #....

        ## info from 4th result set (##SPDiagCleanupCT):
        [int64]$SyscommittabRowCount = 0;
        [int64]$SyscommittabRowsBeyondRetention = 0;
        [int64]$SyscommittabAllRowsBeyondRetention = 0;
        [int64]$SyscommittabNumRowsOrphaned = 0;
        [int64]$SyscommittabNumRowsDeleted = 0;
        [int64]$SyscommittabErrorCount = 0;
        [int64]$SideTableRowCount = 0;
        [int64]$SideTableRowsBeyondRetention = 0;
        [int64]$SideTableAllRowsBeyondRetention = 0;
        [int64]$SideTableNumRowsOrphaned = 0;
        [int64]$SideTableNumRowsDeleted = 0;
        [int64]$SideTableErrorCount = 0;
        ## Summarize the record counts:
        foreach ($rec in $SyscommittabInfo.SqlTableData.Tables[3]) {
            if ($rec.TableName -eq "sys.syscommittab") {
                $SyscommittabRowCount += $rec.CTRowCount;
                $SyscommittabRowsBeyondRetention += $rec.RowsBeyondRetention;
                $SyscommittabAllRowsBeyondRetention += $rec.AllRowsBeyondRetention;
                $SyscommittabNumRowsOrphaned += $rec.NumRowsOrphaned;
                $SyscommittabNumRowsDeleted += $rec.NumRowsDeleted;
                $SyscommittabErrorCount += $rec.ErrorCount;
            }
            else {
                $SideTableRowCount += $rec.CTRowCount;
                $SideTableRowsBeyondRetention += $rec.RowsBeyondRetention;
                $SideTableAllRowsBeyondRetention += $rec.AllRowsBeyondRetention;
                $SideTableNumRowsOrphaned += $rec.NumRowsOrphaned;
                $SideTableNumRowsDeleted += $rec.NumRowsDeleted;
                $SideTableErrorCount += $rec.ErrorCount;
            }
        }
        Remove-Variable -Name rec -ErrorAction SilentlyContinue;

        ## info from the 5th result set:
        $SprocExecutionTimeMinutes = $SyscommittabInfo.SqlTableData.Tables[4].'Execution Time Final (minutes)';

        ## Create the ExtendedInfo string for the xml column:
        [string]$ExtendedInfo = "<CleanupInfo>";
        $ExtendedInfo += "<SprocStartDateTimeUTC>$SprocStartTime</SprocStartDateTimeUTC>";
        $ExtendedInfo += "<SprocEndDateTimeUTC>$SprocEndTime</SprocEndDateTimeUTC>";
        $ExtendedInfo += "<SyscommittabTotalRowCount>$SyscommittabTotalRowCount</SyscommittabTotalRowCount>";
        $ExtendedInfo += "<SyscommittabRowCount>$SyscommittabRowCount</SyscommittabRowCount>";
        $ExtendedInfo += "<SyscommittabRowsBeyondRetention>$SyscommittabRowsBeyondRetention</SyscommittabRowsBeyondRetention>";
        $ExtendedInfo += "<SyscommittabAllRowsBeyondRetention>$SyscommittabAllRowsBeyondRetention</SyscommittabAllRowsBeyondRetention>";
        $ExtendedInfo += "<SyscommittabNumRowsOrphaned>$SyscommittabNumRowsOrphaned</SyscommittabNumRowsOrphaned>";
        $ExtendedInfo += "<SyscommittabNumRowsDeleted>$SyscommittabNumRowsDeleted</SyscommittabNumRowsDeleted>";
        $ExtendedInfo += "<SyscommittabErrorCount>$SyscommittabErrorCount</SyscommittabErrorCount>";
        $ExtendedInfo += "<SideTableRowCount>$SideTableRowCount</SideTableRowCount>";
        $ExtendedInfo += "<SideTableRowsBeyondRetention>$SideTableRowsBeyondRetention</SideTableRowsBeyondRetention>";
        $ExtendedInfo += "<SideTableAllRowsBeyondRetention>$SideTableAllRowsBeyondRetention</SideTableAllRowsBeyondRetention>";
        $ExtendedInfo += "<SideTableNumRowsOrphaned>$SideTableNumRowsOrphaned</SideTableNumRowsOrphaned>";
        $ExtendedInfo += "<SideTableNumRowsDeleted>$SideTableNumRowsDeleted</SideTableNumRowsDeleted>";
        $ExtendedInfo += "<SideTableErrorCount>$SideTableErrorCount</SideTableErrorCount>";
        $ExtendedInfo += "<SprocExecutionTimeMinutes>$SprocExecutionTimeMinutes</SprocExecutionTimeMinutes>";
        $ExtendedInfo += "</CleanupInfo>";

        Remove-Variable -Name SprocStartTime,SprocEndTime,SyscommittabTotalRowCount,SyscommittabRowCount,SyscommittabRowsBeyondRetention,SyscommittabAllRowsBeyondRetention,SyscommittabNumRowsOrphaned,SyscommittabNumRowsDeleted,SyscommittabErrorCount,SideTableRowCount,SideTableRowsBeyondRetention,SideTableAllRowsBeyondRetention,SideTableNumRowsOrphaned,SideTableNumRowsDeleted,SideTableErrorCount,SprocExecutionTimeMinutes -ErrorAction SilentlyContinue;
    }
    else {
        ## Set the Error Number/Message Values:
        $ErrorNumber = $SyscommittabInfo.SqlTableData.Tables[0].ErrorNumber;
        $ErrorMessage = $SyscommittabInfo.SqlTableData.Tables[0].ErrorMessage.Replace("'","''");
        $DatabaseName = $SyscommittabInfo.SqlTableData.Tables[0].DatabaseName;        
        
        #[string]$ExtendedInfo = "<CleanupInfo>";
        #foreach ($col in $SyscommittabInfo.SqlTableData.Tables[0].Columns) {
        #    if ($col.DataType -eq 'System.DateTime') {
        #        $ExtendedInfo += "<$($col.ColumnName)>$(($SyscommittabInfo.SqlTableData.Tables[0]."$($col.ColumnName)").ToString("yyyy-MM-ddTHH:mm:ss.fff"))</$($col.ColumnName)>";
        #    }
        #    else {
        #        $ExtendedInfo += "<$($col.ColumnName)>$($SyscommittabInfo.SqlTableData.Tables[0]."$($col.ColumnName)")</$($col.ColumnName)>";
        #    }
        #}
        #Remove-Variable -Name col -ErrorAction SilentlyContinue;
        #$ExtendedInfo += "</CleanupInfo>";
    }
}
else {#failure
    #ErrorCaptured
    $ErrorNumber = -9;
    [string]$ErrorMessage = $SyscommittabInfo.ErrorCaptured.Exception;
	$ErrorMessage = $ErrorMessage.Replace("'","''");
}
Remove-Variable -Name SyscommittabInfo -ErrorAction SilentlyContinue;

Write-Verbose "Finished parsing the information; now going to handle it appropriately...";

## Log the completion of the script:
if ($SqlLogTblId) {
    # Build the insert command to run:
    [string]$SqlLogCmd = "UPDATE DBA.dbo.CommandLog SET EndTime = '$((Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fff"))'";
    if ($DatabaseName) {
        $SqlLogCmd += ",DatabaseName = N'$DatabaseName'";
    }
    if ($ErrorNumber -ne $null) {
        $SqlLogCmd += ",ErrorNumber = $ErrorNumber,ErrorMessage = N'$ErrorMessage'";
    }
    if ($ExtendedInfo) {
        $SqlLogCmd += ",ExtendedInfo = '$ExtendedInfo'";
    }
    $SqlLogCmd += " WHERE ID = $SqlLogTblId;";

    # Run the insert command:
    if ($SqlCredentials) {
        $SqlLogTblIdObj = Invoke-SqlCommand -SqlConnString $SqlConnStringNonDAC -SqlCredentials $SqlCredentials -SqlCommandText $SqlLogCmd;
    }
    elseif ($SqlConnStringNonDAC.Length -gt 1) { # check length just in case it's empty?
        $SqlLogTblIdObj = Invoke-SqlCommand -SqlConnString $SqlConnStringNonDAC -SqlCommandText $SqlLogCmd;
    }
    # Check if logging to SQL was successful or not:
    if ($SqlLogTblIdObj.Value -eq 0) {#successful
        #Write-Verbose "Script Completed Successfully and logged to SQL";
        return "Script Completed Successfully";
    }
    else {
        #Write-Verbose "Script completed but failed with the following error trying to log the completion to SQL:`r`n$($SqlLogTblIdObj.ErrorCaptured)";
        throw "Script completed but failed with the following error trying to log the completion to SQL:`r`n$($SqlLogTblIdObj.ErrorCaptured)";
    }
    Remove-Variable -Name SqlLogTblIdObj,SqlLogCmd,SqlLogTblId -ErrorAction SilentlyContinue;
}
else {
    if ($ErrorNumber -ne $null -or $ErrorMessage -ne $null) {
        #Write-Verbose "Script returned the following error!:`r`n$ErrorMessage"
        throw "Script returned the following error!:`r`n$ErrorMessage";
    }
    else {
        #Write-Verbose "Script Completed Successfully";
        return "Script Completed Successfully";
    }
}
Remove-Variable -Name ErrorNumber,ErrorMessage,ExtendedInfo,DatabaseName -ErrorAction SilentlyContinue;




<#
$DACTestCmd = 'SELECT  con.session_id
       ,con.endpoint_id
       ,ept.name
       ,ept.is_admin_endpoint
       ,con.connect_time
       ,prc.loginame
       ,prc.hostname
  FROM sys.dm_exec_connections con
       INNER JOIN sys.endpoints ept
          ON con.endpoint_id = ept.endpoint_id
       INNER JOIN sys.sysprocesses prc
          ON con.session_id = prc.spid
         AND prc.ecid = 0
 WHERE ept.endpoint_id = 1;';
#>

## $((Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fff"))
