#Requires -Version 5.0
Clear-Host
Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Script Starting" -ForegroundColor Cyan
Write-Host ""

<#####
 Go through and check all the Remove-Variable's to make sure everything is covered and to remove those listed that may not exist anymore!
######>

## Variables ##
#$OwnDatabase = $true
$BaseURL = 'https://graph.microsoft.com/v1.0' #v1.0
#$EncryptedSQLConnString = 'C:\Users\breynol\Desktop\SQLAzureConnString.txt'
$SqlServerName = 'CMCASSQL'
$SqlDatabaseName = 'IntuneOpData_v1'
$SqlSchemaName =  'dbo'
$WriteBatchSize = 100000000
$DrillDownWriteBatchSize = 50000
$VerboseRecordCount = 25000
$SqlConnTimeout=240
$SqlTimeout=28800
$ApplicationId='b78eaaf9-18b8-49c7-93fa-77d96d729253'
$User='cmauto@microsoft.com'
$CredentialsFile = 'C:\Users\breynol\Desktop\CMAutoEncrptyedPassword.txt' # path to encrypted password for Graph Authentication
#$DataStoreURL='https://graph.microsoft.com/'
$RedirectUri='urn:ietf:wg:oauth:2.0:oob'
$SqlLoggingTableName = 'PowerShellRefreshHistory'
$SqlLoggingByTableName = 'TableRefreshHistory'

#$UseTestTables = "Test"

<##  Sync Table Format:
      -UriPart - Required
      -ExpandColumns - Optional
      -ExpandTableOrColumn - For use with "ExpandColumns"; Valid values: "Column", "Table", or "Both"
      -DrillDownTable - Optional
##>

## Add tables to this array to sync ##
$TablesToSync = @(
 @{"UriPart" = "deviceManagement/managedDevices"<#; "DrillDownTable" = ,@{"DrillDownData" = "deviceCompliancePolicyStates"}#>}
,@{"UriPart" = "deviceAppManagement/mobileApps"}
,@{"UriPart" = "deviceManagement/deviceCompliancePolicies"; "ExpandColumns" = "assignments"; "ExpandTableOrColumn" = "Both"<#; "DrillDownTable" = ,@{"DrillDownData" = "deviceStatuses";"UseSkipCountToken" = "true";"PageSize" = "100"}<#,@{"DrillDownData" = "deviceStatusOverview"}#>}
,@{"UriPart" = "deviceManagement/deviceConfigurations"}
,@{"UriPart" = "deviceManagement/deviceCompliancePolicySettingStateSummaries"}
)

## Log to SQL Table - starting
  ## FIX FOR ConnectionStrings and such...
$SqlLogTblIdObj = Start-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingTableName
if ($SqlLogTblIdObj.Value -eq 0) {#successful
    $SqlLogTblId = $SqlLogTblIdObj.ID.ID
    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged the start of the refresh to SQL table; Log ID = $SqlLogTblId" -ForegroundColor Cyan
}
else {# Value will equal -1 for errors...can check the "ErrorCaptured" property to know what happened
    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Logging to SQL table failed! Will continue running script but the refresh will not be logged!" -ForegroundColor Yellow
}

## If we've got a connection string file we'll get that for use in connecting to SQL:
if ($EncryptedSQLConnString) {
    if (Test-Path $EncryptedSQLConnString) {
        # Example Conn String for Azure: Server=tcp:[server].database.windows.net;Database=[db];User ID=[user];Password=[pword];Trusted_Connection=False;Encrypt=True;
        $SecureSQLConnString = Get-Content $EncryptedSQLConnString | ConvertTo-SecureString
        #$SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SecureSQLConnString
        <#$ConnServer = (($SqlCred.GetNetworkCredential().Password -split ';') -match "Server=").Replace('Server=tcp:','').Replace(',1433','')
        $ConnDatabase = (($SqlCred.GetNetworkCredential().Password -split ';') -match "Database=").Replace('Database=','')
        $ConnPassword = (($SqlCred.GetNetworkCredential().Password -split ';') -match "Password=").Replace('Password=','')
        $ConnUserName = (($SqlCred.GetNetworkCredential().Password -split ';') -match "User ID=").Replace('User ID=','')#>
    }
}

<#
# Run this if need to handle the invoke-webrequest issue?
# "C:\Program Files\internet explorer\iexplore.exe"

# Make sure we have the SqlServer module installed
    $SqlSrvrModule = Get-Module -ListAvailable -Name SqlServer | Sort-Object Version -Descending
    # Maybe look at deleting this module! ## delete the folders to make sure it's gone?
    #Get-Module -ListAvailable -Name SQLPS

    if ($SqlSrvrModule -eq $null) {
        $RunningAsAdmin = [bool](([System.Security.Principal.WindowsIdentity]::GetCurrent()).groups -match "S-1-5-32-544")
        
        if ($RunningAsAdmin) {
            Install-Module -Name SqlServer
        }
        else {
            Install-Module -Name SqlServer -Scope CurrentUser
        }
    }

    if (!(($SqlSrvrModule[0].Version.Major -eq 21) -and ($SqlSrvrModule[0].Version.Build -ge 17099)) -or ($SqlSrvrModule[0].Version.Major -gt 21)) {
        Update-Module SqlServer
    }
    <#else {
        $RunningAsAdmin = [bool](([System.Security.Principal.WindowsIdentity]::GetCurrent()).groups -match "S-1-5-32-544")
        if ($RunningAsAdmin) {
            Update-Module SqlServer -Confirm
        }
        <#else {
            Write-Host "You should consider updating the SqlServer Module. You can do this by running 'Update-Module SqlServer' in an elevated prompt." -ForegroundColor Yellow
        } # >
    }# >

    # Let's make sure to import the module too:
    $SqlSrvrModule = Get-Module -Name SqlServer
    if (!$SqlSrvrModule) {
        Import-Module -Name SqlServer
    }
#>


## Ensure the functions are loaded - requires the functions to be in a module...:
if (!(Get-Module -Name IntuneOperationalStoreFunctions)) {
    #Write-Host "Required Module Missing; Going to Import it" -ForegroundColor Yellow
    try {
        Import-Module IntuneOperationalStoreFunctions
    }
    catch {
        throw "The module 'IntuneOperationalStoreFunctions' is missing. Import this module in order to continue!"
    }
}

<###
    NEED TO ADD/MODIFY THE FOLLOWING IN THIS SCRIPT TO BETTER AUTOMATE:

? Create into a parameterized script???
? Create a module manifest for the module ?
? Add logging to a file or table?
Add - ability to use secure connection strings for SQL - for Azure?
###>

## Variable Validations:
if (!$User) {$User = "$env:USERNAME@microsoft.com"}

## Create "GetAuthStringCmd":
# do:? if (!$GetAuthStringCmd) {...}
if ($CredentialsFile -eq $null) {
    if ($ApplicationId -and $RedirectUri) {
        $GetAuthStringCmd = "Get-Authentication -ApplicationId '$ApplicationId' -RedirectUri '$RedirectUri'"
    }
    else {
        $GetAuthStringCmd = "Get-Authentication"
    }
}
else {
    if ($ApplicationId -and $RedirectUri) {
        $GetAuthStringCmd = "Get-Authentication -ApplicationId '$ApplicationId' -User '$User' -CredentialsFile '$CredentialsFile' -RedirectUri '$RedirectUri'"
    }
    else {
        $GetAuthStringCmd = "Get-Authentication -User '$User' -CredentialsFile '$CredentialsFile'"
    }
} # final check:
if (!$GetAuthStringCmd) {$GetAuthStringCmd = "Get-Authentication"}

## Connect
if (!$global:ADAuthResult) {
    if ($GetAuthStringCmd -like "*CredentialsFile*") {
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Connecting to Graph using Creds file..." -ForegroundColor Cyan
        Invoke-Expression $GetAuthStringCmd
    }
    else {
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Connecting to Graph using current user..." -ForegroundColor Cyan
        Invoke-Expression $GetAuthStringCmd
    }
}
else {
    # add a check for a timeout and connect if timed out...
    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : A Connection to Graph Has already been established...moving on..." -ForegroundColor Cyan
} # End of Connecting/Authenticating

## Get MetaData items
Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Getting MetaData items..." -ForegroundColor Cyan
Get-OperationalStoreMetaData -MetaDataUri "$BaseURL/`$metadata"
# Should these be global variables created by the above metadata call?
$Enums = Get-EntityTypeMetaData -EntityName "Enums"
$Entities = Get-EntityTypeMetaData -EntityName "EntityTypes"
Write-Host "*************************************************************************************" -ForegroundColor Cyan

## Create an array to store DrillDown Information to be handled later:
$DrillDownInfo = New-Object System.Collections.ArrayList

Write-Host "                    Processing Urls in the 'TablesToSync' Object                     " -ForegroundColor Cyan
Write-Host "*************************************************************************************" -ForegroundColor Cyan
Write-Host ""

foreach ($Table in $TablesToSync) {
    $UriPart = $Table.UriPart
    $ExpandCols = $Table.ExpandColumns
    $ExpandTableOrColumn = $Table.ExpandTableOrColumn
    $UseSkipCountToken = $Table.UseSkipCountToken
    $SkipCountPageSize = $Table.PageSize
    $DrillDownTable = $Table.DrillDownTable
    $UriParts = $UriPart -split "/"
    # Reverse the order of the array for now:
    [array]::Reverse($UriParts)

    $SqlTableName = $UriParts[0]
    if ($UseTestTables) {$SqlTableName = "$($SqlTableName)$UseTestTables"}
    $GraphMetaDataEntityName = ((Get-CollectionEntity -UrlPartsReversed $UriParts).NavigationProperty | ? {$_.Name -eq $UriParts[0]}).Type.Replace("Collection(","").Replace(")","")

    # ? - Do a check on ExpandCols to make sure they all exist? If not there will be an error in the Get call...which would be handled anyway but...

    # ? - Put the array back to original order?
    #[array]::Reverse($UriParts)

    # Get Sql Table Definition: NEED TO HANDLE THE CONNECTION STRING AS WELL??
    $SqlDefinition = Get-SqlTableColumnDefinition -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SqlSchemaName $SqlSchemaName -SqlTableName $SqlTableName

    # Make sure we have a table to work with; if not alert and go to the next entity:
    if (!$SqlDefinition) {
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : '$SqlTableName' DOES NOT EXIST! CREATE THE TABLE AND TRY AGAIN!" -ForegroundColor Yellow
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Skipping '$SqlTableName'... " -ForegroundColor Yellow
        break
    }

    # Get MetaData Column Definition for Comparisons:
    if (($ExpandCols) -and ($ExpandTableOrColumn -eq 'Column')) {
        $EntityColDef = Get-ColumnDefWithInheritedProps -GraphMetaDataEntityName $GraphMetaDataEntityName -ExpandedColumns $ExpandCols
    }
    elseif (($ExpandCols) -and ($ExpandTableOrColumn -eq 'Table')) {
        $EntityColDef = Get-ColumnDefWithInheritedProps -GraphMetaDataEntityName $GraphMetaDataEntityName
        # Get an object with the expanded columns' column definition for use in the batch loop?
        $ExpandedEntitiesColDef = Get-ExpandedColDefWithInheritedProps -GraphMetaDataEntityName $GraphMetaDataEntityName -ExpandedColumns $ExpandCols
        # Add the SqlTableName for each item:
        $ExpandedEntitiesColDef | % {$_ | Add-Member -MemberType NoteProperty -Name "ExpandedSqlTableName" -Value "$($SqlTableName)_$($_.ExpandedColName)"}
    }
    elseif (($ExpandCols) -and ($ExpandTableOrColumn -eq 'Both')) {
        $EntityColDef = Get-ColumnDefWithInheritedProps -GraphMetaDataEntityName $GraphMetaDataEntityName -ExpandedColumns $ExpandCols
        # Get an object with the expanded columns' column definition for use in the batch loop?
        $ExpandedEntitiesColDef = Get-ExpandedColDefWithInheritedProps -GraphMetaDataEntityName $GraphMetaDataEntityName -ExpandedColumns $ExpandCols
        # Add the SqlTableName for each item:
        $ExpandedEntitiesColDef | % {$_ | Add-Member -MemberType NoteProperty -Name "ExpandedSqlTableName" -Value "$($SqlTableName)_$($_.ExpandedColName)"}
    }
    else {
        $EntityColDef = Get-ColumnDefWithInheritedProps -GraphMetaDataEntityName $GraphMetaDataEntityName
    }

    # Alert if we don't have any metadata info and skip to the next item...:
    if (!$EntityColDef) {
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : COULD NOT FIND METADATA FOR '$GraphMetaDataEntityName'! PLEASE LOOK INTO THIS!" -ForegroundColor Yellow
        break
    }
    else { # DO THE COMPARISON OF THE DATA HERE!
        $SqlDefinition = Get-ColumnDefinitionsAndCompare -GraphMetaDataColumnDefinition $EntityColDef -SqlColumnDefinition $SqlDefinition
    } # End of Data Comparison ($EntityColDef exists)
    
    # Build the "$Select" portion of the Url and pass in???

    ### If we get data in batches this while loop will handle that:
    # Create the URL based on whether there are expanded columns or not:
    if ($ExpandCols) {
        $OdataURL = "$BaseURL/$($UriPart)?`$expand=$ExpandCols"
    }
    else {
        $OdataURL = "$BaseURL/$UriPart"
    }

    ## Create an array for this 'table' to take care of drill down data:
    if ($DrillDownTable) {
        $CurDrillDownInfo = New-Object System.Collections.ArrayList
    }

    #$TableImportCntArray = New-Object System.Collections.ArrayList
    #$TableImportCount = 0
    
    $IsBatchData = $false
    $CurRecordCount = 0

    ## Log to SQL Table - starting
     ## FIX FOR ConnectionStrings and such...
    $SqlLogByTblIdObj = Start-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingByTableName -TableName $SqlTableName
    if ($SqlLogByTblIdObj.Value -eq 0) {#successful
        $SqlLogByTblId = $SqlLogByTblIdObj.ID.ID
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged the start of the refresh of table '$SqlTableName' to SQL table; Log ID = $SqlLogByTblId" -ForegroundColor Cyan
    }
    else {# Value will equal -1 for errors...can check the "ErrorCaptured" property to know what happened
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Logging to SQL table failed! Will continue running script but the refresh will not be logged!" -ForegroundColor Yellow
    }

    while ($OdataURL) {

        # Determine whether we need to truncate/log the table or not in this iteration (if inserting
         # data in batches we only want to truncate/log the table on the first batch!)
        if (!$IsBatchData) {$TruncateSqlTable = $true}
        else {$TruncateSqlTable = $false}
        
        # Get the data:
        if ($UseSkipCountToken -eq "true") {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Getting data for '$UriPart' from Graph (using skipCount workaround) ..." -ForegroundColor Cyan
            $DtaObjFrmDS = Get-IntuneOpStoreDataUsingSkipCounts -OdataUrl $OdataURL -WriteBatchSize $WriteBatchSize -GetAuthStringCmd $GetAuthStringCmd -TopCount $SkipCountPageSize -VerboseInfo $true -VerboseRecordCount $VerboseRecordCount -CurNumRecords $CurRecordCount
        }
        else {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Getting data for '$UriPart' from Graph..." -ForegroundColor Cyan
            $DtaObjFrmDS = Get-IntuneOpStoreData -OdataUrl $OdataURL -WriteBatchSize $WriteBatchSize -GetAuthStringCmd $GetAuthStringCmd -VerboseInfo $true -VerboseRecordCount $VerboseRecordCount -CurNumRecords $CurRecordCount
        }

        $OdataURL = $DtaObjFrmDS.URL
        $CurRecordCount = $DtaObjFrmDS.RecordCount
        
        # if we don't have any records let's break out of the loop (and log the 'completion'); otherwise keep processing...
        if ($CurRecordCount -eq 0) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : No Records returned; Moving to next table..." -ForegroundColor Yellow
            ## Log to SQL Table - completion with an error...
              ## FIX FOR ConnectionStrings and such...
            if ($SqlLogByTblId) {
                Remove-Variable -Name SqlLogByTblIdObj -ErrorAction SilentlyContinue
                $SqlLogByTblIdObj = Update-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingByTableName -PK_ID $SqlLogByTblId -ErrorNumber -1 -ErrorMessage "No Records returned from the service for '$UriPart'"
                if ($SqlLogByTblIdObj.Value -eq 0) {
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged that no records returned for '$SqlSchemaName.$SqlTableName' to SQL table; Log ID = $SqlLogByTblId" -ForegroundColor Cyan
                }
                else {
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There was an error trying to log that no records returned for '$SqlSchemaName.$SqlTableName' to the SQL table! ; Log ID = $SqlLogByTblId" -ForegroundColor Yellow
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Returned from the Call:" -ForegroundColor Yellow
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($SqlLogByTblIdObj.ErrorCaptured)" -ForegroundColor Yellow
                }
            }
            Remove-Variable -Name SqlLogByTblIdObj,SqlLogByTblId -ErrorAction SilentlyContinue
            break
        }

        # Build/Fill CurDrillDownInfo with the valid Urls for the data we captured:
        if ($DrillDownTable) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Building the 'CurDrillDownInfo' object with all the valid Urls for the data captured..." -ForegroundColor Cyan
            foreach ($DrillDownItem in $DrillDownTable) {
                ($DtaObjFrmDS.DataObject | Select '@odata.type',id -Unique) | % {
                    $CurDrillObj = New-Object -TypeName PSObject -Property @{"UriPart" = $UriPart;"DrillDownData" = $DrillDownItem.DrillDownData;<#"UseSkipCountToken" = $DrillDownItem.UseSkipCountToken#>;"DrillDownUri" = "$($UriPart)/$($_.id)/$($DrillDownItem.DrillDownData)";"PId" = $_.id;"POType" = $_.'@odata.type'}
                    [void]$CurDrillDownInfo.Add($CurDrillObj)
                    Remove-Variable -Name CurDrillObj -ErrorAction SilentlyContinue
                }
            }
            Remove-Variable -Name DrillDownItem -ErrorAction SilentlyContinue
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Finished building the 'CurDrillDownInfo' object; total Urls added: $($CurDrillDownInfo.Count)" -ForegroundColor Cyan
        }

        # Convert the data we got from the service to a DataTable so that we can import it into SQL:
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Converting the data to a DataTable for SQL importing..." -ForegroundColor Cyan
        $DtaTbl = ConvertTo-DataTable -InputObject $DtaObjFrmDS.DataObject -ColumnDef $SqlDefinition
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : DataTable created: Columns = $($DtaTbl.Columns.Count); Rows = $($DtaTbl.Rows.Count)." -ForegroundColor Cyan
        
        #Only try to Truncate the table if it this is the first or only batch of data:
        if ($TruncateSqlTable) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Truncating the table '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
            <#if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
                $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
                Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query "IF OBJECT_ID(N'$SqlSchemaName.$SqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$SqlTableName;"
            }#>
            #else {
                Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query "IF OBJECT_ID(N'$SqlSchemaName.$SqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$SqlTableName;"
            #}
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Table Truncated." -ForegroundColor Cyan
        }
        else {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Writing data in batches...no need to truncate '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
        }
        
        # Write the data to SQL:
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Starting the import of the DataTable for '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
        # CAN"T USE THIS FOR AZURE DATABASES!!!
        Write-SqlTableData -ServerInstance $SqlServerName -DatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -TableName $SqlTableName -InputData $DtaTbl -Timeout $SqlTimeout -ConnectionTimeout $SqlConnTimeout -ErrorAction SilentlyContinue -ErrorVariable WriteSqlTableErrInfo
        
        # if we hit a failure (try/catch doesn't catch it) handle it here:
        if ($WriteSqlTableErrInfo) {
            # For some reason Write-Error was giving me strange results and pissed me off so I just went with Write-Host...
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Importing the records into SQL. Original Error is:" -ForegroundColor Red
            Write-Host $($WriteSqlTableErrInfo | Out-String) -ForegroundColor Red
            #### Log to table??? and then also break???
        }
        else {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Finished importing data for '$SqlSchemaName.$SqlTableName'. Records Imported: $($DtaTbl.Rows.Count)" -ForegroundColor Cyan
        }

        Remove-Variable -Name WriteSqlTableErrInfo,DtaTbl -ErrorAction SilentlyContinue

        ## Create an object to track the logging of the expanded tables to SQL:
        if ($SqlLogByTblId -and $ExpandedEntitiesColDef) {
            $ExpandedEntitiesLoggingByTblObj = New-Object System.Collections.ArrayList
        }

        ###########################################################################################
        ### Do separate table inserts here??? (for the batch cases)
          ### Currently not logging these tables to the refresh history table (by table one)....
        foreach ($ExpEnt in $ExpandedEntitiesColDef) {
            $CurSqlTableName = $ExpEnt.ExpandedSqlTableName
            # we already have the data so we just need to:
              # Get Sql definition?
            ## Get Sql Table Definition: NEED TO HANDLE THE CONNECTION STRING AS WELL??
            $CurSqlDefinition = Get-SqlTableColumnDefinition -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SqlSchemaName $SqlSchemaName -SqlTableName $CurSqlTableName

            if (!$CurSqlDefinition) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** '$CurSqlTableName' DOES NOT EXIST! CREATE THE TABLE AND TRY AGAIN!" -ForegroundColor Yellow
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** Skipping '$CurSqlTableName'... " -ForegroundColor Yellow
                break
            }

              # ? - Do a meta data comparison? Create a function for this and use above and here???
            $CurSqlDefinition = Get-ColumnDefinitionsAndCompare -GraphMetaDataColumnDefinition $ExpEnt.ColumnDefinition -SqlColumnDefinition $CurSqlDefinition
            
            # Start Logging to table?
            if ($SqlLogByTblId) {
                # 
                if ($TruncateSqlTable) {
                    $ExpEntLogByTblIdObj = Start-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingByTableName -TableName $CurSqlTableName
                    if ($ExpEntLogByTblIdObj.Value -eq 0) {#successful
                        $ExpEntLogByTblId = $ExpEntLogByTblIdObj.ID.ID
                        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged the start of the refresh of table '$CurSqlTableName' to SQL table; Log ID = $ExpEntLogByTblId" -ForegroundColor Cyan

                        $ExpObj = New-Object -TypeName PSObject -Property @{"TableName" = $CurSqlTableName;"PK_ID" = $ExpEntLogByTblId}
                        [void]$ExpandedEntitiesLoggingByTblObj.Add($ExpObj)
                        Remove-Variable -Name ExpObj -ErrorAction SilentlyContinue

                    }
                    else {# Value will equal -1 for errors...can check the "ErrorCaptured" property to know what happened
                        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Logging to SQL table failed! Will continue running script but the refresh will not be logged!" -ForegroundColor Yellow
                    }
                }
            }
            Remove-Variable -Name ExpEntLogByTblIdObj,ExpEntLogByTblId -ErrorAction SilentlyContinue

              # Create a data table
            
            # Create the Parent properties in the expanded column data:
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** blah blah blah..." -ForegroundColor Cyan
            $DtaObjFrmDS.DataObject | ? {$_."$($ExpEnt.ExpandedColName)" -ne $null} | % {
                # I had if statements here but that seemed to screw things up so I removed them...just make sure to reset the dataobject if running multiple times (via testing):
                $_."$($ExpEnt.ExpandedColName)" | Add-Member -MemberType NoteProperty -Name "ParentOdataType" -Value $_.'@odata.type'
                $_."$($ExpEnt.ExpandedColName)" | Add-Member -MemberType NoteProperty -Name "ParentId" -Value $_.id
            }
            $CurDtaObj = $DtaObjFrmDS.DataObject."$($ExpEnt.ExpandedColName)"
            # ? - Do this??
            #if ($CurDtaObj.Count -eq 0) {break}
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** Converting the Expanded Column data to a DataTable for SQL importing..." -ForegroundColor Cyan
            $DtaTbl = ConvertTo-DataTable -InputObject $CurDtaObj -ColumnDef $CurSqlDefinition
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** DataTable created: Columns = $($DtaTbl.Columns.Count); Rows = $($DtaTbl.Rows.Count)." -ForegroundColor Cyan
              
              # Write the data to Sql
            
            #Only try to Truncate the table if it this is the first or only batch of data:
            if ($TruncateSqlTable) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** Truncating the table '$SqlSchemaName.$CurSqlTableName'..." -ForegroundColor Cyan
                <#if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
                    $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
                    Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query "IF OBJECT_ID(N'$SqlSchemaName.$SqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$SqlTableName;"
                }#>
                #else {
                    Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query "IF OBJECT_ID(N'$SqlSchemaName.$CurSqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$CurSqlTableName;"
                #}
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** Table Truncated." -ForegroundColor Cyan
            }
            else {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** Writing data in batches...no need to truncate '$SqlSchemaName.$CurSqlTableName'..." -ForegroundColor Cyan
            }
            
            # Write the data to SQL:
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** Starting the import of the DataTable for '$SqlSchemaName.$CurSqlTableName'..." -ForegroundColor Cyan
            # CAN"T USE THIS FOR AZURE DATABASES!!! 
            Write-SqlTableData -ServerInstance $SqlServerName -DatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -TableName $CurSqlTableName -InputData $DtaTbl -Timeout $SqlTimeout -ConnectionTimeout $SqlConnTimeout -ErrorAction SilentlyContinue -ErrorVariable WriteSqlTableErrInfo
            
            # if we hit a failure (try/catch doesn't catch it) handle it here:
            if ($WriteSqlTableErrInfo) {
                # For some reason Write-Error was giving me strange results and pissed me off so I just went with Write-Host...
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** Error Importing the records into SQL. Original Error is:" -ForegroundColor Red
                Write-Host $($WriteSqlTableErrInfo | Out-String) -ForegroundColor Red
            }
            else {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : *** Finished importing data for '$SqlSchemaName.$CurSqlTableName'. Records Imported: $($DtaTbl.Rows.Count)" -ForegroundColor Cyan
            }
            
            Remove-Variable -Name WriteSqlTableErrInfo,DtaTbl,CurDtaObj,CurSqlTableName,CurSqlDefinition -ErrorAction SilentlyContinue
        }
        
        Remove-Variable -Name ExpEnt -ErrorAction SilentlyContinue

        ######################### End: Expanded Columns to Separate Tables Stuff ##################

        # Final checks/assignments (to know if getting data in batches):
        if ($DtaObjFrmDS.ErrorCaught -eq "true") {
            ## Log to SQL Table - completion with an error...
              ## FIX FOR ConnectionStrings and such...
            if ($SqlLogByTblId) {
                Remove-Variable -Name SqlLogByTblIdObj -ErrorAction SilentlyContinue
                $SqlLogByTblIdObj = Update-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingByTableName -PK_ID $SqlLogByTblId -ErrorNumber -1 -ErrorMessage "An exception was caught while getting data from the service for '$UriPart'"
                if ($SqlLogByTblIdObj.Value -eq 0) {
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged that an exception was caught for '$SqlSchemaName.$SqlTableName'; Log ID = $SqlLogByTblId" -ForegroundColor Cyan
                }
                else {
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There was an error trying to log that an exception was caught for '$SqlSchemaName.$SqlTableName'; Log ID = $SqlLogByTblId" -ForegroundColor Yellow
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Returned from the Call:" -ForegroundColor Yellow
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($SqlLogByTblIdObj.ErrorCaptured)" -ForegroundColor Yellow
                }
            }
            Remove-Variable -Name SqlLogByTblIdObj,SqlLogByTblId -ErrorAction SilentlyContinue
            break
        }
        if ($OdataURL) {$IsBatchData = $true}

        # Cleanup for the next loop in the while?
        Remove-Variable -Name DtaObjFrmDS -ErrorAction SilentlyContinue

    } # End While Loop (for OdataUrl)
    
    # ? - Let's give the total records imported (in case it was batched) now that we're done with the current table:
    #Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Finished importing All data for '$SqlSchemaName.$SqlTableName'. Total Records Imported: $CurRecordCount" -ForegroundColor Cyan

    
    ## Log to SQL Table - completion
      ## FIX FOR ConnectionStrings and such...
    if ($SqlLogByTblId) {
        # Expanded Column Table(s) Completion Logging:
        foreach ($ExpTblLogId in $ExpandedEntitiesLoggingByTblObj) {
            $ExpTblLogByTblIdObj = Update-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingByTableName -PK_ID $ExpTblLogId.PK_ID
            if ($ExpTblLogByTblIdObj.Value -eq 0) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged the refresh for '$SqlSchemaName.$($ExpTblLogId.TableName)'; Log ID = $($ExpTblLogId.PK_ID)" -ForegroundColor Cyan
            }
            else {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There was an error trying to log the completion of the refresh for '$SqlSchemaName.$($ExpTblLogId.TableName)'; Log ID = $($ExpTblLogId.PK_ID)" -ForegroundColor Yellow
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Returned from the Call:" -ForegroundColor Yellow
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($ExpTblLogByTblIdObj.ErrorCaptured)" -ForegroundColor Yellow
            }
            Remove-Variable -Name ExpTblLogByTblIdObj -ErrorAction SilentlyContinue
        }
        Remove-Variable -Name ExpTblLogId -ErrorAction SilentlyContinue

        # Initial Table Completion Logging:
        Remove-Variable -Name SqlLogByTblIdObj -ErrorAction SilentlyContinue
        $SqlLogByTblIdObj = Update-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingByTableName -PK_ID $SqlLogByTblId
        if ($SqlLogByTblIdObj.Value -eq 0) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged the refresh for '$SqlSchemaName.$SqlTableName'; Log ID = $SqlLogByTblId" -ForegroundColor Cyan
        }
        else {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There was an error trying to log the completion of the refresh for '$SqlSchemaName.$SqlTableName'; Log ID = $SqlLogByTblId" -ForegroundColor Yellow
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Returned from the Call:" -ForegroundColor Yellow
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($SqlLogByTblIdObj.ErrorCaptured)" -ForegroundColor Yellow
        }
    }
    
    # Now that we've got all the data taken care of get the unique records from "CurDrillDownInfo" into "DrillDownInfo" for processing later:
    if ($CurDrillDownInfo.Count -gt 0) {
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Building the 'DrillDownInfo' object now that all data has been captured (including if in batches) - getting only unique Urls..." -ForegroundColor Cyan
        ($CurDrillDownInfo | Select UriPart,DrillDownData,<#UseSkipCountToken,#>DrillDownUri,POType,PId -Unique) | % {
            $DrillObj = New-Object -TypeName PSObject -Property @{"UriPart" = $_.UriPart;"DrillDownData" = $_.DrillDownData;<#"UseSkipCountToken" = $_.UseSkipCountToken;#>"DrillDownUri" = $_.DrillDownUri;"POType" = $_.POType;"PId" = $_.PId}
            [void]$DrillDownInfo.Add($DrillObj)
            Remove-Variable -Name DrillObj -ErrorAction SilentlyContinue
        }
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Finished building the 'DrillDownInfo' object; total Urls added: $($DrillDownInfo.Count)" -ForegroundColor Cyan
    }

    # Cleanup Current 'table' item:
    Remove-Variable -Name ExpandedEntitiesColDef,EntityColDef,UriPart,UriParts,ExpandCols,ExpandTableOrColumn,UseSkipCountToken,SkipCountPageSize,SqlTableName,GraphMetaDataEntityName,SqlDefinition,OdataURL,IsBatchData,CurRecordCount,TruncateSqlTable,DrillDownTable,CurDrillDownInfo,SqlLogByTblIdObj,SqlLogByTblId,ExpandedEntitiesLoggingByTblObj -ErrorAction SilentlyContinue

} # End foreach table in TablesToSync

Write-Host ""
Write-Host "Next we'll process the drill down data tables if there are any...." -ForegroundColor Cyan
Write-Host ""

<##################################################################################################################
                                                    Next Steps
* Not fully tested!
* Not sure if the batch processing will actually work due to the multiple Url calls never really hitting the batch size
   probably need additional logic to handle that correctly.
* Need to check all output written to the host to see what changes need to be made...

##################################################################################################################>


## Process DrillDownInfo if it exists
if ($DrillDownInfo.Count -gt 0) {
    Write-Host "*************************************************************************************" -ForegroundColor Cyan
    Write-Host "                  Processing Urls in the 'Drill Down Table' Object                   " -ForegroundColor Cyan
    Write-Host "*************************************************************************************" -ForegroundColor Cyan
    Write-Host ""

    foreach ($DrillDown in ($DrillDownInfo | Select UriPart,DrillDownData <#,UseSkipCountToken#> -Unique)) {
        # Build the main variable info to be used for the unique UriPart DrillDownData combo:
        $UriPart = $DrillDown.UriPart
        $DrillDownData = $DrillDown.DrillDownData
        
        # Determine if we should use the SkipCount workaround or not:
        $UriPartPos = [array]::IndexOf($TablesToSync.UriPart,$UriPart)
        $DrlDwnDtaPos = [array]::IndexOf($TablesToSync[$UriPartPos].DrillDownTable.DrillDownData,$DrillDownData)
        $UseSkipCountToken = $TablesToSync[$UriPartPos].DrillDownTable[$DrlDwnDtaPos].UseSkipCountToken
        $SkipCountPageSize = $TablesToSync[$UriPartPos].DrillDownTable[$DrlDwnDtaPos].PageSize
        if (!$SkipCountPageSize) {$SkipCountPageSize = 100}

        $UriParts = $UriPart -split "/"
        # Reverse the order of the array for now:
        [array]::Reverse($UriParts)

        $SqlTableName = "$($UriParts[0])_$($DrillDownData)"
        if ($UseTestTables) {$SqlTableName = "$($SqlTableName)$UseTestTables"}
        
        $UriParts = "$UriPart/$DrillDownData" -split "/"
        # Reverse the order of the array for now:
        [array]::Reverse($UriParts)

        $GraphMetaDataEntityName = ((Get-CollectionEntity -UrlPartsReversed $UriParts).NavigationProperty | ? {$_.Name -eq $UriParts[0]}).Type.Replace("Collection(","").Replace(")","")

        # Get Sql Table Definition: NEED TO HANDLE THE CONNECTION STRING AS WELL??
        $SqlDefinition = Get-SqlTableColumnDefinition -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SqlSchemaName $SqlSchemaName -SqlTableName $SqlTableName

        # Make sure we have a table to work with; if not alert and go to the next entity:
        if (!$SqlDefinition) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : '$SqlTableName' DOES NOT EXIST! CREATE THE TABLE AND TRY AGAIN!" -ForegroundColor Yellow
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Skipping '$SqlTableName'... " -ForegroundColor Yellow
            break
        }

        # Get MetaData Column Definition for Comparisons:
        $EntityColDef = Get-ColumnDefWithInheritedProps -GraphMetaDataEntityName $GraphMetaDataEntityName
        # Now we need to add the parent properties/columns (hard coding) since this is drill down data:
        $CCD = New-Object System.Collections.ArrayList
        $CCO = New-Object -TypeName PSObject -Property @{"DataName" = "ParentOdataType";"Name" = "ParentOdataType";"Type" = "String";"Nullable" = "false";"IsCollection" = "false"}
        [void]$CCD.Add($CCO)
        Remove-Variable -Name CCO -ErrorAction SilentlyContinue
        $CCO = New-Object -TypeName PSObject -Property @{"DataName" = "ParentId";"Name" = "ParentId";"Type" = "String";"Nullable" = "false";"IsCollection" = "false"}
        [void]$CCD.Add($CCO)
        Remove-Variable -Name CCO -ErrorAction SilentlyContinue
        # Now add all the original properties/columns in:
        foreach ($c in $EntityColDef) {
            $CCO = New-Object -TypeName PSObject -Property @{"DataName" = $c.DataName;"Name" = $c.Name;"Type" = $c.Type;"Nullable" = $c.Nullable;"IsCollection" = $c.IsCollection}
            [void]$CCD.Add($CCO)
            Remove-Variable -Name CCO -ErrorAction SilentlyContinue
        }
        # Assign the new result set to what we expect:
        $EntityColDef = $CCD
        # Cleanup
        Remove-Variable -Name CCD,c -ErrorAction SilentlyContinue
        # End: getting MetaData Column Definition

        # Alert if we don't have any metadata info and skip to the next item...:
        if (!$EntityColDef) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : COULD NOT FIND METADATA FOR '$GraphMetaDataEntityName'! PLEASE LOOK INTO THIS!" -ForegroundColor Yellow
            break
        }
        else { # DO THE COMPARISON OF THE DATA HERE!
            $SqlDefinition = Get-ColumnDefinitionsAndCompare -GraphMetaDataColumnDefinition $EntityColDef -SqlColumnDefinition $SqlDefinition
        } # End: Data Comparison ($EntityColDef exists)
        
        $CurTblRecordCount = 0
        $IsBatchData = $false

        ## Log to SQL Table - starting
         ## FIX FOR ConnectionStrings and such...
        $SqlLogByTblIdObj = Start-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingByTableName -TableName $SqlTableName
        if ($SqlLogByTblIdObj.Value -eq 0) {#successful
            $SqlLogByTblId = $SqlLogByTblIdObj.ID.ID
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged the start of the refresh of table '$SqlTableName' to SQL table; Log ID = $SqlLogByTblId" -ForegroundColor Cyan
        }
        else {# Value will equal -1 for errors...can check the "ErrorCaptured" property to know what happened
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Logging to SQL table failed! Will continue running script but the refresh will not be logged!" -ForegroundColor Yellow
        }
        
        foreach ($DrillUriDtl in ($DrillDownInfo | ? {($_.UriPart -eq $UriPart) -and ($_.DrillDownData -eq $DrillDownData)})) {
            # 

            ### If we get data in batches this while loop will handle that:
            $OdataURL = "$BaseURL/$($DrillUriDtl.DrillDownUri)"
            
            $CurUrlRecordCount = 0
            
            while ($OdataURL) {

                # Determine whether we need to truncate the table or not in this iteration (if inserting
                 # data in batches we only want to truncate the table on the first batch!)
                if (!$IsBatchData) {$TruncateSqlTable = $true}
                else {$TruncateSqlTable = $false}

                
                # Get the data:
                if ($UseSkipCountToken -eq "true") {
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Getting Drill Down data for '$($DrillUriDtl.DrillDownUri)' from Graph (using skipCount workaround) ..." -ForegroundColor Cyan
                    $DtaObjFrmDS = Get-IntuneOpStoreDataUsingSkipCounts -OdataUrl $OdataURL -WriteBatchSize $DrillDownWriteBatchSize -GetAuthStringCmd $GetAuthStringCmd -TopCount $SkipCountPageSize -VerboseInfo $true -VerboseRecordCount $VerboseRecordCount -CurNumRecords $CurUrlRecordCount
                }
                else {
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Getting Drill Down data for '$($DrillUriDtl.DrillDownUri)' from Graph..." -ForegroundColor Cyan
                    $DtaObjFrmDS = Get-IntuneOpStoreData -OdataUrl $OdataURL -WriteBatchSize $DrillDownWriteBatchSize -GetAuthStringCmd $GetAuthStringCmd -VerboseInfo $true -VerboseRecordCount $VerboseRecordCount -CurNumRecords $CurUrlRecordCount
                }

                $OdataURL = $DtaObjFrmDS.URL
                $CurUrlRecordCount = $DtaObjFrmDS.RecordCount
                $CurUrlBatchCount = $DtaObjFrmDS.BatchRecordCount

                $CurTblRecordCount += $CurUrlBatchCount
                
                # if we don't have any records let's break out of the loop; otherwise keep processing...
                if ($CurUrlRecordCount -eq 0) {
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : No Records returned; Moving to next drill down table..." -ForegroundColor Yellow
                    ### Log this?? Probably not since there could be other Url's with data and we'll continue to get that info...
                    break
                }

                ### Create Parent columns
                if (($DtaObjFrmDS.DataObject <#$CurUrlBatchCount -gt 0#>) -and ($SqlDefinition.Name -like 'ParentOdataType' -or $SqlDefinition.Name -like 'ParentId')) {
                    $DtaObjFrmDS.DataObject | % {
                        $_ | Add-Member -MemberType NoteProperty -Name "ParentOdataType" -Value $DrillUriDtl.POType
                        $_ | Add-Member -MemberType NoteProperty -Name "ParentId" -Value $DrillUriDtl.PId
                    }
                }

                $CurTblDataObject += $DtaObjFrmDS.DataObject

                #if (($CurTblRecordCount % $DrillDownWriteBatchSize -eq 0) -or ($CurUrlRecordCount % $DrillDownWriteBatchSize -eq 0)) { # I need another OR statement here...but not sure what that is yet...
                if ($CurTblDataObject.Count -ge $DrillDownWriteBatchSize) {
                    # We've hit the WriteBatchSize so we need to write to SQL before continuing on...
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : We've hit the batch size for drill down data and need to sync with SQL before continuing..." -ForegroundColor Cyan
                    $IsBatchData = $true

                    # Convert the data we got from the service to a DataTable so that we can import it into SQL:
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Converting the data to a DataTable for SQL importing..." -ForegroundColor Cyan
                    $DtaTbl = ConvertTo-DataTable -InputObject $CurTblDataObject -ColumnDef $SqlDefinition
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : DataTable created: Columns = $($DtaTbl.Columns.Count); Rows = $($DtaTbl.Rows.Count)." -ForegroundColor Cyan
                    
                    #Only try to Truncate the table if this is the first or only batch of data:
                    if ($TruncateSqlTable) {
                        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Truncating the table '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
                        <#if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
                            $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
                            Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query "IF OBJECT_ID(N'$SqlSchemaName.$SqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$SqlTableName;"
                        }#>
                        #else {
                            Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query "IF OBJECT_ID(N'$SqlSchemaName.$SqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$SqlTableName;"
                        #}
                        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Table Truncated." -ForegroundColor Cyan
                    }
                    else {
                        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Writing data in batches...no need to truncate '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
                    }
                    
                    # Write the data to SQL:
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Starting the import of the DataTable for '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
                    # CAN"T USE THIS FOR AZURE DATABASES!!!
                    Write-SqlTableData -ServerInstance $SqlServerName -DatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -TableName $SqlTableName -InputData $DtaTbl -Timeout $SqlTimeout -ConnectionTimeout $SqlConnTimeout -ErrorAction SilentlyContinue -ErrorVariable WriteSqlTableErrInfo
                    
                    # if we hit a failure (try/catch doesn't catch it) handle it here:
                    if ($WriteSqlTableErrInfo) {
                        # For some reason Write-Error was giving me strange results and pissed me off so I just went with Write-Host...
                        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Importing the records into SQL. Original Error is:" -ForegroundColor Red
                        Write-Host $($WriteSqlTableErrInfo | Out-String) -ForegroundColor Red
                        ###### LOg to table and break??????
                    }
                    else {
                        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Finished importing data for '$SqlSchemaName.$SqlTableName'. Records Imported: $($DtaTbl.Rows.Count)" -ForegroundColor Cyan
                    }
                    Remove-Variable -Name WriteSqlTableErrInfo,DtaTbl,CurTblDataObject -ErrorAction SilentlyContinue

                } # End: Writing to Sql due to batch size hit

                # Final checks/assignments (to know if getting data in batches):
                # FIX THIS: if ($DtaObjFrmDS.ErrorCaught -eq "true") {break}
                if ($DtaObjFrmDS.ErrorCaught -eq "true") {
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : We hit/caught an error so will skip the current URL and try to continue processing the remaining data..." -ForegroundColor Yellow
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : 'Next' URL where error encountered: $OdataURL" -ForegroundColor Yellow
                    $ErrCaught = $true
                    $OdataURL = $null
                    #break
                }
                
                # Cleanup for the next loop in the while?
                Remove-Variable -Name DtaObjFrmDS,CurUrlBatchCount -ErrorAction SilentlyContinue

            } # End: While OdataUrl loop

            if ($ErrCaught) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Total Records Received (before error encountered) for Drill Down Url '$($DrillUriDtl.DrillDownUri)' = $CurUrlRecordCount" -ForegroundColor Cyan
            }
            else {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Total Records Received for Drill Down Url '$($DrillUriDtl.DrillDownUri)' = $CurUrlRecordCount" -ForegroundColor Cyan
            }

            # Cleanup:
            Remove-Variable -Name OdataURL,CurUrlRecordCount,ErrCaught -ErrorAction SilentlyContinue

        } # End foreach DrillUriDtl (aka unique graph url)

        # Cleanup:
        Remove-Variable -Name DrillUriDtl -ErrorAction SilentlyContinue

        # We've gotten all the data for the drill down Urls; check to see if we need to Write the data to SQL (either the first/only time or the last time):
        if ($CurTblDataObject) {
            # Determine whether we need to truncate the table or not in this iteration (if inserting
             # data in batches we only want to truncate the table on the first batch!)
            if (!$IsBatchData) {$TruncateSqlTable = $true}
            else {$TruncateSqlTable = $false}

            # Convert the data we got from the service to a DataTable so that we can import it into SQL:
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Converting the data to a DataTable for SQL importing..." -ForegroundColor Cyan
            $DtaTbl = ConvertTo-DataTable -InputObject $CurTblDataObject -ColumnDef $SqlDefinition
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : DataTable created: Columns = $($DtaTbl.Columns.Count); Rows = $($DtaTbl.Rows.Count)." -ForegroundColor Cyan
            
            #Only try to Truncate the table if this is the first or only batch of data:
            if ($TruncateSqlTable) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Truncating the table '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
                <#if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
                    $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
                    Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query "IF OBJECT_ID(N'$SqlSchemaName.$SqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$SqlTableName;"
                }#>
                #else {
                    Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query "IF OBJECT_ID(N'$SqlSchemaName.$SqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$SqlTableName;"
                #}
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Table Truncated." -ForegroundColor Cyan
            }
            else {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Writing data in batches...no need to truncate '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
            }
            
            # Write the data to SQL:
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Starting the import of the DataTable for '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
            # CAN"T USE THIS FOR AZURE DATABASES!!!
            Write-SqlTableData -ServerInstance $SqlServerName -DatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -TableName $SqlTableName -InputData $DtaTbl -Timeout $SqlTimeout -ConnectionTimeout $SqlConnTimeout -ErrorAction SilentlyContinue -ErrorVariable WriteSqlTableErrInfo
            
            # if we hit a failure (try/catch doesn't catch it) handle it here:
            if ($WriteSqlTableErrInfo) {
                # For some reason Write-Error was giving me strange results and pissed me off so I just went with Write-Host...
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Importing the records into SQL. Original Error is:" -ForegroundColor Red
                Write-Host $($WriteSqlTableErrInfo | Out-String) -ForegroundColor Red
                #### Log to table and break?????
            }
            else {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Finished importing data for '$SqlSchemaName.$SqlTableName'. Records Imported: $($DtaTbl.Rows.Count)" -ForegroundColor Cyan
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Total Records Imported for Drill Down Table '$SqlSchemaName.$SqlTableName' = $CurTblRecordCount" -ForegroundColor Cyan
            }
            Remove-Variable -Name WriteSqlTableErrInfo,DtaTbl,CurTblDataObject -ErrorAction SilentlyContinue
        }
        else {
            Write-Host "We shouldn't hit this...BUT WE CAN if the graph call doesn't return a 'value' property (in the Invoke-RestMethod)." -ForegroundColor Red
            Write-Host " For example, 'deviceStatusOverview' from deviceCompliancePolicies does not return a value property but rather " -ForegroundColor Red
            Write-Host " returns a single 'record' with the properties listed..." -ForegroundColor Red
        } # End: last writing to Sql for the Table

        ## Log to SQL Table - completion
          ## FIX FOR ConnectionStrings and such...
        if ($SqlLogByTblId) {
            Remove-Variable -Name SqlLogByTblIdObj -ErrorAction SilentlyContinue
            $SqlLogByTblIdObj = Update-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingByTableName -PK_ID $SqlLogByTblId
            if ($SqlLogByTblIdObj.Value -eq 0) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged the refresh for '$SqlSchemaName.$SqlTableName'; Log ID = $SqlLogByTblId" -ForegroundColor Cyan
            }
            else {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There was an error trying to log the completion of the refresh for '$SqlSchemaName.$SqlTableName'; Log ID = $SqlLogByTblId" -ForegroundColor Yellow
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Returned from the Call:" -ForegroundColor Yellow
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($SqlLogByTblIdObj.ErrorCaptured)" -ForegroundColor Yellow
            }
        }
        
        # Cleanup:
        Remove-Variable -Name UriPart,DrillDownData,UriPartPos,DrlDwnDtaPos,UseSkipCountToken,SkipCountPageSize,UriParts,SqlTableName,GraphMetaDataEntityName,SqlDefinition,EntityColDef,CurTblRecordCount,IsBatchData,TruncateSqlTable,SqlLogByTblIdObj,SqlLogByTblId -ErrorAction SilentlyContinue

    } # End foreach DrillDown (aka DrillDownData in UriPart) in DrillDownInfo
    
    # Cleanup:
    Remove-Variable -Name DrillDown -ErrorAction SilentlyContinue

    Write-Host ""
} # End Processing DrillDownInfo
#>

## Log to SQL Table - completion
  ## FIX FOR ConnectionStrings and such...
if ($SqlLogTblId) { # Currently this isn't logging any errors but the framework is there...just need to update things above if necessary...
    $SqlLogTblObj = Update-SqlLogging -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -LogTableName $SqlLoggingTableName -PK_ID $SqlLogTblId
    if ($SqlLogTblObj.Value -eq 0) {
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Successfully logged the completion of the refresh to SQL table; Log ID = $SqlLogTblId" -ForegroundColor Cyan
    }
    else {
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There was an error trying to log the completion to the SQL table! ; Log ID = $SqlLogTblId" -ForegroundColor Yellow
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Returned from the Call:" -ForegroundColor Yellow
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($SqlLogTblObj.ErrorCaptured)" -ForegroundColor Yellow
    }
}

## Final Cleanup:
Remove-Variable -Name OwnDatabase,BaseURL,EncryptedSQLConnString,SqlServerName,SqlDatabaseName,SqlSchemaName,SyncMgdDvcCertStatesData,SyncDeviceStatusesData,SynchardwareInformation,WriteBatchSize,DrillDownWriteBatchSize,VerboseRecordCount,SqlConnTimeout,SqlTimeout,ApplicationId,User,CredentialsFile,DataStoreURL,RedirectUri,SqlLoggingTableName,UseTestTables,TablesToSync,SecureSQLConnString,GetAuthStringCmd,Enums,Entities,Table,DrillDownInfo,SqlLogTblObj,SqlLogTblId -ErrorAction SilentlyContinue
Remove-Variable -Scope Global -Name MetaData,NamespaceMgr -ErrorAction SilentlyContinue
Clear-Authentication

Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Script Finished" -ForegroundColor Cyan
