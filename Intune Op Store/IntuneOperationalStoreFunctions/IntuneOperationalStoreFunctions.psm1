#Requires -Version 5.0
# y
function Get-Authentication { # change name to Initialize-Authentication?
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$false)]$ApplicationId="b78eaaf9-18b8-49c7-93fa-77d96d729253" #"9d6449a6-e0c5-417c-bb43-ce679df0a3f4" #Intune appId? = d1ddf0e4-d672-4dae-b554-9d5bdfd93547
       ,[Parameter(Mandatory=$false)]$User="cmauto@microsoft.com"
       ,[Parameter(Mandatory=$false)]$DataStoreURL="https://graph.microsoft.com/" # is "v1.0/ or beta/" required??
       ,[Parameter(Mandatory=$false)]$CredentialsFile
       ,[Parameter(Mandatory=$false)]$RedirectUri='urn:ietf:wg:oauth:2.0:oob' #'http://localhost:8000' #'urn:ietf:wg:oauth:2.0:oob'
    )
<#
.SYNOPSIS
    This function is used to authenticate with the Azure Active Directory using ADAL
.DESCRIPTION
    The function authenticates with Azure Active Directory with a UserPrincipalName
.EXAMPLE
    Get-Authentication -ApplicationId ee6e1234-5655-4321-83f4-ef4fd36ce1c2 -User user@microsoft.com
    Authenticates you to a specific Application ID within Azure Active Directory with the users UPN
.NOTES
    NAME: Get-Authentication
    HISTORY:
        Date              Author                                       Notes
        12/15/17          Benjamin Reynolds (breynol@microsoft.com)    Adapted from Nick Ciaravella's "Connect-IntuneDataWarehouse"
#>

    $userUpn = New-Object "System.Net.Mail.MailAddress" -ArgumentList $User
    $tenant = $userUpn.Host

    # Finding the AzureAD cmdlets that can be used for authentication.
    $AadModule = Get-Module -Name "AzureAD" -ListAvailable

    if ($AadModule -eq $null) {
        Write-Host "AzureAD PowerShell module not found, looking for AzureADPreview"
        $AadModule = Get-Module -Name "AzureADPreview" -ListAvailable
    }

    if ($AadModule -eq $null) {
        throw "AzureAD Powershell module not installed...Install by running 'Install-Module AzureAD' or 'Install-Module AzureADPreview' from an elevated PowerShell prompt"
    }

    # Getting path to Active Directory Assemblies
    # If the module count is greater than 1 find the latest version
    if ($AadModule.count -gt 1) {

        $Latest_Version = ($AadModule | select version | Sort-Object)[-1]
        $aadModule = $AadModule | ? { $_.version -eq $Latest_Version.version }

        # Checking if there are multiple versions of the same module found
        if ($AadModule.count -gt 1) {
            $aadModule = $AadModule | select -Unique
        }

        $adal = Join-Path $AadModule.ModuleBase "Microsoft.IdentityModel.Clients.ActiveDirectory.dll"
        $adalforms = Join-Path $AadModule.ModuleBase "Microsoft.IdentityModel.Clients.ActiveDirectory.Platform.dll"
    }
    else {
        $adal = Join-Path $AadModule.ModuleBase "Microsoft.IdentityModel.Clients.ActiveDirectory.dll"
        $adalforms = Join-Path $AadModule.ModuleBase "Microsoft.IdentityModel.Clients.ActiveDirectory.Platform.dll"
    }

    [System.Reflection.Assembly]::LoadFrom($adal) | Out-Null
    [System.Reflection.Assembly]::LoadFrom($adalforms) | Out-Null

    $resourceAppIdURI = "https://graph.microsoft.com/"
    $authority = "https://login.windows.net/$Tenant"

    try {
        $authContext = New-Object "Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext" -ArgumentList $authority

        # Change the prompt behaviour to force credentials each time: Auto, Always, Never, RefreshSession
        $platformParameters = New-Object "Microsoft.IdentityModel.Clients.ActiveDirectory.PlatformParameters" -ArgumentList "Auto"
        $userId = New-Object "Microsoft.IdentityModel.Clients.ActiveDirectory.UserIdentifier" -ArgumentList ($User, "OptionalDisplayableId")

        if ($CredentialsFile -eq $null){
            $authResult = $authContext.AcquireTokenAsync($resourceAppIdURI,$ApplicationId,$RedirectUri,$platformParameters,$userId).Result
        }
        else {
            if (Test-Path "$CredentialsFile") {
                $UserPassword = Get-Content "$CredentialsFile" | ConvertTo-SecureString
                $userCredentials = New-Object Microsoft.IdentityModel.Clients.ActiveDirectory.UserPasswordCredential -ArgumentList $userUPN,$UserPassword
                $authResult = [Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContextIntegratedAuthExtensions]::AcquireTokenAsync($authContext, $resourceAppIdURI, $ApplicationId, $userCredentials).Result;
            }
            else {
                throw "Path to Password file $Password doesn't exist, please specify a valid path..."
            }
        }#>

        if ($authResult.AccessToken) {
            $global:ADAuthResult = $authResult;
            $global:ADAuthUser = $User;
            $global:OpStoreURL = $DataStoreURL;
        }
        else {
            throw "Authorization Access Token is null, please re-run authentication..."
        }
    }
    catch {
        write-host $_.Exception.Message -f Red
        write-host $_.Exception.ItemName -f Red
        write-host
        throw
    }
} #End: Get-Authentication
# y
function Clear-Authentication { # was "Dispose-Authentication"; should be Clear or Revoke??
<#
.SYNOPSIS
    This function is used to 
.DESCRIPTION
    The function 
.EXAMPLE
    Clear-Authentication .....
.NOTES
    NAME: Clear-Authentication
    HISTORY:
        Date              Author
        03/14/18          Benjamin Reynolds (breynol@microsoft.com)
#>

Remove-Variable -Scope Global -Name ADAuthResult,ADAuthUser,OpStoreURL

} #End: Clear-Authentication
# y
function Get-IntuneOpStoreData {
    [cmdletbinding(PositionalBinding=$false)]
    param
    (
        [Parameter(Mandatory=$true)][String]$OdataUrl
       ,[Parameter(Mandatory=$false,HelpMessage="This should be the command used to create the auth token - it needs to start with Get-Authentication")]$GetAuthStringCmd
       ,[Parameter(Mandatory=$false)][int]$WriteBatchSize=50000
       ,[Parameter(Mandatory=$false)][int64]$CurNumRecords=0
       ,[Parameter(Mandatory=$false)][bool]$VerboseInfo=$false
       ,[Parameter(Mandatory=$false)][int]$VerboseRecordCount=0
    )
<#
.SYNOPSIS
    This function is used to get a collection of data from the Intune Data Warehouse
.DESCRIPTION
    The function connects to the Data Warehouse URL and returns all data in a collection of data from a given starting point/URL
.PARAMETER OdataUrl
    Required.
    This is the "starting point" for the collection - all data should be collected from this point on.
.PARAMETER GetAuthStringCmd
    Not Required.
    This is the command used to create the authentication token - it needs to start with "Get-Authentication".
    This is used to re-authenticate to the service in the event the access token has expired.
.PARAMETER WriteBatchSize
    Not Required. Default = 50,000
    This is the point in which the function will stop collecting data and send the data back to the caller for processing.
    The data is sent along with the "next URL" in order to be handled by the caller and if desired, the rest of the data can be obtained by calling this function again with the link previously provided in the output object
.PARAMETER VerboseInfo
    Not Required. Default = False
    This is just to return some verbose information without using the regular "-Verbose" command (so that extra data isn't returned from Invoke-WebRequest).
.EXAMPLE
    Get-IntuneOpStoreData -OdataUrl "" -GetAuthStringCmd "Get-Authentication -User user@example.com -ApplicationId 4184c61a-e324-4f51-83d7-022b6a82b991 -CredentialsFile 'c:\path to encrypted password file.txt'"
    Returns all devices from the Operational Store in a batch of 50,000 records (or all records if less than this amount)
.EXAMPLE
    Get-IntuneOpStoreData -OdataUrl "" -GetAuthStringCmd "Get-Authentication -User user@example.com -ApplicationId 4184c61a-e324-4f51-83d7-022b6a82b991 -CredentialsFile 'c:\path to encrypted password file.txt'" -WriteBatchSize 99999999 -VerboseInfo $true
    Returns all devices from the Operational Store in a batch of 99,999,999 records (or all records if less than this amount) and writes to the host the time of the call and how many records received
.OUTPUTS
    A PSObject containing the data and the "Next URL" if one exists (or is required to get the rest of the data from the collection).
    The data is contained in the "DataObject" object and the next url in "URL" (a string).
.NOTES
    NAME: Get-IntuneOpStoreData
    HISTORY:
        Date          Author                                       Notes
        12/04/2017    Benjamin Reynolds (breynol@microsoft.com)    Adapted from Nick Ciaravella's "Get-IntuneDataWarehouseCollection"
        04/02/2018    Benjamin Reynolds (breynol@microsoft.com)    Changed to Invoke-RestMethod instead of Invoke-WebRequest (for automation - avoids some issues)
        04/05/2018    Benjamin Reynolds (breynol@microsoft.com)    Updated WriteBatchSize logic to account for multi-url calls to the function

ISSUES: 
The 'WriteBatchSize' doesn't work when the records returned are not exactly the count - i.e., if 50,000 is the WriteBatchSize/VerboseRecordCount and 50,050 records are gotten then we don't hit the logic to write the info out...

#>

    if (!$global:ADAuthResult) {
        if ($GetAuthStringCmd) {
            Invoke-Expression $GetAuthStringCmd
        }
        else {
            try {
                Get-Authentication -User "$env:USERNAME@microsoft.com"
            }
            catch {
                throw "No authentication context. Authenticate first by running 'Get-Authentication'"
            }
        }
    }
    
    $URL = $OdataUrl
    
    # Variables to handle retries:
    [int]$ReconnRetry = 0
    [int]$GatewayTimeoutRetry = 0
    
    while ($URL) {
        $clientRequestId = [Guid]::NewGuid()
        $headers = @{
                    'Content-Type'='application/json'
                    'Authorization'="Bearer " + $global:ADAuthResult.AccessToken
                    'ExpiresOn'= $global:ADAuthResult.ExpiresOn
                    'client-request-id'=$clientRequestId
                    }
        try {
    
            if (($VerboseInfo) -and (($VerboseRecordCount -eq 0) -or ($(if ($VerboseRecordCount -gt 0) {($CurNumRecords % $VerboseRecordCount -eq 0) -or ($CurNumRecords % $WriteBatchSize -eq 0)} else {$CurNumRecords -eq $VerboseRecordCount})))) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Getting Records Greater Than: $CurNumRecords" -ForegroundColor Cyan
                #Write-Host "Calling service with URL: $URL" -ForegroundColor Yellow
            }
            
               #$Response = Invoke-WebRequest -Uri $URL -Method Get -Headers $headers
            $Response = Invoke-RestMethod -Uri $URL -Method Get -Headers $headers
            
            [int]$CurRecordsReceived = $Response.value.Count
            [int]$TotalRecordsReceived += $CurRecordsReceived
            $CurNumRecords += $CurRecordsReceived
            
            if (($VerboseInfo) -and (($VerboseRecordCount -eq 0) -or ($(if ($VerboseRecordCount -gt 0) {$CurNumRecords % $VerboseRecordCount -eq 0} else {$CurNumRecords -eq $VerboseRecordCount})))) {
                if ($VerboseRecordCount -eq 0) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Records Received = $CurRecordsReceived" -ForegroundColor Cyan
                }
                else {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Records Received = $TotalRecordsReceived" -ForegroundColor Cyan
                }
            }
            
               #$JsonResponse += $($Response.Content | ConvertFrom-Json).value
            $JsonResponse += $Response.value
    
               #$URL = $($Response.Content | ConvertFrom-Json).'@odata.nextLink'
            $URL = $Response.'@odata.nextLink'
    
            # if we successfully got here then we can safely reset the gateway timeout retry count...
            $GatewayTimeoutRetry = 0
            
            ## Check to see if we've hit the batch size:
            # the gt 0 records is in the event the URL returned 0 records; If so we don't want to hit this
            if ($CurNumRecords % $WriteBatchSize -eq 0 -and $CurNumRecords -gt 0 -and $CurRecordsReceived -gt 0) {
                if ($VerboseInfo) {Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : We've hit the Write BatchSize so sending data back for processing..." -ForegroundColor Cyan}
                break # stop the while loop and return the object to the caller for processing
            }
            
            ### Old logic for WriteBatchSize:
            ## the gt 0 records is in the event the URL returned 0 records; If so we don't want to hit this
            #if ($TotalRecordsReceived % $WriteBatchSize -eq 0 -and $CurNumRecords -gt 0) { ## ($TotalRecordsReceived % $WriteBatchSize -eq 0 -and $CurNumRecords -gt 0) -or ($CurNumRecords % $WriteBatchSize -eq 0) # should use $CurRecordsReceived instead of $CurNumRecords???
            #    if ($VerboseInfo) {Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : We've hit the Write BatchSize so sending data back for processing..." -ForegroundColor Cyan}
            #    break # stop the while loop and return the object to the caller for processing
            #}

        }
        catch [System.Net.WebException] {
            # Check for authentication expiry issues:
            if ((($_.ErrorDetails -like "*Access token has expired*") -eq $true) -or (($_.ErrorDetails -like "*(401) Unauthorized*") -eq $true)) {
                # this reconnection retry stuff works because the URL is the same at this point and is retried when the loop continues on...
                $ReconnRetry += 1
                if ($VerboseInfo) {Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : We Need to Handle the timeout of the Access Token; Going to try to re-authenticate after 5 seconds..."}
                Start-Sleep -Seconds 5
                # Re-connect to get a new access token
                if ($GetAuthStringCmd) {
                    Invoke-Expression $GetAuthStringCmd
                }
                else {
                    Get-Authentication -User "$env:USERNAME@microsoft.com"
                }
                # Check to see if the reconnect worked:
                if (($global:ADAuthResult.ExpiresOn.datetime - $((Get-Date).ToUniversalTime())).Minutes -ge 10) {
                    $ReconnRetry = 0
                    continue # this continues the while loop
                }
                else {
                    if ($ReconnRetry -le 3) {
                        Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Unable to get a new AccessToken will try again. Retry $ReconnRetry of 3."
                        # don't do anything so it tries again...no explicit retry of the connection but the URL is the same and will try again in the loop..
                    }
                    else {
                        $CatchEndLoop = $true
                        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Unable to get a new AccessToken for the last number of retries; returning to caller." -ForegroundColor Red
                        Write-Host $_.Exception -ForegroundColor Red # Is this enough or do we want more data?
                        break # this breaks out of the while loop
                    }
                }
            } #End AccessToken Expiration if block
            # If not expiry, check for known errors and handle appropriately:
            elseif (($_.Exception -like "*(504) Gateway Timeout*") -eq $true) {
                # this retry works because the URL is the same at this point and is retried when the loop continues on...
                $GatewayTimeoutRetry += 1
                
                if ($GatewayTimeoutRetry -le 5) {
                    Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Gateway Timed out!; will try again..."
                    continue
                }
                else {
                    $CatchEndLoop = $true
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Gateway Timed out for the last number of retries; returning to caller" -ForegroundColor Red
                    break
                }
            }
            elseif (($_.Exception -like "*(400) Bad Request*") -eq $true) {
                $CatchEndLoop = $true
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Bad Request Error Caught; will return to caller." -ForegroundColor Red
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Bad Request URL: $URL" -ForegroundColor Red
                break # this breaks out of the while loop
            }
            elseif (($_.Exception -like "*(403) Forbidden*") -eq $true) {
                $CatchEndLoop = $true
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Forbidden Error Caught; will return to caller." -ForegroundColor Red
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Forbidden URL (need to get access to this resource): $URL" -ForegroundColor Red
                break # this breaks out of the while loop
            }
            else {
                $CatchEndLoop = $true
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Unhandled Error Encountered. Original Error is:" -ForegroundColor Red
                Write-Host $_.Exception -ForegroundColor Red
                break # this breaks out of the while loop
            }
        } # End Catch block
    } # End While Loop
    
    if (!$TotalRecordsReceived) {$TotalRecordsReceived = 0}
    # if URL doesn't exist, we've hit the WriteBatchSize, or we've used a "break" in the catch block we'll get to this section...
    if ($CatchEndLoop) { # We broke the loop due to exceptions
        #if ($CurNumRecords -ne $TotalRecordsReceived) {Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There is a mismatch in record counts: $(if ($TotalRecordsReceived) {$TotalRecordsReceived} else {"null"}) (TotalRecordsReceived) vs $CurNumRecords (CurNumRecords)" -ForegroundColor Yellow}
        if ($VerboseInfo) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Records Received (before error encountered): Total Count = $CurNumRecords ; Batch Count = $TotalRecordsReceived" -ForegroundColor Cyan
        }
        if ($JsonResponse.Count -gt 0) {
            $ReturnObj = New-Object -TypeName PSObject -Property @{"DataObject"=$JsonResponse;"URL"=$URL;"RecordCount"=$CurNumRecords;"BatchRecordCount"=$TotalRecordsReceived;"ErrorCaught"="true"}
        }
        else {
            $ReturnObj = New-Object -TypeName PSObject -Property @{"URL"=$URL;"RecordCount"=$CurNumRecords;"BatchRecordCount"=$TotalRecordsReceived;"ErrorCaught"="true"}
        }
    }
    else { # No break used in the catch block: We got all data or hit the WriteBatchSize
        #if ($CurNumRecords -ne $TotalRecordsReceived) {Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There is a mismatch in record counts: $(if ($TotalRecordsReceived) {$TotalRecordsReceived} else {"null"}) (TotalRecordsReceived) vs $CurNumRecords (CurNumRecords)" -ForegroundColor Yellow}
        if ($VerboseInfo) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Records Received: Total Count = $CurNumRecords ; Batch Count = $TotalRecordsReceived" -ForegroundColor Cyan
        }
        $ReturnObj = New-Object -TypeName PSObject -Property @{"DataObject"=$JsonResponse;"URL"=$URL;"RecordCount"=$CurNumRecords;"BatchRecordCount"=$TotalRecordsReceived;"ErrorCaught"="false"}
    }

    return $ReturnObj
} #End: Get-IntuneOpStoreData
# y
function ConvertTo-DataTable {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true)]$InputObject
       ,[Parameter(Mandatory=$true)]$ColumnDef
    )
<#
.SYNOPSIS
    This function is used to convert an object to a data table object
.DESCRIPTION
    The function creates a DataTable object with the column definition provided and then fills it with the information from the provided input object
.PARAMETER InputObject
    Required.
    This is an object containing the data that will be converted to the data table - insert these "records"
.PARAMETER ColumnDef
    Required.
    This is the table definition which is to be used to create the data table.
    The definition must contain the properties: Name, Type, Nullable; with DataName, ColRemoved optional. (Any other properties are currently ignored)
.EXAMPLE
    ConvertTo-DataTable -InputObject $ObjectWithData -ColumnDef $ObjectContainingColumnDefinitions
    Converts the data in the input object to a data table using the columns and data types from the column definition input object
.INPUTS
    An object with property names (that match the ColumnName's in the ColumnDef parameter) and associated data and an object containing the column definitions
.OUTPUTS
    A "System.Data.DataTable" object containing all the data from the input object for the "columns" provided in the ColumnDef parameter
.NOTES
    NAME: ConvertTo-DataTable
    HISTORY:
        Date          Author                                       Notes
        12/04/2017    Benjamin Reynolds (breynol@microsoft.com)    
        03/28/2018    Benjamin Reynolds (breynol@microsoft.com)    Accounting for a data name and a column name;
                                                                   Added missing property handling (for not null properties)
        04/02/2018    Benjamin Reynolds (breynol@microsoft.com)    Added handling of 'collection' columns - JSON
#>
    # Create the DataTable (with column definitions):
    $DtaTbl = New-Object System.Data.DataTable
    foreach ($tblcol in $ColumnDef) {
        if ($tblcol.Nullable -eq "true") {$CurNul = $true} else {$CurNul = $false}
        #####  this is for backwards compatibility...consider removing at a later time...
        # This handles Odata where a column has a name with an asterisk in it, i.e., "@odata.type" --> "odatatype"
        if ($tblcol.Name -like "@*") {$ColName = $tblcol.Name.Replace('@','').Replace('.','')} else {$ColName = $tblcol.Name}
        #####
        $CurCol = New-Object System.Data.DataColumn
        $CurCol.ColumnName = $ColName
        $CurCol.DataType = $tblcol.Type
        $CurCol.AllowDBNull = $CurNul
        $DtaTbl.Columns.Add($CurCol)
        Write-Verbose "Column $ColName Added to Data Table Definition"
        Remove-Variable -Name ColName,CurNul,CurCol -ErrorAction SilentlyContinue
    } # end creating DataTable
    
    # Fill the DataTable with the data from the InputObject
    foreach ($Rec in $InputObject) {
        $CurRow = $DtaTbl.NewRow()
        foreach ($col in $ColumnDef) {
            # This works if not converting the Odata column names: # The original working code
            <#if ($(($Rec).($col.Name)) -or $(($Rec).($col.Name)) -eq $false) {
                $CurRow["$($col.Name)"] = ($Rec).($col.Name)
            }#>
            
            if ($col.DataName) {
                $DataName = $col.DataName
                $ColName = $col.Name
            }
            else {
                $DataName = $col.Name
                #### This is for backwards compatibility...
                # This handles the Odata column names:
                if ($col.Name -like "@*") {
                    $ColName = $col.Name.Replace('@','').Replace('.','') # may consider replacing all special chars?
                }
                else {
                    $ColName = $col.Name
                }
            }
            
            # Check that the property has a value to set, and if so add the info to the record/row:
            if ($(($Rec).($DataName)) -or $(($Rec).($DataName)) -eq $false) {
                if ($col.IsCollection -eq "true") {
                    #Convert the column data to JSON string and Add the property to the current record/row:
                    $CurRow["$ColName"] = $(($Rec).($DataName) | ConvertTo-Json)
                }
                else {
                    # Add the property to the current record/row:
                    $CurRow["$ColName"] = ($Rec).($DataName)
                }
            }
            elseif ($col.Nullable -ne "true") {
                # We're going to handle properties that don't exist when the property is NOT Nullable:
                $Val =
                Switch ($col.Type) {
                        "String" {"";break}
                        "DateTime" {"1900-01-01 00:00:00.000";break}
                        "Int64" {-1;break}
                        "Int32" {-1;break}
                        "Int16" {0;break}
                        "Byte" {0;break}
                        "Boolean" {0;break}
                        "Guid" {"00000000-0000-0000-0000-000000000000";break}
                        default {"0";break}
                }
                $CurRow["$ColName"] = $Val
                Remove-Variable -Name Val -ErrorAction SilentlyContinue
            }
        } # end foreach record

        # Now that all properties/columns are added to the record/row, add the row to the table:
        $DtaTbl.Rows.Add($CurRow)
    }
    Write-Output @(,($DtaTbl))
} # End: ConvertTo-DataTable

function Get-ColumnDefinitionFromObjects {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true)]$MetaData
       ,[Parameter(Mandatory=$true)]$Enums
       ,[Parameter(Mandatory=$true)][Alias("DefFromObj")]$DefinitionFromDataObject
    )
<#
.SYNOPSIS
    This function is used to 
.DESCRIPTION
    The function 
.EXAMPLE
    Get-ColumnDefinitionFromObjects .....
.NOTES
    NAME: Get-ColumnDefinitionFromObjects
    HISTORY:
        Date              Author
        01/30/18          Benjamin Reynolds (breynol@microsoft.com)
#>

    # Create the Working Objects:
    $ColDefMetaData = New-Object System.Collections.ArrayList
    $ColumnDefinition = New-Object System.Collections.ArrayList

    # Determine which properties are 'selectable' - regular/known data types and those that point to Enums (filtering out the collections/complex types):
    $SelectCols = $MetaData.Property | Where-Object {$_.Type -like 'Edm.*' -or $($_.Type.Replace('Collection','').Replace('microsoft.graph.','').Replace('(','').Replace(')','')) -in $($Enums.Name)} | Select Name

    # Create a column definition object based on the metadata:
    foreach ($col in ($MetaData.Property)) {
        # If the column in the metadata is a known data type or an enum type then we'll add it to the ColumnDef object:
        if ($col.Name -in $SelectCols.Name) {
            $CurType = $col.Type.Replace('Collection','').Replace('microsoft.graph.','').Replace('(','').Replace(')','')
            # If the datatype is not a known entity, those starting with "Edm.", then just make it a string:
            if ($CurType -notlike "Edm*") {
                $CurType = "String"
            }
            # ...otherwise, remove the "Edm." and return the known type (replace some datatypes for consistency):
            else {
                $CurType = $CurType.Replace('Edm.','').Replace("DateTimeOffset","DateTime").Replace("TimeOfDay","DateTime").Replace("Binary","String").Replace("bool","Boolean").Replace("int","Int32")
            }
              # This handles Odata where a column has a name with an asterisk in it, i.e., "@odata.type" --> "odatatype"
              #if ($col.Name -like "@*") {$ColName = $col.Name.Replace('@','').Replace('.','')} else {$ColName = $col.Name}
              #$CurColObj = New-Object -TypeName PSObject -Property @{"Name" = $ColName;"Type" = $CurType;"Nullable" = $(if (!$col.Nullable) {"true"} else {$col.Nullable})}
            $CurColObj = New-Object -TypeName PSObject -Property @{"Name" = $($col.Name);"Type" = $CurType;"Nullable" = $(if (!$col.Nullable) {"true"} else {$col.Nullable})}
            [void]$ColDefMetaData.Add($CurColObj)

            Remove-Variable -Name CurType <#,ColName#> -ErrorAction SilentlyContinue
        }
    }

    # Create a column definition object using the data actually received from the service,
    # and use the data types and Nullable definition from the metadata if it exists:
    foreach ($coldef in $DefinitionFromDataObject) {
        if ($ColDefMetaData.Count) {
        $defpos = [array]::IndexOf($ColDefMetaData.Name,$coldef.Name)
        }
        else {
            $defpos = -1
        }
        
        # If the column shows up in the metadata get the data type and Nullable flag from there:
        if ($defpos -ge 0) {
            $AltType = $ColDefMetaData[$defpos].Type
            $NullableVal = $ColDefMetaData[$defpos].Nullable
        }
        
        # Stupid Hardcoding:
         # Make the column NOT NULL if it meets some known entity.properties we know should not be nullable:
        if (($MetaData.Name -eq "managedDevice") -and ($coldef.Name -eq "id")) {$NullableVal = "false"}
        if (($MetaData.Name -eq "mobileApps") -and ($coldef.Name -eq "id")) {$NullableVal = "false"}

        # Determine whether the column is one that will be collected. If yes, it is 'not an object'; otherwise it 'is an object'
          # If the data type exists from the metadata then we want to collect the data.
          # If the column isn't in the metadata and the data type does not contain the word "object" then we'll collect the data for the property/column:
           # This could be used to change the column name for the odata names (i.e., "@odata.type" --> "odatatype") but the ConvertTo-DataTable would have to be updated as well:
           #  $(if ($coldef.Name -like "@*") {$coldef.Name.Replace('@','').Replace('.','')} else {$coldef.Name})
        if (($AltType) -or !("object" -like "*$($coldef.Definition.Substring(0,$coldef.Definition.IndexOf(' ')).Replace('[]','').Replace('System.Management.Automation.PSCustom',''))*")) {
            # Odata name handling here?????
            $TmpColObj = New-Object -TypeName PSObject -Property @{"Name" = $($coldef.Name);"Type" = $(if ($AltType) {$AltType} else {$coldef.Definition.Substring(0,$coldef.Definition.IndexOf(' ')).Replace("DateTimeOffset","DateTime").Replace("TimeOfDay","DateTime").Replace("Binary","String").Replace("bool","Boolean").Replace("int","Int32")});"IsObject" = $false;"Nullable" = $(if ($NullableVal) {$NullableVal} else {"true"})}
            [void]$ColumnDefinition.Add($TmpColObj)
            Remove-Variable -Name TmpColObj -ErrorAction SilentlyContinue
        }
          # If it's an object type then it will come here and be marked as an object (so that we ignore the property when creating the data table):
        else {
            $TmpColObj = New-Object -TypeName PSObject -Property @{"Name" = $($coldef.Name);"Type" = $($coldef.Definition.Substring(0,$coldef.Definition.IndexOf(' ')));"IsObject" = $true;"Nullable" = "true"}
            [void]$ColumnDefinition.Add($TmpColObj)
            Remove-Variable -Name TmpColObj -ErrorAction SilentlyContinue
        }
        Remove-Variable -Name AltType,NullableVal -ErrorAction SilentlyContinue
    }

    # Return the final definition:
    return $ColumnDefinition
}# End: Get-ColumnDefinitionFromObjects
# y needs: restmethod change
function Get-OperationalStoreMetaData {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$false)]$MetaDataUri='https://graph.microsoft.com/v1.0/$metadata'
    )
<#
.SYNOPSIS
    This function is used to 
.DESCRIPTION
    The function 
.EXAMPLE
    Get-OperationalStoreMetaData .....
.NOTES
    NAME: Get-OperationalStoreMetaData
    HISTORY:
        Date              Author
        01/30/18          Benjamin Reynolds (breynol@microsoft.com)
#>

    # Initialize the MetaData global variable
    [xml]$global:MetaData = (Invoke-WebRequest -Uri $MetaDataUri -Method Get).Content
    #$global:MetaData = (Invoke-RestMethod -Uri $MetaDataUri -Method Get).Edmx.DataServices.Schema

    # Initialize the global namespace manager variable (for shredding the xml):
    [System.Xml.XmlNamespaceManager]$global:NamespaceMgr = New-Object System.Xml.XmlNamespaceManager $global:MetaData.NameTable
    $global:NamespaceMgr.AddNamespace("edm", "http://docs.oasis-open.org/odata/ns/edmx")
    $global:NamespaceMgr.AddNamespace("sch", "http://docs.oasis-open.org/odata/ns/edm")

    <#####################################################################
    # Should I do this?
    # Create objects for the different types that will be used:
    $global:Enums = Get-EntityTypeMetaData -EntityName "Enums"
    $global:Entities = Get-EntityTypeMetaData -EntityName "EntityTypes"
    # ComplexTypes, Singletons, etc???
    ######################################################################>

}# End: Get-OperationalStoreMetaData

# y needs: restmethod change
function Get-EntityTypeMetaData {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true)][Alias("Entity")]$EntityName
    )
<#
.SYNOPSIS
    This function is used to 
.DESCRIPTION
    The function 
.EXAMPLE
    Get-EntityTypeMetaData .....
.NOTES
    NAME: Get-EntityTypeMetaData
    HISTORY:
        Date              Author
        01/31/18          Benjamin Reynolds (breynol@microsoft.com)
#>

    if ((!$global:MetaData) -or (!$global:NamespaceMgr)) { # When flipping to restmethod instead of webrequest don't look for the namespacemgr
        Get-OperationalStoreMetaData
    }

    if ($EntityName -in ('Enum','Enums','EnumType','EnumTypes')) {
        $EntityName = "EnumType"
    }
    elseif ($EntityName -in ('ComplexType','ComplexTypes')) {
        $EntityName = "ComplexType"
    }
    elseif ($EntityName -in ('EntityType','EntityTypes')) {
        $EntityName = "EntityType"
    }
    elseif ($EntityName -in ('EntitySet','EntitySets','Sets')) {
        $EntityName = "EntitySet"
    }

    if ($EntityName -in ("ComplexType","EnumType","EntityType")) {
        $EntityMetaData = $global:MetaData.SelectNodes("/edm:Edmx/edm:DataServices/sch:Schema/sch:$EntityName",$global:NamespaceMgr)
        #$EntityMetaData = $global:MetaData.$EntityName
    }
    elseif ($EntityName -in ("EntitySet")) {
        $EntityMetaData = $global:MetaData.SelectNodes("/edm:Edmx/edm:DataServices/sch:Schema/sch:EntityContainer/sch:$EntityName",$global:NamespaceMgr)
        #$EntityMetaData = $global:MetaData.EntityContainer.EntitySet
    }
    else {
        $EntityMetaData = $global:MetaData.SelectSingleNode("/edm:Edmx/edm:DataServices/sch:Schema/sch:EntityType[@Name=""$EntityName""]",$global:NamespaceMgr)
        #$EntityMetaData = $global:MetaData.EntityType | ? {$_.Name -eq $EntityName}
        # check to see if that worked, if not let's try the ComplexType instead of EntityType (i.e. hardwareInformation)
        if (!$EntityMetaData) {
            $EntityMetaData = $global:MetaData.SelectSingleNode("/edm:Edmx/edm:DataServices/sch:Schema/sch:ComplexType[@Name=""$EntityName""]",$global:NamespaceMgr)
            #$EntityMetaData = $global:MetaData.ComplexType | ? {$_.Name -eq $EntityName}
        }
    }
    
    return $EntityMetaData
}# End: Get-EntityTypeMetaData
# y
function Get-InheritedProperties {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true)][String]$BaseTypeName
    )
<#
.SYNOPSIS
    This function is used to 
.DESCRIPTION
    The function 
.EXAMPLE
    Get-InheritedProperties .....
.NOTES
    NAME: Get-InheritedProperties
    HISTORY:
        Date              Author
        03/22/18          Benjamin Reynolds (breynol@microsoft.com)
#>
    
    if (!$Entities) {
        $Entities = Get-EntityTypeMetaData -EntityName "EntityTypes"
    }

    $BaseTypeName = Split-Path -Path $BaseTypeName.Replace(".","\") -Leaf
    $InheritedProperties = New-Object System.Collections.ArrayList
    $ParentOrder = 1

    while ($BaseTypeName) {
        $ParentObj = ($Entities | ? {$_.Name -eq $BaseTypeName})
        $PropOrder = 1
        foreach ($Prp in ($ParentObj.Property)) {
            # Assuming that inherited properties won't have collections or enums here...otherwise I should add that in ...
            $CurType = $Prp.Type.Replace('Edm.','').Replace("DateTimeOffset","DateTime").Replace("TimeOfDay","DateTime").Replace("Binary","String").Replace("bool","Boolean").Replace("int","Int32")
            $CurPrp = New-Object -TypeName PSObject -Property @{"DataName" = $($Prp.Name);"Name" = $($Prp.Name);"Type" = $CurType;"Nullable" = $(if (!$Prp.Nullable) {"true"} else {$Prp.Nullable});"ParentOrder" = $ParentOrder;"PropertyOrder" = $PropOrder}
            [void]$InheritedProperties.Add($CurPrp)
            $PropOrder += 1
        }
        if (!$ParentObj.BaseType) {
            Remove-Variable -Name BaseTypeName
        }
        else {
            $BaseTypeName = Split-Path -Path $ParentObj.BaseType.Replace(".","\") -Leaf
            $ParentOrder += 1
        }
        Remove-Variable -Name ParentObj
    }
    return $InheritedProperties
}# End: Get-InheritedProperties
# y
function Get-SqlTableColumnDefinition {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlServerName
       ,[Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlDatabaseName
       ,[Parameter(Mandatory=$true)]$SqlSchemaName
       ,[Parameter(Mandatory=$true)]$SqlTableName
       #,[Parameter(Mandatory=$false,ParameterSetName='NoConnString')]$SqlUserName
       #,[Parameter(Mandatory=$false,ParameterSetName='NoConnString')]$SqlUserPassword
       ,[Parameter(Mandatory=$true,ParameterSetName='ConnString')][Security.SecureString]$SqlConnString_Secure
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Get-SqlTableColumnDefinition .....
.NOTES
    NAME: Get-SqlTableColumnDefinition
    HISTORY:
        Date              Author                                         Changes
        02/07/18          Benjamin Reynolds (breynol@microsoft.com)      Initial Creation
        03/26/18          Benjamin Reynolds (breynol@microsoft.com)      Added MaxLength
#>

    # Create the query to run:
    $SqlTblDefQry = "SELECT  col.name AS [Name]
       ,tip.NewType AS [Type]
       ,CASE col.is_nullable WHEN 1 THEN 'true' ELSE 'false' END AS [Nullable]
       ,col.column_id AS [ColumnOrder]
       ,CASE WHEN tip.NewType = 'String' AND col.max_length > 0 THEN col.max_length/2 END AS [MaxLength]
  FROM sys.objects obj
       INNER JOIN sys.columns col
          ON obj.object_id = col.object_id
       INNER JOIN sys.types typ
          ON col.user_type_id = typ.user_type_id
       INNER JOIN sys.schemas scm
          ON obj.schema_id = scm.schema_id
       CROSS APPLY (
                    SELECT CASE WHEN typ.name = N'bit' THEN 'Boolean'
                                WHEN typ.name = N'uniqueidentifier' THEN 'Guid'
                                WHEN typ.name = N'smallint' THEN 'Int16'
                                WHEN typ.name = N'int' THEN 'Int32'
                                WHEN typ.name = N'bigint' THEN 'Int64'
                                WHEN typ.name = N'tinyint' THEN 'UInt16'
                                WHEN typ.name IN (N'date',N'time',N'datetime2',N'datetimeoffset',N'smalldatetime',N'datetime',N'timestamp') THEN 'DateTime'
                                WHEN typ.name IN (N'real',N'money',N'float',N'decimal',N'numeric',N'smallmoney') THEN 'Decimal'
                                ELSE 'String'
                           END
                    ) tip(NewType)
 WHERE scm.name+N'.'+obj.name = N'$SqlSchemaName.$SqlTableName'
 ORDER BY col.column_id;"

    # Connect to SQL and get the data:
    if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
        $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
        $SqlTableDefinition = Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query $SqlTblDefQry
    }
    else {
        $SqlTableDefinition = Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query $SqlTblDefQry
    }

    # Create the Return Object:
    $SqlTableDefinitionCol = New-Object System.Collections.ArrayList

    foreach ($col in $SqlTableDefinition) {
        if ($col.MaxLength -eq [System.DBNull]::Value) {
            $TmpColObj = New-Object -TypeName PSObject -Property @{"Name" = $($col.Name);"Type" = $($col.Type);"Nullable" = $($col.Nullable);"ColumnOrder" = $($col.ColumnOrder)}
            [void]$SqlTableDefinitionCol.Add($TmpColObj)
            Remove-Variable -Name TmpColObj -ErrorAction SilentlyContinue
        }
        else {        
            $TmpColObj = New-Object -TypeName PSObject -Property @{"Name" = $($col.Name);"Type" = $($col.Type);"Nullable" = $($col.Nullable);"ColumnOrder" = $($col.ColumnOrder);"MaxLength" = $($col.MaxLength)}
            [void]$SqlTableDefinitionCol.Add($TmpColObj)
            Remove-Variable -Name TmpColObj -ErrorAction SilentlyContinue
        }
    }

    # Return the object:
    return $SqlTableDefinitionCol
} # End: Get-SqlTableColumnDefinition

function Get-ColumnDefinitionOrder {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true)]$SqlTableColumns
       ,[Parameter(Mandatory=$true)]$CurColumnDef
    )
<#
.SYNOPSIS
    This function is used to 
.DESCRIPTION
    The function 
.EXAMPLE
    Get-ColumnDefinitionOrder .....
.NOTES
    NAME: Get-ColumnDefinitionOrder
    HISTORY:
        Date              Author
        02/08/18          Benjamin Reynolds (breynol@microsoft.com)
#>

    $NewColumnDef = New-Object System.Collections.ArrayList
    foreach ($DtaCol in ($CurColumnDef | Where-Object {($_.IsObject -eq $false)})) {
        # Check the column to see if it starts with "@" so we handle it correctly:
        if ($DtaCol.Name -like "@*") {
            $CurColName = $DtaCol.Name.Replace('@','').Replace('.','')
        }
        else {
            $CurColName = $DtaCol.Name
        }
        
        # Find the array position of the Column in the SQL Array:
        $SqlPos = [array]::IndexOf($SqlTableColumns.Name,$CurColName)
        # If the column name exists in SQL add the column along with the column position (from SQL) into the return object:
        if ($SqlPos -ne -1) {
            $TmpColObj = New-Object -TypeName PSObject -Property @{"Name" = $($DtaCol.Name);"Type" = $($DtaCol.Type);"Nullable" = $($DtaCol.Nullable);"ColumnOrder" = $($SqlTableColumns[$SqlPos].ColumnOrder)}
            [void]$NewColumnDef.Add($TmpColObj)
            Remove-Variable -Name TmpColObj -ErrorAction SilentlyContinue
        }
        Remove-Variable -Name CurColName -ErrorAction SilentlyContinue
    }
    return $NewColumnDef
} # End: Get-ColumnDefinitionOrder

function Sync-OdataToSql {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlServerName
       ,[Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlDatabaseName
       ,[Parameter(Mandatory=$true,ParameterSetName='ConnString')][Security.SecureString]$SqlConnString_Secure
       ,[Parameter(Mandatory=$true)]$SqlSchemaName
       ,[Parameter(Mandatory=$true)]$SqlTableName
       ,[Parameter(Mandatory=$true)]$GraphMetaDataEntityName
       ,[Parameter(Mandatory=$true)]$Enums
       ,[Parameter(Mandatory=$true)]$DefFromDtaObj
       ,[Parameter(Mandatory=$true)]$DataObject
       ,[Parameter(Mandatory=$false)]$SqlTimeout=28800
       ,[Parameter(Mandatory=$false)]$SqlConnTimeout=240
       ,[Parameter(Mandatory=$false)][switch]$IsBatchData
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Sync-OdataToSql .....
.NOTES
    NAME: Sync-OdataToSql
    HISTORY:
        Date              Author
        02/09/18          Benjamin Reynolds (breynol@microsoft.com)
#>
    
    # Make sure we have the SqlServer module installed
    $SqlSrvrModule = Get-Module -ListAvailable -Name SqlServer | Sort-Object Version -Descending

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
    }#>

    # Let's make sure to import the module too:
    $SqlSrvrModule = Get-Module -Name SqlServer
    if (!$SqlSrvrModule) {
        Import-Module -Name SqlServer
    }

    $TruncateSqlTable = $false

    # Get column definitions from sql, metadata, and the data returned from the service:
    # This could be another function??
    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Getting Column Definitions from SQL, MetaData, and the Data returned..." -ForegroundColor Cyan
    if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
        $ColumnDefSql = Get-SqlTableColumnDefinition -SqlConnString_Secure $SqlConnString_Secure -SqlSchemaName $SqlSchemaName -SqlTableName $SqlTableName
    }
    else {
        $ColumnDefSql = Get-SqlTableColumnDefinition -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SqlSchemaName $SqlSchemaName -SqlTableName $SqlTableName
    }
    $ColumnDefXML = Get-ColumnDefinitionFromObjects -MetaData $(Get-EntityTypeMetaData -EntityName $GraphMetaDataEntityName) -Enums $Enums -DefinitionFromDataObject $DefFromDtaObj
    # If we got information from SQL then we will compare the column definitions:
    if ($ColumnDefSql) {
        # Create a copy of the object for comparison purposes only getting the data we want to use for comparing:
        $ColumnDefXMLForCompare = $ColumnDefXML | Where-Object {$_.IsObject -eq $false} | Select Name,Type,Nullable
        
        # If there is a column from the MetaData/Data returned that starts with an '@' we need to handle that since it is handled when the data table is created:
        # Get all positions where the Name starts with "@":
        $AtPositions = @($ColumnDefXMLForCompare.Name -like "@*" | ForEach-Object {[array]::IndexOf($ColumnDefXMLForCompare.Name,$_)})
        # If there are any columns starting with an "@", update the ColumnName for the Comparison:
        if ($AtPositions.Count) {
            # Update all the column names to remove the "@" and any "."s too (since this is what the data table will do and we want the comparison to be the same):
            $AtPositions | Foreach {$ColumnDefXMLForCompare[$_].Name = $ColumnDefXMLForCompare[$_].Name.Replace('@','').Replace('.','')}
        }
        
        # Compare the definition in SQL to the XML/Data Received to get any column differences:
        $ColsRemoved = $ColumnDefSql | % {if (!($_.Name -in $ColumnDefXMLForCompare.Name)) {$_}}
        $ColsAdded = $ColumnDefXMLForCompare | % {if (!($_.Name -in $ColumnDefSql.Name)) {$_}}

        # If there were any properties removed but SQL has them in the table definition the import will fail so:
        #  we'll alert this, move and rename the table for historical purposes, and, finally, treat this import as a brand new table - get all the data returned
        if ($ColsRemoved) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Column(s) Removed! Will rename and move the old table and create a new one!" -ForegroundColor DarkYellow
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($ColsRemoved.Name.Count) Column(s) Removed. The Removed Column(s) is/are:" -ForegroundColor DarkYellow
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($ColsRemoved.Name -join ",")" -ForegroundColor DarkYellow
            # I'm not currently checking the return codes but could...
            if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
                $MoveTblRtn = Move-SqlTable -SqlConnString_Secure $SqlConnString_Secure -OldSchemaName $SqlSchemaName -NewSchemaName IntuneOpData_OldDefs -TableName $SqlTableName
                $RenameTblRtn = Rename-SqlTable -SqlConnString_Secure $SqlConnString_Secure -SchemaName IntuneOpData_OldDefs -TableName $SqlTableName
            }
            else {
                $MoveTblRtn = Move-SqlTable -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -OldSchemaName $SqlSchemaName -NewSchemaName IntuneOpData_OldDefs -TableName $SqlTableName
                $RenameTblRtn = Rename-SqlTable -SqlServerName $SqlServerName -SqlDatabaseName $SqlDatabaseName -SchemaName IntuneOpData_OldDefs -TableName $SqlTableName
            }
            
            # We don't have a table for this anymore so just use the metadata data (and create a new table):
            $NewColumnDef = $ColumnDefXML | Where-Object {$_.IsObject -eq $false} | Select Name,Type,Nullable
        }
        else {
            if (!$IsBatchData) {$TruncateSqlTable = $true}
            # This accounts for when columns are added - we will just insert the data we can handle so we don't get an error:
            $NewColumnDef = (Get-ColumnDefinitionOrder -SqlTableColumns $ColumnDefSql -CurColumnDef $ColumnDefXML) | Sort ColumnOrder | Select Name,Type,Nullable
            # Now Check if columns were added and alert:
            if ($ColsAdded) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($ColsAdded.Name.Count) Column(s) Added. The Added Column(s) is/are:" -ForegroundColor Yellow
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($ColsAdded.Name -join ",")" -ForegroundColor Yellow
            }
        }
        # Cleanup our Removed/Added objects:
        Remove-Variable -Name ColsRemoved,ColsAdded -ErrorAction SilentlyContinue
    } # End Sql table exists IF
    else {
        # We don't have a table for this yet so just use the metadata data (and create a new table):
        $NewColumnDef = $ColumnDefXML | Where-Object {$_.IsObject -eq $false} | Select Name,Type,Nullable
    }

    # Convert the data we got from the service to a DataTable so that we can import it into SQL:
    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Converting the data to a DataTable for SQL importing..." -ForegroundColor Cyan
    $DtaTbl = ConvertTo-DataTable -InputObject $DataObject -ColumnDef $NewColumnDef
    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : DataTable created: Columns = $($DtaTbl.Columns.Count); Rows = $($DtaTbl.Rows.Count)." -ForegroundColor Cyan

    #Only try to Truncate the table if it exists:
    if ($TruncateSqlTable) {
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Truncating the table '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
        if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
            $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
            Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query "IF OBJECT_ID(N'$SqlSchemaName.$SqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$SqlTableName;"
        }
        else {
            Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query "IF OBJECT_ID(N'$SqlSchemaName.$SqlTableName') IS NOT NULL TRUNCATE TABLE $SqlSchemaName.$SqlTableName;"
        }
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Table Truncated." -ForegroundColor Cyan
    }

    # Write the data to SQL:
    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Starting the import of the DataTable for '$SqlSchemaName.$SqlTableName'..." -ForegroundColor Cyan
    
    # CAN"T USE THIS FOR AZURE DATABASES!!! 
    Write-SqlTableData -ServerInstance $SqlServerName -DatabaseName $SqlDatabaseName -SchemaName $SqlSchemaName -TableName $SqlTableName -InputData $DtaTbl -Timeout $SqlTimeout -ConnectionTimeout $SqlConnTimeout -Force -ErrorAction SilentlyContinue -ErrorVariable WriteSqlTableErrInfo
    
    
    # if we hit a failure (try/catch doesn't catch it) handle it here:
    if ($WriteSqlTableErrInfo) {
        # For some reason Write-Error was giving me strange results and pissed me off so I just went with Write-Host...
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Error Importing the records into SQL. Original Error is:" -ForegroundColor Red
        Write-Host $($WriteSqlTableErrInfo | Out-String) -ForegroundColor Red
    }
    else {
        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Finished importing data for '$SqlSchemaName.$SqlTableName'. Records Imported: $($DtaTbl.Rows.Count)" -ForegroundColor Cyan
    }
} # End: Sync-OdataToSql

function Move-SqlTable {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlServerName
       ,[Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlDatabaseName
       ,[Parameter(Mandatory=$true,ParameterSetName='ConnString')][Security.SecureString]$SqlConnString_Secure
       ,[Parameter(Mandatory=$true)]$OldSchemaName
       ,[Parameter(Mandatory=$true)]$NewSchemaName
       ,[Parameter(Mandatory=$true)]$TableName
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Move-SqlTable .....
.NOTES
    NAME: Move-SqlTable
    HISTORY:
        Date              Author
        02/21/18          Benjamin Reynolds (breynol@microsoft.com)
#>

    # Create the query to run:
    $SqlQry = "ALTER SCHEMA $NewSchemaName TRANSFER $OldSchemaName.$TableName;"

    # Connect to SQL and move the table:
    if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
        $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
        Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query $SqlQry -ErrorVariable SqlErrorCaptured #-ErrorAction SilentlyContinue
    }
    else {
        Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query $SqlQry -ErrorVariable SqlErrorCaptured #-ErrorAction SilentlyContinue
    }
    
    $ReturnObj = New-Object System.Collections.ArrayList

    # Create the return object (include the error if one was caught):
    if ($SqlErrorCaptured) {
        $TmpRtnObj = New-Object -TypeName PSObject -Property @{"ErrorCaptured" = $SqlErrorCaptured;"Value" = -1}
        [void]$ReturnObj.Add($TmpRtnObj)
        return $ReturnObj
    }
    else {
        $TmpRtnObj = New-Object -TypeName PSObject -Property @{"Value" = 0}
        [void]$ReturnObj.Add($TmpRtnObj)
        return $ReturnObj
    }
} # End: Move-SqlTable

function Rename-SqlTable {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlServerName
       ,[Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlDatabaseName
       ,[Parameter(Mandatory=$true,ParameterSetName='ConnString')][Security.SecureString]$SqlConnString_Secure
       ,[Parameter(Mandatory=$true)]$SchemaName
       ,[Parameter(Mandatory=$true)]$TableName
       ,[Parameter(Mandatory=$false)]$NewTableName
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Rename-SqlTable .....
.NOTES
    NAME: Rename-SqlTable
    HISTORY:
        Date              Author
        02/21/18          Benjamin Reynolds (breynol@microsoft.com)
#>

    # Create the query to run:
    if ($NewTableName) {
        $SqlQry = "EXECUTE sp_rename N'$SchemaName.$TableName',N'$NewTableName';"
    }
    else {
        $SqlQry = "EXECUTE sp_rename N'$SchemaName.$TableName',N'$($TableName)_$(Get-Date -Format "yyyyMMdd_HHmmss")';"
    }

    # Connect to SQL and move the table:
    if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
        $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
        Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query $SqlQry -ErrorVariable SqlErrorCaptured #-ErrorAction SilentlyContinue
    }
    else {
        Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query $SqlQry -ErrorVariable SqlErrorCaptured #-ErrorAction SilentlyContinue
    }
    
    $ReturnObj = New-Object System.Collections.ArrayList
    
    # Create the return object (include the error if one was caught):
    if ($SqlErrorCaptured) {
        $TmpRtnObj = New-Object -TypeName PSObject -Property @{"ErrorCaptured" = $SqlErrorCaptured;"Value" = -1}
        [void]$ReturnObj.Add($TmpRtnObj)
        return $ReturnObj
    }
    else {
        $TmpRtnObj = New-Object -TypeName PSObject -Property @{"Value" = 0}
        [void]$ReturnObj.Add($TmpRtnObj)
        return $ReturnObj
    }
 } # End Function: Rename-SqlTable

function Get-SqlTableDefinition {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$false)]$SchemaName='dbo'
       ,[Parameter(Mandatory=$true)]$TableName
       ,[Parameter(Mandatory=$true)]$ColumnDefinition
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Get-SqlTableDefinition .....
.NOTES
    NAME: Get-SqlTableDefinition
    HISTORY:
        Date              Author
        03/26/18          Benjamin Reynolds (breynol@microsoft.com)
#>

    # 
    [String]$TableDefinitionSql = "CREATE TABLE $SchemaName.$TableName ("
    $NumSpaces = $TableDefinitionSql.Length
    $FirstLine = $true

    foreach ($Col in $ColumnDefinition) {
        # If the column has a MaxLength property we'll use that, otherwise just use "max":
        if ($Col.MaxLength) {
            $MaxLength = "$($Col.MaxLength)"
        }
        else {
            $MaxLength = "max"
        }
        
        # Change the Type to something usable in SQL:
        $SqlType = 
        Switch ($Col.Type) {
                "String" {"nvarchar($MaxLength)";break}
                "DateTime" {"datetime2";break}
                "Int64" {"bigint";break}
                "Int32" {"int";break}
                "Int16" {"smallint";break}
                "Byte" {"tinyint";break}
                "Boolean" {"bit";break}
                "Guid" {"uniqueidentifier";break}
                default {$Col.Type;break}
        }

        # Change the Nullable to something usable in SQL:
        $Nullable =
        Switch ($Col.Nullable) {
                "false" {"NOT NULL";break}
                default {"NULL";break}
        }

        # Create each of the column lines:
        if ($FirstLine) {
            $TableDefinitionSql += " $($Col.Name) $SqlType $Nullable`r`n"
            $FirstLine = $false
        }
        else {
            $TableDefinitionSql += "$(" "*$NumSpaces),$($Col.Name) $SqlType $Nullable`r`n"
        }

    }

    $TableDefinitionSql += "$(" "*$NumSpaces));"

    return $TableDefinitionSql

} # End Function: Get-SqlTableDefinition
# y
function Get-CollectionEntity {
    [cmdletbinding(PositionalBinding=$false)]
    param
    (
        [Parameter(Mandatory=$false)]$EndEntity
       ,[Parameter(Mandatory=$true)]$UrlPartsReversed
       ,[Parameter(Mandatory=$false)]$UrlPosition=0
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Get-CollectionEntity .....
.NOTES
    NAME: Get-CollectionEntity
    HISTORY:
        Date              Author
        03/26/18          Benjamin Reynolds (breynol@microsoft.com)
#>

    if (!$Entities) {$Entities = Get-EntityTypeMetaData -EntityName "EntityTypes"}
    
    if (!$EndEntity) {
        $EndEntity = $Entities | ? {$_.NavigationProperty.Name -eq $UrlPartsReversed[$UrlPosition].Replace("microsoft.graph.","")}
    }
    else {
        $Ent = $Entities | ? {$_.NavigationProperty.Name -eq $UrlPartsReversed[$UrlPosition].Replace("microsoft.graph.","")}
        if ($Ent) {
            $EndEntity = $EndEntity | ? {$_.Name -eq (($Ent.NavigationProperty | ? {$_.Name -eq $UrlPartsReversed[$UrlPosition].Replace("microsoft.graph.","")} | Select Type -Unique).Type.Replace("Collection(","").Replace(")","")).Replace("microsoft.graph.","")}
        }
        else {
            $Ent = $Entities | ? {$_.Name -eq $UrlPartsReversed[$UrlPosition].Replace("microsoft.graph.","")}
            if ($Ent) {
                $EndEntity = $EndEntity | ? {$_.Name -eq ($Ent | Select Name -Unique).Name}
            }
        }
    }

    if ($EndEntity.Count -gt 1) {
        $UrlPosition += 1
        if ($UrlPosition -le $UrlPartsReversed.Count) {
            Get-CollectionEntity -EndEntity $EndEntity -UrlPartsReversed $UrlPartsReversed -UrlPosition $UrlPosition
        }
        else {
            throw "No Applicable Object Found..."
        }
    }
    else {
        $EndEntity
    }
}  # End Function: Get-CollectionEntity

# y needs: restmethod change?
function Get-ColumnDefWithInheritedProps {
    [cmdletbinding(PositionalBinding=$false)]
    param
    (
        [Parameter(Mandatory=$true)]$GraphMetaDataEntityName
       ,[Parameter(Mandatory=$false)]$ExpandedColumns
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Get-ColumnDefWithInheritedProps .....
.NOTES
    NAME: Get-ColumnDefWithInheritedProps
    HISTORY:
        Date          Author                                        Notes
        03/27/2018    Benjamin Reynolds (breynol@microsoft.com)     Initial Creation
        04/02/2018    Benjamin Reynolds (breynol@microsoft.com)     Added Expanded Column handling.
#>

    # What scope should these be in??
    if (!$Entities) {$Entities = Get-EntityTypeMetaData -EntityName "EntityTypes"}
    if (!$Enums) {$Enums = Get-EntityTypeMetaData -EntityName "Enums"}
    
    $GraphEntityNameInfo = $Entities | ? {$_.Name -eq $($GraphMetaDataEntityName.Replace("microsoft.graph.",""))}
    $InheritedProps = Get-InheritedProperties -BaseTypeName $GraphEntityNameInfo.BaseType
    $DerivedTypes = $Entities | ? {$_.BaseType -eq $GraphMetaDataEntityName}
    if ($DerivedTypes) {$WillHaveOdataType = $true} else {$WillHaveOdataType = $false}
    
    $ColumnDefinition = New-Object System.Collections.ArrayList

    # if it has an Odata.Type then we want that to be the first column:
    if ($WillHaveOdataType) {
        $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = "@odata.type";"Name" = "odatatype";"Type" = "String";"Nullable" = "false";"IsCollection" = "false"}
        [void]$ColumnDefinition.Add($CurColObj)
        Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
    }
    
    
    # Next add the inherited properties in order of highest parent down in the order they exist in the metadata:
    if ($InheritedProps) {
        # We'll sort the Inherited Properties in the order they should be and add each one to our ArrayList:
        foreach ($Prop in ($InheritedProps | Sort-Object -Property ParentOrder -Descending | Sort-Object -Property PropertyOrder)) {
            $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = $Prop.DataName;"Name" = $Prop.Name;"Type" = $Prop.Type;"Nullable" = $Prop.Nullable;"IsCollection" = "false"} # Assume that inherited props aren't collections or enums...
            [void]$ColumnDefinition.Add($CurColObj)
            Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
        }
        Remove-Variable -Name Prop -ErrorAction SilentlyContinue
    }
    
    
    # Now add all the properties for the 'class' (i.e., not the derived column properties):
    foreach ($Prop in ($GraphEntityNameInfo.Property)) {
        if (($Prop.Type -like 'Edm.*') -or (($Prop.Type.Replace('Collection','').Replace('microsoft.graph.','').Replace('(','').Replace(')','')) -in $($Enums.Name))) {
            $CurType = $Prop.Type.Replace('Collection','').Replace('microsoft.graph.','').Replace('(','').Replace(')','')
            # If the datatype is not a known entity, those starting with "Edm.", then just make it a string:
            if ($CurType -notlike "Edm*") {
                $CurType = "String"
            }
            # ...otherwise, remove the "Edm." and return the known type (replace some datatypes for consistency):
            else {
                $CurType = $CurType.Replace('Edm.','').Replace("DateTimeOffset","DateTime").Replace("TimeOfDay","DateTime").Replace("Binary","String").Replace("bool","Boolean").Replace("int","Int32")
            }
    
            $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = $Prop.Name;"Name" = $Prop.Name;"Type" = $CurType;"Nullable" = $(if (!$Prop.Nullable) {"true"} else {$Prop.Nullable});"IsCollection" = "false"}
            [void]$ColumnDefinition.Add($CurColObj)
            Remove-Variable -Name CurColObj,CurType -ErrorAction SilentlyContinue
        }
        else {
            # This is if we wanted to do something about the collection/complex types...
            #Write-Host "The Property $($Prop.Name) is an object and should be ignored for now..." -ForegroundColor Yellow
            # Or we could do this...
            $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = $Prop.Name;"Name" = "$($Prop.Name)_JSON";"Type" = "String";"Nullable" = "true";"IsCollection" = "true"}
            [void]$ColumnDefinition.Add($CurColObj)
            Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
        }#>
    }

    
    # Add a Column for all derived properties as a JSON column?

    
    # Lastly, add a property for the expanded columns (if there are any):
    $GraphEntityNameInfo.NavigationProperty | ? {$_.Name -in ($ExpandedColumns -split ",")} | % {
        $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = $_.Name;"Name" = "$($_.Name)_JSON";"Type" = "String";"Nullable" = "true";"IsCollection" = "true"}
        [void]$ColumnDefinition.Add($CurColObj)
        Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
        }


    return $ColumnDefinition
} # End Function: Get-ColumnDefWithInheritedProps

# y needs: restmethod change?
function Get-ExpandedColDefWithInheritedProps {
    [cmdletbinding(PositionalBinding=$false)]
    param
    (
        [Parameter(Mandatory=$true)]$GraphMetaDataEntityName
       ,[Parameter(Mandatory=$false)]$ExpandedColumns
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Get-ExpandedColDefWithInheritedProps .....
.NOTES
    NAME: Get-ExpandedColDefWithInheritedProps
    HISTORY:
        Date          Author                                        Notes
        04/02/2018    Benjamin Reynolds (breynol@microsoft.com)     Initial Creation; to handle expanded columns into their own tables.
#>

    # What scope should these be in??
    if (!$Entities) {$Entities = Get-EntityTypeMetaData -EntityName "EntityTypes"}
    if (!$Enums) {$Enums = Get-EntityTypeMetaData -EntityName "Enums"}
    
    $GraphEntityNameInfo = $Entities | ? {$_.Name -eq $($GraphMetaDataEntityName.Replace("microsoft.graph.",""))}

    $ExpColsGraphMetaDataEntityNames = @()
    $GraphEntityNameInfo.NavigationProperty | ? {$_.Name -in ($ExpandedColumns -split ",")} | % {$ExpColsGraphMetaDataEntityNames += @{"Name" = $_.Name;"EntityName" = $_.Type.Replace('Collection(','').Replace(')','')}}

    # Create an object with the data for each expanded column:
    $ExpandColDefinition = New-Object System.Collections.ArrayList

    foreach ($ExpCol in $ExpColsGraphMetaDataEntityNames) {
        # 
        $CurEntInfo = $Entities | ? {$_.Name -eq $($ExpCol.EntityName.Replace("microsoft.graph.",""))}
        $CurInheritedProps = Get-InheritedProperties -BaseTypeName $CurEntInfo.BaseType
        $CurDerivedTypes = $Entities | ? {$_.BaseType -eq $ExpCol.EntityName}
        if ($CurDerivedTypes) {$CurWillHaveOdataType = $true} else {$CurWillHaveOdataType = $false}

        $CurColumnDefinition = New-Object System.Collections.ArrayList

        # Let's hardcode a "ParentOdataType" and a "ParentId" since that wouldn't be included otherwise:
          # Not my favorite way to do things, but...gotta do what we gotta do...
        $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = "ParentOdataType";"Name" = "ParentOdataType";"Type" = "String";"Nullable" = "false";"IsCollection" = "false"}
        [void]$CurColumnDefinition.Add($CurColObj)
        Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
        $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = "ParentId";"Name" = "ParentId";"Type" = "String";"Nullable" = "false";"IsCollection" = "false"}
        [void]$CurColumnDefinition.Add($CurColObj)
        Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
        ## End hardcoding the parent columns
        
        # if it has an Odata.Type then we want that to be the first column (after the parent columns):
         # I don't think this will ever be the case but leaving just in case...not sure how it could work since the DataName would be the same as above!
        if ($CurWillHaveOdataType) {
            $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = "@odata.type";"Name" = "odatatype";"Type" = "String";"Nullable" = "false";"IsCollection" = "false"}
            [void]$CurColumnDefinition.Add($CurColObj)
            Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
        }
        
        
        # Next add the inherited properties in order of highest parent down in the order they exist in the metadata:
        if ($CurInheritedProps) {
            # We'll sort the Inherited Properties in the order they should be and add each one to our ArrayList:
            foreach ($Prop in ($CurInheritedProps | Sort-Object -Property ParentOrder -Descending | Sort-Object -Property PropertyOrder)) {
                $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = $Prop.DataName;"Name" = $Prop.Name;"Type" = $Prop.Type;"Nullable" = $Prop.Nullable;"IsCollection" = "false"} # Assume that inherited props aren't collections or enums...
                [void]$CurColumnDefinition.Add($CurColObj)
                Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
            }
            Remove-Variable -Name Prop -ErrorAction SilentlyContinue
        }
        
        
        # Now add all the properties for the 'class' (i.e., not the derived column properties):
        foreach ($Prop in ($CurEntInfo.Property)) {
            if (($Prop.Type -like 'Edm.*') -or (($Prop.Type.Replace('Collection','').Replace('microsoft.graph.','').Replace('(','').Replace(')','')) -in $($Enums.Name))) {
                $CurType = $Prop.Type.Replace('Collection','').Replace('microsoft.graph.','').Replace('(','').Replace(')','')
                # If the datatype is not a known entity, those starting with "Edm.", then just make it a string:
                if ($CurType -notlike "Edm*") {
                    $CurType = "String"
                }
                # ...otherwise, remove the "Edm." and return the known type (replace some datatypes for consistency):
                else {
                    $CurType = $CurType.Replace('Edm.','').Replace("DateTimeOffset","DateTime").Replace("TimeOfDay","DateTime").Replace("Binary","String").Replace("bool","Boolean").Replace("int","Int32")
                }
        
                $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = $Prop.Name;"Name" = $Prop.Name;"Type" = $CurType;"Nullable" = $(if (!$Prop.Nullable) {"true"} else {$Prop.Nullable});"IsCollection" = "false"}
                [void]$CurColumnDefinition.Add($CurColObj)
                Remove-Variable -Name CurColObj,CurType -ErrorAction SilentlyContinue
            }
            else {
                $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = $Prop.Name;"Name" = "$($Prop.Name)_JSON";"Type" = "String";"Nullable" = "true";"IsCollection" = "true"}
                [void]$CurColumnDefinition.Add($CurColObj)
                Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
            }
        }
        Remove-Variable -Name Prop -ErrorAction SilentlyContinue
        
        # Add a Column for all derived properties as a JSON column?
        
        <# ## This would only be necessary if we wanted to deal with something like '$expand=Something($expand=AnotherLevelofExpansion)'...and would need to be fixed up so it would work:
        # Lastly, add a property for the expanded columns (if there are any):
        $CurEntInfo.NavigationProperty | ? {$_.Name -in ($ExpandedColumns -split ",")} | % {
            $CurColObj = New-Object -TypeName PSObject -Property @{"DataName" = $_.Name;"Name" = "$($_.Name)_JSON";"Type" = "String";"Nullable" = "true";"IsCollection" = "true"}
            [void]$CurColumnDefinition.Add($CurColObj)
            Remove-Variable -Name CurColObj -ErrorAction SilentlyContinue
            }
        #>

        # Add this information to the return object:
        $CurExpObj = New-Object -TypeName PSObject -Property @{"ExpandedColName" = $ExpCol.Name; "ExpandedColEntityName" = $ExpCol.EntityName; "ColumnDefinition" = $CurColumnDefinition}
        [void]$ExpandColDefinition.Add($CurExpObj)

        Remove-Variable -Name CurEntInfo,CurInheritedProps,CurDerivedTypes,CurWillHaveOdataType,CurColumnDefinition,CurExpObj -ErrorAction SilentlyContinue
    }



    return $ExpandColDefinition
} # End Function: Get-ExpandedColDefWithInheritedProps

function Get-ColumnDefinitionsAndCompare {
    [cmdletbinding(PositionalBinding=$false)]
    param
    (
        [Parameter(Mandatory=$true)]$GraphMetaDataColumnDefinition
       ,[Parameter(Mandatory=$true)]$SqlColumnDefinition
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Get-ColumnDefinitionsAndCompare .....
.NOTES
    NAME: Get-ColumnDefinitionsAndCompare
    HISTORY:
        Date          Author                                        Notes
        04/03/2018    Benjamin Reynolds (breynol@microsoft.com)     Initial Creation
#>

    # Compare the definition in SQL to the XML/Data Received to get any column differences:
      # We're only looking at column name here - not comparing Type or Nullable
    $ColsRemoved = $SqlColumnDefinition | % {if (!($_.Name -in $GraphMetaDataColumnDefinition.Name)) {$_}}
    $ColsAdded = $GraphMetaDataColumnDefinition | % {if (!($_.Name -in $SqlColumnDefinition.Name)) {$_}}
    
    # We're going to create a new Column Definition to send to the ConvertTo-DataTable to account for "DataName"s and such:
    # New ColDefinition for DtaTable:
    $SqlColumnDefinition | % {
        $p = [array]::IndexOf($GraphMetaDataColumnDefinition.Name,$_.Name)
    
        if ($p -gt -1) {
            $_ | Add-Member -MemberType NoteProperty -Name "DataName" -Value $GraphMetaDataColumnDefinition[$p].DataName
            $_ | Add-Member -MemberType NoteProperty -Name "IsCollection" -Value $GraphMetaDataColumnDefinition[$p].IsCollection
        }
        else {# The property falls into the "RemovedCols":
            $_ | Add-Member -MemberType NoteProperty -Name "DataName" -Value $_.Name
        }
        Remove-Variable -Name p
    }
    
    # If there were any properties removed but SQL has them in the table definition we need to handle that:
      #  Let's Alert this
    if ($ColsRemoved) {
        Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Column(s) Removed! The removed columns will be ignored for now but this should be taken care of ASAP!"
        Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($ColsRemoved.Name.Count) Column(s) Removed. The Removed Column(s) is/are:"
        Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($ColsRemoved.Name -join ",")"
    }
    if ($ColsAdded) {
            Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($ColsAdded.Name.Count) Column(s) Added. The Added Column(s) is/are:"
            Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : $($ColsAdded.Name -join ",")"
    }
    # Cleanup our Removed/Added objects:
    Remove-Variable -Name ColsRemoved,ColsAdded -ErrorAction SilentlyContinue

    return $SqlColumnDefinition

} # End Function: Get-ColumnDefinitionsAndCompare

#y
function Start-SqlLogging {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlServerName
       ,[Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlDatabaseName
       ,[Parameter(Mandatory=$true,ParameterSetName='ConnString')][Security.SecureString]$SqlConnString_Secure
       ,[Parameter(Mandatory=$true)]$SchemaName
       ,[Parameter(Mandatory=$true)]$LogTableName
       ,[Parameter(Mandatory=$false)]$TableName
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Start-SqlLogging .....
.NOTES
    NAME: Start-SqlLogging
    HISTORY:
        Date                Author                                         Notes:
        04/06/2018          Benjamin Reynolds (breynol@microsoft.com)      Initial Creation
        04/20/2018          Benjamin Reynolds (breynol@microsoft.com)      Added 'by table' logging logic
#>

    $ReturnObj = New-Object System.Collections.ArrayList

    # Create the query to run:
    if ($LogTableName -eq 'PowerShellRefreshHistory') {
        $SqlQry = "INSERT $SchemaName.$LogTableName (StartDateUTC) VALUES (DEFAULT);
SELECT SCOPE_IDENTITY() AS [ID];"
    }
    if ($LogTableName -eq 'TableRefreshHistory') {
        if (!$TableName) {
            $TmpRtnObj = New-Object -TypeName PSObject -Property @{"ErrorCaptured" = "No TableName Provided!";"Value" = -1}
            [void]$ReturnObj.Add($TmpRtnObj)
            return $ReturnObj
        }
        $SqlQry = "INSERT $SchemaName.$LogTableName (TableName,StartDateUTC) VALUES (N'$TableName',DEFAULT);
SELECT SCOPE_IDENTITY() AS [ID];"
    }

    # Connect to SQL and start logging:
    if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
        $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
        $CurSqlId = Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query $SqlQry -ErrorVariable SqlErrorCaptured -ErrorAction SilentlyContinue
    }
    else {
        $CurSqlId = Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query $SqlQry -ErrorVariable SqlErrorCaptured -ErrorAction SilentlyContinue
    }
    
    

    # Create the return object (include the error if one was caught):
    if ($SqlErrorCaptured) {
        $TmpRtnObj = New-Object -TypeName PSObject -Property @{"ID"=$CurSqlId;"ErrorCaptured" = $SqlErrorCaptured;"Value" = -1}
        [void]$ReturnObj.Add($TmpRtnObj)
        return $ReturnObj
    }
    else {
        $TmpRtnObj = New-Object -TypeName PSObject -Property @{"ID"=$CurSqlId;"Value" = 0}
        [void]$ReturnObj.Add($TmpRtnObj)
        return $ReturnObj
    }
} # End: Start-SqlLogging

#y
function Update-SqlLogging {
    [cmdletbinding(PositionalBinding=$false)]
    param (
        [Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlServerName
       ,[Parameter(Mandatory=$true,ParameterSetName='NoConnString')]$SqlDatabaseName
       ,[Parameter(Mandatory=$true,ParameterSetName='ConnString')][Security.SecureString]$SqlConnString_Secure
       ,[Parameter(Mandatory=$true)]$SchemaName
       ,[Parameter(Mandatory=$true)]$LogTableName
       ,[Parameter(Mandatory=$true)][int]$PK_ID
       #,[Parameter(Mandatory=$false)][bool]$IsFinished=$true
       ,[Parameter(Mandatory=$false)][int]$ErrorNumber
       ,[Parameter(Mandatory=$false)][string]$ErrorMessage
    )
<#
.SYNOPSIS
    This function is used to ...
.DESCRIPTION
    The function ...
.EXAMPLE
    Update-SqlLogging .....
.NOTES
    NAME: Update-SqlLogging
    HISTORY:
        Date                Author                                         Notes:
        04/06/2018          Benjamin Reynolds (breynol@microsoft.com)      Initial Creation
        04/20/2018          Benjamin Reynolds (breynol@microsoft.com)      Added check for ErrorMessage
#>

    $ReturnObj = New-Object System.Collections.ArrayList
    
    # Fix the ErrorMessage variable if it exists to account for the single quotes:
    if ($ErrorMessage) {
        $ErrorMessage = $ErrorMessage.Replace("'","''")
    }
    
    # Create the query to run:
    if ($ErrorNumber) {
        if ($ErrorMessage) {
            $SqlQry = "UPDATE $SchemaName.$LogTableName
   SET EndDateUTC = SYSUTCDATETIME()
      ,ErrorNumber = $ErrorNumber
      ,ErrorMessage = N'$ErrorMessage'
 WHERE ID = $PK_ID;"
        }
        else {
            $SqlQry = "UPDATE $SchemaName.$LogTableName
   SET EndDateUTC = SYSUTCDATETIME()
      ,ErrorNumber = $ErrorNumber
 WHERE ID = $PK_ID;"
        }
    }
    else {    
        $SqlQry = "UPDATE $SchemaName.$LogTableName
   SET EndDateUTC = SYSUTCDATETIME()
 WHERE ID = $PK_ID;"
    }


    # Connect to SQL and log the completion:
    if ($PsCmdlet.ParameterSetName -eq 'ConnString') {
        $SqlCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "Me",$SqlConnString_Secure
        Invoke-Sqlcmd -ConnectionString $($SqlCred.GetNetworkCredential().Password) -Query $SqlQry -ErrorVariable SqlErrorCaptured -ErrorAction SilentlyContinue
    }
    else {
        Invoke-Sqlcmd -ServerInstance $SqlServerName -Database $SqlDatabaseName -Query $SqlQry -ErrorVariable SqlErrorCaptured -ErrorAction SilentlyContinue
    }
    
    

    # Create the return object (include the error if one was caught):
    if ($SqlErrorCaptured) {
        $TmpRtnObj = New-Object -TypeName PSObject -Property @{"ErrorCaptured" = $SqlErrorCaptured;"Value" = -1}
        [void]$ReturnObj.Add($TmpRtnObj)
        return $ReturnObj
    }
    else {
        $TmpRtnObj = New-Object -TypeName PSObject -Property @{"Value" = 0}
        [void]$ReturnObj.Add($TmpRtnObj)
        return $ReturnObj
    }
} # End: Update-SqlLogging

#y
function Get-IntuneOpStoreDataUsingSkipCounts {
    [cmdletbinding(PositionalBinding=$false)]
    param
    (
        [Parameter(Mandatory=$true)][String]$OdataUrl
       ,[Parameter(Mandatory=$false,HelpMessage="This should be the command used to create the auth token - it needs to start with Get-Authentication")]$GetAuthStringCmd
       ,[Parameter(Mandatory=$false)]$TopCount=100
       ,[Parameter(Mandatory=$false)][int]$WriteBatchSize=50000
       ,[Parameter(Mandatory=$false)][int64]$CurNumRecords=0
       ,[Parameter(Mandatory=$false)][bool]$VerboseInfo=$false
       ,[Parameter(Mandatory=$false)][int]$VerboseRecordCount=0
    )
<#
.SYNOPSIS
    This function is used to get a collection of data from the Intune Data Warehouse
.DESCRIPTION
    The function connects to the Data Warehouse URL and returns all data in a collection of data from a given starting point/URL
.PARAMETER OdataUrl
    Required.
    This is the "starting point" for the collection - all data should be collected from this point on.
.PARAMETER GetAuthStringCmd
    Not Required.
    This is the command used to create the authentication token - it needs to start with "Get-Authentication".
    This is used to re-authenticate to the service in the event the access token has expired.
.PARAMETER WriteBatchSize
    Not Required. Default = 50,000
    This is the point in which the function will stop collecting data and send the data back to the caller for processing.
    The data is sent along with the "next URL" in order to be handled by the caller and if desired, the rest of the data can be obtained by calling this function again with the link previously provided in the output object
.PARAMETER VerboseInfo
    Not Required. Default = False
    This is just to return some verbose information without using the regular "-Verbose" command (so that extra data isn't returned from Invoke-WebRequest).
.EXAMPLE
    Get-IntuneOpStoreDataUsingSkipCounts -OdataUrl "" -GetAuthStringCmd "Get-Authentication -User user@example.com -ApplicationId 4184c61a-e324-4f51-83d7-022b6a82b991 -CredentialsFile 'c:\path to encrypted password file.txt'"
    Returns all devices from the Operational Store in a batch of 50,000 records (or all records if less than this amount)
.EXAMPLE
    Get-IntuneOpStoreDataUsingSkipCounts -OdataUrl "" -GetAuthStringCmd "Get-Authentication -User user@example.com -ApplicationId 4184c61a-e324-4f51-83d7-022b6a82b991 -CredentialsFile 'c:\path to encrypted password file.txt'" -WriteBatchSize 99999999 -VerboseInfo $true
    Returns all devices from the Operational Store in a batch of 99,999,999 records (or all records if less than this amount) and writes to the host the time of the call and how many records received
.OUTPUTS
    A PSObject containing the data and the "Next URL" if one exists (or is required to get the rest of the data from the collection).
    The data is contained in the "DataObject" object and the next url in "URL" (a string).
.NOTES
    NAME: Get-IntuneOpStoreDataUsingSkipCounts
    HISTORY:
        Date          Author                                       Notes
        04/20/2018    Benjamin Reynolds (breynol@microsoft.com)    Adapted from "Get-IntuneOpStoreData" to use SkipCounts as a workaround for when nextLink isn't working correctly
        04/27/2018    Benjamin Reynolds (breynol@microsoft.com)    Added TopCount to account for API silliness...


        Example of URL to be used...then iterate the SkipCount number based on the records returned...
        https://graph.microsoft.com/v1.0/deviceManagement/deviceCompliancePolicies/3111ad75-9fe5-490d-b513-d63151af7681/deviceStatuses?$skiptoken=skipCount%3d100

ISSUES: 
The 'WriteBatchSize' doesn't work when the records returned are not exactly the count - i.e., if 50,000 is the WriteBatchSize/VerboseRecordCount and 50,050 records are gotten then we don't hit the logic to write the info out...

#>

    if (!$global:ADAuthResult) {
        if ($GetAuthStringCmd) {
            Invoke-Expression $GetAuthStringCmd
        }
        else {
            try {
                Get-Authentication -User "$env:USERNAME@microsoft.com"
            }
            catch {
                throw "No authentication context. Authenticate first by running 'Get-Authentication'"
            }
        }
    }
    
    if ($OdataUrl -notlike '*skiptoken*') {
        #$URL = "$($OdataUrl)?`$skiptoken=skipCount%3d$CurNumRecords"
        $URL = "$($OdataUrl)?`$top=$TopCount&`$skiptoken=skipCount%3d$CurNumRecords"
    }
    else {
        $URL = $OdataUrl
    }

    # Variable to handle output:
    [int]$WriteCounter = 0
    [int]$SkipCountNum = $CurNumRecords
    
    # Variables to handle retries:
    [int]$ReconnRetry = 0
    [int]$GatewayTimeoutRetry = 0
    [int]$ServerUnavailableRetry = 0
    
    while ($URL) {
        $clientRequestId = [Guid]::NewGuid()
        $headers = @{
                    'Content-Type'='application/json'
                    'Authorization'="Bearer " + $global:ADAuthResult.AccessToken
                    'ExpiresOn'= $global:ADAuthResult.ExpiresOn
                    'client-request-id'=$clientRequestId
                    }
        try {
    
            #if (($VerboseInfo) -and (($VerboseRecordCount -eq 0) -or ($(if ($VerboseRecordCount -gt 0) {($CurNumRecords % $VerboseRecordCount -eq 0) -or ($CurNumRecords % $WriteBatchSize -eq 0)} else {$CurNumRecords -eq $VerboseRecordCount})))) {
            ##if (($VerboseInfo) -and (($VerboseRecordCount -eq 0) -or ($TotalRecordsReceived -ge $VerboseRecordCount))) {
            if (($VerboseInfo) -and ($GatewayTimeoutRetry -ne 0) -and ($ServerUnavailableRetry -ne 0) -and (($VerboseRecordCount -eq 0) -or ($WriteCounter -eq 0) -or ($TotalRecordsReceived -ge ($WriteCounter * $VerboseRecordCount)))) {
            #if (($VerboseInfo) -and ($WriteCounter -eq 0)) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Getting Records Greater Than: $CurNumRecords | With a batch size of $($WriteBatchSize.ToString("###,###"))" -ForegroundColor Cyan
                #Write-Host "Calling service with URL: $URL" -ForegroundColor Yellow
                # don't iterate $WriteCounter yet...
            }
            
               #$Response = Invoke-WebRequest -Uri $URL -Method Get -Headers $headers
            $Response = Invoke-RestMethod -Uri $URL -Method Get -Headers $headers
            
            [int]$CurRecordsReceived = $Response.value.Count  # current call's record count
            [int]$TotalRecordsReceived += $CurRecordsReceived # current batch's record count
            $CurNumRecords += $CurRecordsReceived             # total record count across all batches
            $SkipCountNum += $TopCount

            #Write-Host "CurRecordsReceived = $CurRecordsReceived; CurNumRecords = $CurNumRecords;" -ForegroundColor Yellow
            
            #if (($VerboseInfo) -and (($VerboseRecordCount -eq 0) -or ($(if ($VerboseRecordCount -gt 0) {$CurNumRecords % $VerboseRecordCount -eq 0} else {$CurNumRecords -eq $VerboseRecordCount})))) {
            #if (($VerboseInfo) -and (($VerboseRecordCount -eq 0) -or ($(if ($VerboseRecordCount -gt 0) {$TotalRecordsReceived -ge $VerboseRecordCount})))) {
            if (($VerboseInfo) -and (($VerboseRecordCount -eq 0) <#-or ($WriteCounter -eq 0)#> -or ($(if ($WriteCounter -gt 0) {($TotalRecordsReceived -ge ($WriteCounter * $VerboseRecordCount))} else {$false})))) {
                #if ($VerboseRecordCount -eq 0) {
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Records Received = $TotalRecordsReceived" -ForegroundColor Cyan
                #}
                #else {
                #Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Records Received = $TotalRecordsReceived" -ForegroundColor Cyan
                #}
                $WriteCounter += 1
            }
            
               #$JsonResponse += $($Response.Content | ConvertFrom-Json).value
            $JsonResponse += $Response.value
    
               #$URL = $($Response.Content | ConvertFrom-Json).'@odata.nextLink'
                #$URL = $Response.'@odata.nextLink'
            if ($CurRecordsReceived -gt 0) {
                #$URL = "$($URL.Substring(0,$URL.IndexOf('skipCount%3d')+12))$CurNumRecords"
                $URL = "$($URL.Substring(0,$URL.IndexOf('skipCount%3d')+12))$SkipCountNum"
            }
            else {
                $URL = $null
            }

            #Write-Host "New URL: $URL" -ForegroundColor Yellow
    
            # if we successfully got here then we can safely reset the gateway timeout and server unavailable retry count...
            $GatewayTimeoutRetry = 0
            $ServerUnavailableRetry = 0
            
            ## Check to see if we've hit the batch size:
            # the gt 0 records is in the event the URL returned 0 records; If so we don't want to hit this
            #v2 'old' logic:
            #if ($CurNumRecords % $WriteBatchSize -eq 0 -and $CurNumRecords -gt 0 -and $CurRecordsReceived -gt 0) {
            if ($TotalRecordsReceived -ge $WriteBatchSize -and $CurRecordsReceived -gt 0) {
                if ($VerboseInfo) {Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : We've hit the BatchSize so sending data back for processing..." -ForegroundColor Cyan}
                break # stop the while loop and return the object to the caller for processing
            }
            
            ### Old logic for WriteBatchSize:
            ## the gt 0 records is in the event the URL returned 0 records; If so we don't want to hit this
            #if ($TotalRecordsReceived % $WriteBatchSize -eq 0 -and $CurNumRecords -gt 0) { ## ($TotalRecordsReceived % $WriteBatchSize -eq 0 -and $CurNumRecords -gt 0) -or ($CurNumRecords % $WriteBatchSize -eq 0) # should use $CurRecordsReceived instead of $CurNumRecords???
            #    if ($VerboseInfo) {Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : We've hit the Write BatchSize so sending data back for processing..." -ForegroundColor Cyan}
            #    break # stop the while loop and return the object to the caller for processing
            #}

        }
        catch [System.Net.WebException] {
            # Check for authentication expiry issues:
            if ((($_.ErrorDetails -like "*Access token has expired*") -eq $true) -or (($_.ErrorDetails -like "*(401) Unauthorized*") -eq $true)) {
                # this reconnection retry stuff works because the URL is the same at this point and is retried when the loop continues on...
                $ReconnRetry += 1
                if ($VerboseInfo) {Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : We Need to Handle the timeout of the Access Token; Going to try to re-authenticate after 5 seconds..."}
                Start-Sleep -Seconds 5
                # Re-connect to get a new access token
                if ($GetAuthStringCmd) {
                    Invoke-Expression $GetAuthStringCmd
                }
                else {
                    Get-Authentication -User "$env:USERNAME@microsoft.com"
                }
                # Check to see if the reconnect worked:
                if (($global:ADAuthResult.ExpiresOn.datetime - $((Get-Date).ToUniversalTime())).Minutes -ge 10) {
                    $ReconnRetry = 0
                    continue # this continues the while loop
                }
                else {
                    if ($ReconnRetry -le 3) {
                        Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Unable to get a new AccessToken will try again. Retry $ReconnRetry of 3."
                        # don't do anything so it tries again...no explicit retry of the connection but the URL is the same and will try again in the loop..
                    }
                    else {
                        $CatchEndLoop = $true
                        Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Unable to get a new AccessToken for the last number of retries; returning to caller." -ForegroundColor Red
                        Write-Host $_.Exception -ForegroundColor Red # Is this enough or do we want more data?
                        break # this breaks out of the while loop
                    }
                }
            } #End AccessToken Expiration if block
            # If not expiry, check for known errors and handle appropriately:
            elseif (($_.Exception -like "*(504) Gateway Timeout*") -eq $true) {
                # this retry works because the URL is the same at this point and is retried when the loop continues on...
                $GatewayTimeoutRetry += 1
                
                if ($GatewayTimeoutRetry -le 5) {
                    Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Gateway Timed out! (Records Received thus far: $TotalRecordsReceived); will try again..."
                    continue
                }
                else {
                    $CatchEndLoop = $true
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Gateway Timed out for the last number of retries; returning to caller" -ForegroundColor Red
                    break
                }
            }
            elseif (($_.Exception -like "*(503) Server Unavailable*") -eq $true) {
                # this retry works because the URL is the same at this point and is retried when the loop continues on...
                $ServerUnavailableRetry += 1
                
                if ($ServerUnavailableRetry -le 5) {
                    Write-Warning "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Server Unavailable! (Records Received thus far: $TotalRecordsReceived); will try again in 3 minutes..."
                    Start-Sleep -Seconds 300
                    continue
                }
                else {
                    $CatchEndLoop = $true
                    Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Server Unavailable for the last number of retries; returning to caller" -ForegroundColor Red
                    break
                }
            }
            elseif (($_.Exception -like "*(400) Bad Request*") -eq $true) {
                $CatchEndLoop = $true
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Bad Request Error Caught; will return to caller." -ForegroundColor Red
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Bad Request URL: $URL" -ForegroundColor Red
                break # this breaks out of the while loop
            }
            elseif (($_.Exception -like "*(403) Forbidden*") -eq $true) {
                $CatchEndLoop = $true
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Forbidden Error Caught; will return to caller." -ForegroundColor Red
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Forbidden URL (need to get access to this resource): $URL" -ForegroundColor Red
                break # this breaks out of the while loop
            }
            else {
                $CatchEndLoop = $true
                Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Unhandled Error Encountered. Original Error is:" -ForegroundColor Red
                Write-Host $_.Exception -ForegroundColor Red
                break # this breaks out of the while loop
            }
        } # End Catch block
    } # End While Loop
    
    if (!$TotalRecordsReceived) {$TotalRecordsReceived = 0}
    # if URL doesn't exist, we've hit the WriteBatchSize, or we've used a "break" in the catch block we'll get to this section...
    if ($CatchEndLoop) { # We broke the loop due to exceptions
        #if ($CurNumRecords -ne $TotalRecordsReceived) {Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There is a mismatch in record counts: $(if ($TotalRecordsReceived) {$TotalRecordsReceived} else {"null"}) (TotalRecordsReceived) vs $CurNumRecords (CurNumRecords)" -ForegroundColor Yellow}
        if ($VerboseInfo) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Records Received (before error encountered): Total Count = $CurNumRecords ; Batch Count = $TotalRecordsReceived" -ForegroundColor Cyan
        }
        if ($JsonResponse.Count -gt 0) {
            $ReturnObj = New-Object -TypeName PSObject -Property @{"DataObject"=$JsonResponse;"URL"=$URL;"RecordCount"=$CurNumRecords;"BatchRecordCount"=$TotalRecordsReceived;"ErrorCaught"="true"}
        }
        else {
            $ReturnObj = New-Object -TypeName PSObject -Property @{"URL"=$URL;"RecordCount"=$CurNumRecords;"BatchRecordCount"=$TotalRecordsReceived;"ErrorCaught"="true"}
        }
    }
    else { # No break used in the catch block: We got all data or hit the WriteBatchSize
        #if ($CurNumRecords -ne $TotalRecordsReceived) {Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : There is a mismatch in record counts: $(if ($TotalRecordsReceived) {$TotalRecordsReceived} else {"null"}) (TotalRecordsReceived) vs $CurNumRecords (CurNumRecords)" -ForegroundColor Yellow}
        if ($VerboseInfo) {
            Write-Host "$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff") : Records Received: Total Count = $CurNumRecords ; Batch Count = $TotalRecordsReceived" -ForegroundColor Cyan
        }
        $ReturnObj = New-Object -TypeName PSObject -Property @{"DataObject"=$JsonResponse;"URL"=$URL;"RecordCount"=$CurNumRecords;"BatchRecordCount"=$TotalRecordsReceived;"ErrorCaught"="false"}
    }

    return $ReturnObj
} #End: Get-IntuneOpStoreDataUsingSkipCounts


# <<<<<<<    IN PROGRESS FUNCTIONS    >>>>>>
