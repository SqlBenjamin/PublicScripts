# Syscommittab (aka Change Tracking) Cleanup

## PowerShell Prerequisites
These PowerShell scripts make use of a CommandLog table (creation script can be found in Ola Hallengren's maintenance solution: https://github.com/olahallengren/sql-server-maintenance-solution). So ensure that table is created first. You may need to update the scripts to look for the table in the right database - currently it assumes the table is in the "DBA" database.

They also assume that your CM (aka MEMCM, SCCM, etc) database is named with the following naming convention: CM_[3 letter site code] 

So, ensure that both of these things exist and/or you update the script(s) appropriately.

## SQL Agent Prerequisites
The SQL Jobs assume that the PowerShell scripts are located on the server in a folder named "DBA_Objects" at the root of either the D or C drive. If a different location is desired just update the script to look in the right location.

