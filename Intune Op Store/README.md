# Intune Op Store

This is a beta solution for syncing Intune Graph calls and the data returned from the service to a SQL instance.

1. The module is in folder "IntuneOperationalStoreFunctions". This should be placed in a location where your profile will pick up the module for importing unless you wish to manually import it.
2. The "SQL Environment Setup.sql" file contains the SQL table definitions. The tables must exist in order for the script to work.