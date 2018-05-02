IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'IntuneOpStore')
BEGIN
    EXECUTE (N'CREATE SCHEMA IntuneOpStore;');
END;
--DROP TABLE IF EXISTS dbo.managedDevices;
CREATE TABLE IntuneOpStore.managedDevices ( id nvarchar(36) NOT NULL
                                           ,userId nvarchar(36) NULL
                                           ,deviceName nvarchar(256) NULL
                                           ,deviceActionResults_JSON nvarchar(max) NULL
                                           ,enrolledDateTime datetime2 NOT NULL
                                           ,lastSyncDateTime datetime2 NOT NULL
                                           ,operatingSystem nvarchar(64) NULL
                                           ,complianceState nvarchar(13) NOT NULL
                                           ,jailBroken nvarchar(10) NULL
                                           ,managementAgent nvarchar(32) NOT NULL
                                           ,osVersion nvarchar(128) NULL
                                           ,easActivated bit NOT NULL
                                           ,easDeviceId nvarchar(256) NULL
                                           ,easActivationDateTime datetime2 NOT NULL
                                           ,azureADRegistered bit NULL
                                           ,deviceEnrollmentType nvarchar(26) NOT NULL
                                           ,activationLockBypassCode nvarchar(128) NULL
                                           ,emailAddress nvarchar(320) NULL
                                           ,azureADDeviceId nvarchar(36) NULL
                                           ,deviceRegistrationState nvarchar(30) NOT NULL
                                           ,deviceCategoryDisplayName nvarchar(25) NULL
                                           ,isSupervised bit NOT NULL
                                           ,exchangeLastSuccessfulSyncDateTime datetime2 NOT NULL
                                           ,exchangeAccessState nvarchar(11) NOT NULL
                                           ,exchangeAccessStateReason nvarchar(29) NOT NULL
                                           ,remoteAssistanceSessionUrl nvarchar(256) NULL
                                           ,remoteAssistanceSessionErrorDetails nvarchar(64) NULL
                                           ,isEncrypted bit NOT NULL
                                           ,userPrincipalName nvarchar(256) NULL
                                           ,model nvarchar(256) NULL
                                           ,manufacturer nvarchar(256) NULL
                                           ,imei nvarchar(64) NULL
                                           ,complianceGracePeriodExpirationDateTime datetime2 NOT NULL
                                           ,serialNumber nvarchar(128) NULL
                                           ,phoneNumber nvarchar(64) NULL
                                           ,androidSecurityPatchLevel nvarchar(64) NULL
                                           ,userDisplayName nvarchar(256) NULL
                                           ,configurationManagerClientEnabledFeatures_JSON nvarchar(max) NULL
                                           ,wiFiMacAddress nvarchar(64) NULL
                                           ,deviceHealthAttestationState_JSON nvarchar(max) NULL
                                           ,subscriberCarrier nvarchar(25) NULL
                                           ,meid nvarchar(256) NULL
                                           ,totalStorageSpaceInBytes bigint NOT NULL
                                           ,freeStorageSpaceInBytes bigint NOT NULL
                                           ,managedDeviceName nvarchar(256) NULL
                                           ,partnerReportedThreatState nvarchar(14) NOT NULL
                                           );

--CREATE TABLE dbo.managedDevices_deviceCompliancePolicyStates ( ParentOdataType nvarchar(max) NOT NULL
--                                                              ,ParentId nvarchar(max) NOT NULL
--                                                              ,id nvarchar(max) NOT NULL
--                                                              ,settingStates_JSON nvarchar(max) NULL
--                                                              ,displayName nvarchar(max) NULL
--                                                              ,version int NOT NULL
--                                                              ,platformType nvarchar(max) NOT NULL
--                                                              ,state nvarchar(max) NOT NULL
--                                                              ,settingCount int NOT NULL
--                                                              );

CREATE TABLE IntuneOpStore.mobileApps ( odatatype nvarchar(max) NOT NULL
                                       ,id nvarchar(max) NOT NULL
                                       ,displayName nvarchar(max) NULL
                                       ,description nvarchar(max) NULL
                                       ,publisher nvarchar(max) NULL
                                       --,largeIcon_JSON nvarchar(max) NULL
                                       ,createdDateTime datetime2 NOT NULL
                                       ,lastModifiedDateTime datetime2 NOT NULL
                                       ,isFeatured bit NOT NULL
                                       ,privacyInformationUrl nvarchar(max) NULL
                                       ,informationUrl nvarchar(max) NULL
                                       ,owner nvarchar(max) NULL
                                       ,developer nvarchar(max) NULL
                                       ,notes nvarchar(max) NULL
                                       ,publishingState nvarchar(max) NOT NULL
                                       );


CREATE TABLE IntuneOpStore.deviceCompliancePolicies ( odatatype nvarchar(max) NOT NULL
                                                     ,id nvarchar(max) NOT NULL
                                                     ,createdDateTime datetime2 NOT NULL
                                                     ,description nvarchar(max) NULL
                                                     ,lastModifiedDateTime datetime2 NOT NULL
                                                     ,displayName nvarchar(max) NOT NULL
                                                     ,version int NOT NULL
                                                     --,assignments_JSON nvarchar(max) NULL
                                                     );

--CREATE TABLE dbo.deviceCompliancePolicies_deviceStatuses ( ParentOdataType nvarchar(max) NOT NULL
--                                                          ,ParentId nvarchar(max) NOT NULL
--                                                          ,id nvarchar(max) NOT NULL
--                                                          ,deviceDisplayName nvarchar(max) NULL
--                                                          ,userName nvarchar(max) NULL
--                                                          ,deviceModel nvarchar(max) NULL
--                                                          ,complianceGracePeriodExpirationDateTime datetime2 NOT NULL
--                                                          ,status nvarchar(max) NOT NULL
--                                                          ,lastReportedDateTime datetime2 NOT NULL
--                                                          ,userPrincipalName nvarchar(max) NULL
--                                                          );


--CREATE TABLE dbo.deviceCompliancePolicies_deviceStatusOverview ( ParentOdataType nvarchar(max) NOT NULL
--                                                                ,ParentId nvarchar(max) NOT NULL
--                                                                ,id nvarchar(max) NOT NULL
--                                                                ,pendingCount int NOT NULL
--                                                                ,notApplicableCount int NOT NULL
--                                                                ,successCount int NOT NULL
--                                                                ,errorCount int NOT NULL
--                                                                ,failedCount int NOT NULL
--                                                                ,lastUpdateDateTime datetime2 NOT NULL
--                                                                ,configurationVersion int NOT NULL
--                                                                );

--DROP TABLE IF EXISTS dbo.deviceCompliancePolicies_assignments;
CREATE TABLE dbo.deviceCompliancePolicies_assignments ( ParentOdataType nvarchar(max) NOT NULL
                                                       ,ParentId nvarchar(max) NOT NULL
                                                       ,id nvarchar(max) NOT NULL
                                                       ,target_JSON nvarchar(max) NULL
                                                       );


CREATE TABLE IntuneOpStore.deviceConfigurations ( odatatype nvarchar(max) NOT NULL
                                                 ,id nvarchar(max) NOT NULL
                                                 ,lastModifiedDateTime datetime2 NOT NULL
                                                 ,createdDateTime datetime2 NOT NULL
                                                 ,description nvarchar(max) NULL
                                                 ,displayName nvarchar(max) NOT NULL
                                                 ,version int NOT NULL
                                                 );

--CREATE TABLE IntuneOpStore.deviceStatuses ( /*id nvarchar(max) NOT NULL -- CONVERT THIS INTO THE FOLLOWING TWO CUSTOM COLUMNS:
--                                           ,*/CertId nvarchar(36) NOT NULL
--                                           ,deviceId nvarchar(36) NOT NULL
--                                           ,deviceDisplayName nvarchar(max) NULL
--                                           ,userName nvarchar(max) NULL
--                                           ,deviceModel nvarchar(max) NULL
--                                           ,complianceGracePeriodExpirationDateTime datetime2 NOT NULL
--                                           ,status nvarchar(max) NOT NULL
--                                           ,lastReportedDateTime datetime2 NOT NULL
--                                           ,userPrincipalName nvarchar(max) NULL
--                                           );

CREATE TABLE dbo.deviceCompliancePolicySettingStateSummaries ( id nvarchar(max) NOT NULL
                                                              ,setting nvarchar(max) NULL
                                                              ,settingName nvarchar(max) NULL
                                                              ,platformType nvarchar(max) NOT NULL
                                                              ,unknownDeviceCount int NOT NULL
                                                              ,notApplicableDeviceCount int NOT NULL
                                                              ,compliantDeviceCount int NOT NULL
                                                              ,remediatedDeviceCount int NOT NULL
                                                              ,nonCompliantDeviceCount int NOT NULL
                                                              ,errorDeviceCount int NOT NULL
                                                              ,conflictDeviceCount int NOT NULL
                                                              );

--CREATE TABLE dbo.PowerShellRefreshHistory ( ID int IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED
--                                           ,StartDateUTC datetime2 NOT NULL DEFAULT (SYSUTCDATETIME())
--                                           ,EndDateUTC datetime2 NULL
--                                           ,ErrorNumber int NULL
--                                           ,ErrorMessage nvarchar(max) NULL
--                                           ,RunBy_User nvarchar(256) NOT NULL DEFAULT (SUSER_SNAME())
--                                           ) ON [PRIMARY];

--DROP TABLE IF EXISTS dbo.TableRefreshHistory;
--CREATE TABLE dbo.TableRefreshHistory ( ID int IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED
--                                      ,TableName sysname NOT NULL
--                                      ,StartDateUTC datetime2 NOT NULL DEFAULT (SYSUTCDATETIME())
--                                      ,EndDateUTC datetime2 NULL
--                                      ,ErrorNumber int NULL
--                                      ,ErrorMessage nvarchar(max) NULL
--                                      ,RunBy_User nvarchar(256) NOT NULL DEFAULT (SUSER_SNAME())
--                                      ) ON [PRIMARY];




