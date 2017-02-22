USE [SEPDB_ONLINE]
GO
/****** Object:  StoredProcedure [dbo].[USP_SE_RTL_ScanDataStatusForScanStores]    Script Date: 10/12/2016 10:52:09 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
=============================================================================================================================================           
 OBJECT       : STORED PROCEDURE  [dbo].[USP_SE_RTL_ScanDataStatusForScanStores]
-=============================================================================================================================================             
*********************************************************************      
**   Name         : dbo.USP_SE_RTL_ScanDataStatusForScanStores
**   Desc         : Process to define the ScanData Status for Scan Stores Submitting ScanData.
**   AUTHOR       : Capgemini   
**   CREATED DATE : 2016-08-10
********************************************************************************************************************************************       
**              CHANGE HISTORY        
*********************************************************************************************************************************************       
** Date:            Author:     Description:
** ** *******************************************************************************************************************************************/
--EXEC DBO.[USP_SE_RTL_ScanDataStatusForScanStores] 'USP_SE_RTL_ScanDataStatusForScanStores',''
ALTER PROCEDURE [dbo].[USP_SE_RTL_ScanDataStatusForScanStores]
(
	@InterfaceName NVARCHAR(100)
	,@RunDate NVARCHAR(25)
)
AS
BEGIN
	SET NOCOUNT ON;

	-- interfering with SELECT statements.
	BEGIN TRY

			DECLARE @l_RunDate					DATETIME 
					,@l_UserName				NVARCHAR(50)
					,@l_TerminationDate         DATETIME
					,@l_AttributeTypeGUID		UNIQUEIDENTIFIER
					,@ScanStatusOPCOXML			XML
					
			SET @l_RunDate = CONVERT(DATE, ISNULL(NULLIF(@RunDate, ''), GETDATE()))
			SET @l_TerminationDate='9999-12-31 23:59:59.000' 
			SET @l_UserName = SUSER_SNAME()
			
			--Droping Temp tables
			IF OBJECT_ID('TEMPDB..#ScanRecords') IS NOT NULL
				DROP TABLE #ScanRecords
			IF OBJECT_ID('TEMPDB..#OPCO') IS NOT NULL
				DROP TABLE #OPCO
			IF OBJECT_ID('TEMPDB..#StoresWithNewScanDataStatus') IS NOT NULL
				DROP TABLE #StoresWithNewScanDataStatus
			IF OBJECT_ID('TEMPDB..#RPOEffectiveData') IS NOT NULL
				DROP TABLE #RPOEffectiveData
			IF OBJECT_ID('TEMPDB..#SalesCycleDates') IS NOT NULL
				DROP TABLE #SalesCycleDates

			--Create Temp Tables
			CREATE TABLE #OPCO
			(
			     OPCOCompanyObjectGUID		UNIQUEIDENTIFIER
				,CategoryCompanyObjectGUID  UNIQUEIDENTIFIER
				,CategoryCompanyObjectCode	NVARCHAR(100)
				,FirstChainSubmittedDate	DATETIME
			)

			CREATE TABLE #SalesCycleDates
			(
				 ProcessingCycleStartDate	DATETIME
				,ProcessingCycleCode		NVARCHAR(20)
				,ProcessingQtrStartDate		DATETIME		
				,PreviousQtrStartDate		DATETIME
				,PreviousQtrEndDate			DATETIME
				,NextCycleStartDate			DATETIME
			)

			CREATE TABLE #ScanRecords
			(
				 RetailStoreGUID			 UNIQUEIDENTIFIER
				,OPCOCompanyObjectGUID		 UNIQUEIDENTIFIER
				,CategoryCompanyObjectGUID   UNIQUEIDENTIFIER
				,FirstSubmissionDate		 DATETIME
				,ScanIncentiveOptinStatus	 BIT 
				,ScanDataStatus				 NVARCHAR(300)
			)
			CREATE TABLE #RPOEffectiveData
			(
				 RetailStoreGUID			     UNIQUEIDENTIFIER
				,CategoryCompanyObjectGUID		 UNIQUEIDENTIFIER												
			)
			CREATE TABLE #StoresWithNewScanDataStatus
			(
				 RetailstoreExtendedAttributesGUID	UNIQUEIDENTIFIER
				,RetailStoreGuid					UNIQUEIDENTIFIER
				,CategoryCompanyObjectGUID			UNIQUEIDENTIFIER
				,ScanDataStatus						NVARCHAR(300)
				,NewStatusEffectiveDate				DATETIME
			)
		

			--Fetching System Setting values for configured SCAN-UNMATCHED-MATERIALID
			SELECT @ScanStatusOPCOXML = SystemSettingXMLValue
			FROM DBO.SEP_SystemSetting WITH(NOLOCK)
			WHERE Active = 1
				AND SystemSettingName = 'SCAN-UNMATCHED-MATERIALID'

			
			--Get the configured CategoryCompanyObjectCode, with OPCOMaterialIDs and AOMMaterialIds
			INSERT INTO #OPCO (CategoryCompanyObjectCode,FirstChainSubmittedDate)										
			SELECT t.c.value('@CategoryCompanyObjectCode', 'NVARCHAR(100)') AS CategoryCompanyObjectCode,
				   CONVERT(DATETIME,t.c.value('@FirstChainSubmittedDate', 'NVARCHAR(40)'),126) AS FirstChainSubmittedDate		
			FROM @ScanStatusOPCOXML.nodes('//ScanData/Config') AS t(c)
			WHERE t.c.value('@FirstChainSubmittedDate', 'NVARCHAR(40)') <>''				--Only OPCOs having configured FirstSubmissionDate will be processed
			
			--Get companyobjectGUID for respective OPCOs/Categories
			UPDATE OP
			SET  OP.OPCOCompanyObjectGUID  = OPCO.CompanyObjectGUID
				,OP.CategoryCompanyObjectGUID = CAT.CompanyObjectGUID
			FROM #OPCO OP 
			INNER JOIN DBO.SEP_CompanyObject OPCO WITH (NOLOCK)
				ON SUBSTRING(OP.CategoryCompanyObjectCode,1,7) = OPCO.CompanyObjectCode
				AND OPCO.Active=1
			INNER JOIN DBO.SEP_CompanyObject CAT WITH (NOLOCK)
				ON OP.CategoryCompanyObjectCode = CAT.CompanyObjectCode
				AND CAT.Active=1
			
			--index
			--CREATE INDEX IDX_#OPCO ON #OPCO (OPCOCompanyObjectGUID) INCLUDE (CategoryCompanyObjectGUID)
			
			--Get ProcessingCycleStartDate,ProcessingCycleCode,ProcessingQtrStartDate,CurrentQtrEndDate,PreviousQtrStartDate,PreviousQtrEndDate
			INSERT INTO #SalesCycleDates										
			(				 		
				 ProcessingCycleCode			
				,ProcessingCycleStartDate
				,ProcessingQtrStartDate			
				,PreviousQtrStartDate	
				,PreviousQtrEndDate		
				,NextCycleStartDate		
			)
			SELECT SC.CalendarCode
			     ,SC.StartDate 
				 ,QC.StartDate
				 ,PQC.StartDate
				 ,PQC.EndDate
				 ,NSC.StartDate 
			FROM dbo.SEP_PeriodType PT WITH (NOLOCK)
			INNER JOIN dbo.SEP_Calendar SC WITH (NOLOCK)
				ON SC.PeriodTypeGUID = PT.PeriodTypeGUID
				AND PT.PeriodTypeCode = 'MM'
				AND SC.Active = 1 AND PT.Active = 1
				AND @l_RunDate BETWEEN SC.StartDate AND SC.EndDate
			INNER JOIN dbo.SEP_Calendar QC WITH (NOLOCK)
				ON SC.EndDate BETWEEN QC.StartDate AND QC.ENdDate
			INNER JOIN dbo.SEP_PeriodType QPT WITH(NOLOCK)
				ON QPT.PeriodTypeGUID = QC.PeriodTypeGUID
				AND QPT.PeriodTypeCode = 'QTR'
				AND QPT.Active = 1
			INNER JOIN dbo.SEP_Calendar PQC WITH (NOLOCK)
				ON DATEADD(DD,-1,QC.StartDate) BETWEEN PQC.StartDate AND PQC.EndDate
				AND PQC.Active = 1
				AND PQC.PeriodTypeGUID = QPT.PeriodTypeGUID
			INNER JOIN dbo.SEP_Calendar NSC WITH(NOLOCK)
				ON DATEADD(DD,1,SC.EndDate) BETWEEN NSC.StartDate AND NSC.EndDate
				AND NSC.PeriodTypeGUID = PT.PeriodTypeGUID
				AND NSC.Active = 1

			--index
			--CREATE INDEX IDX1_#SalesCycleDates ON #SalesCycleDates (ProcessingCycleCode)
			--CREATE INDEX IDX2_#SalesCycleDates ON #SalesCycleDates (ProcessingCycleStartDate,ProcessingQtrStartDate,NextCycleStartDate)	
			--CREATE INDEX IDX3_#SalesCycleDates ON #SalesCycleDates (PreviousQtrStartDate,PreviousQtrEndDate)	
			
			--Get Scan Records along with first Scandata submission Date
			INSERT INTO #ScanRecords
			(
				 RetailStoreGUID
				,OPCOCompanyObjectGUID
				,CategoryCompanyObjectGUID
				,FirstSubmissionDate
				,ScanIncentiveOptinStatus								
			)
			SELECT   SD.RetailStoreGUID
					,CO.OPCOCompanyObjectGUID 
					,CO.CategoryCompanyObjectGUID
					,Min(SD.ChainSubmittedDate) AS FirstSubmissionDate
					,0 AS ScanIncentiveOptinStatus						
			FROM #OPCO CO
			INNER JOIN DBO.OLTP_SEP_ScanData_RetailReporting  SD WITH (NOLOCK)
					ON CO.CategoryCompanyObjectGUID = SD.CategoryCompanyObjectGUID
					AND SD.Active = 1
			WHERE 	SD.ChainSubmittedDate >= CO.FirstChainSubmittedDate					-- ChainSubmittedDate > configured cateogry FirstSubmissionDate	records will be considered only		
			GROUP BY SD.RetailStoreGUID
					,CO.OPCOCompanyObjectGUID
					,CO.CategoryCompanyObjectGUID
					
			--Get RPO data effective on Rundate for Scan Incentive Option
			INSERT INTO #RPOEffectiveData												
			( 
					 RetailStoreGUID				 
					,CategoryCompanyObjectGUID	
			)
			SELECT   RPO.RetailStoreGUID				 
					,CO.CategoryCompanyObjectGUID
			FROM #OPCO CO				
			INNER JOIN DBO.SEP_Initiative I WITH (NOLOCK) 
				ON CO.OPCOCompanyObjectGUID = I.CompanyObjectGUID	
				AND @l_RunDate BETWEEN I.EndSellDate AND I.InitiativeEndDate 				
			INNER JOIN  DBO.SEP_InitiativeType IT WITH (NOLOCK)	
				ON I.InitiativeTypeGUID = IT.InitiativeTypeGUID 
				AND IT.InitiativeTypeCode = '00017'											--ScanIncentive Option
				AND IT.Active = 1
				AND I.Active = 1
			INNER JOIN DBO.SEP_InitiativeSubType IST WITH (NOLOCK)
				On IST.InitiativeSubTypeGUID = I.InitiativeSubTypeGUID
				AND IST.InitiativeSubTypeCode IN ('00015')									--User Optin
				AND IST.Active = 1
			INNER JOIN DBO.SEP_RetailstorePerformanceOption RPO WITH (NOLOCK)
				ON  RPO.InitiativeGUID = I.InitiativeGUID
				AND RPO.BrandCompanyObjectGUID =I.CompanyObjectGUID
				AND RPO.STATUS = 'Valid'
				AND RPO.Active = 1
				AND @l_RunDate BETWEEN RPO.EffectiveDate AND RPO.TerminationDate
			

			--index
			CREATE INDEX IDX_#RPOEffectiveData ON #RPOEffectiveData (RetailStoreGUID,CategoryCompanyObjectGUID) 
			
			--Get the scan optin status as well as Optin Effective Date for scan stores
			UPDATE SD
			SET	 SD.ScanIncentiveOptinStatus	=	1										
			FROM  #ScanRecords SD
			INNER JOIN 	#RPOEffectiveData RPO
				ON SD.RetailStoreGUID = RPO.RetailStoreGUID
				AND SD.CategoryCompanyObjectGUID = RPO.CategoryCompanyObjectGUID
			
			--Update the scandatastatus depending on First scanData submission and ScanOptin
			UPDATE SS
			SET SS.ScanDataStatus = CASE WHEN SS.ScanIncentiveOptinStatus = 0	THEN 'S'		--Submitted Status,if stores submits ScanData and not opts in ScanData as well as for stores Opts Out of Scan Incentive Option
								         ELSE 
											CASE
												 WHEN SS.FirstSubmissionDate >= SC.ProcessingCycleStartDate THEN 'O'										--Onboarding Status,if stores submits ScanData and opts in ScanData. First file submission Date fall in Current Quarter.
												 WHEN SS.FirstSubmissionDate BETWEEN SC.PreviousQtrStartDate  AND SC.PreviousQtrEndDate THEN 'T'	--Transition Status,if stores submits ScanData and opts in ScanData. First file submission Date fall in Previous Quarter.
												 WHEN SS.FirstSubmissionDate < SC.PreviousQtrStartDate THEN 'C'										--Automated  Status,if stores submits ScanData and opts in ScanData. First file submission Date fall in Prior to Previous Quarter.
											END
								   END 
			FROM #ScanRecords SS
			CROSS JOIN #SalesCycleDates SC
			
			--Index
			CREATE INDEX IDX_#ScanRecords ON #ScanRecords(RetailStoreGUID,CategoryCompanyObjectGUID,ScanDataStatus) 

			--Get AttributeTypeGUID for Scan Automation
			SELECT @l_AttributeTypeGUID=AttributeTypeGUID
			FROM DBO.SEP_Attributetype WITH (NOLOCK)
			WHERE AttributeTypeCode='00010'			--Scan Automation
			AND Active = 1
				  
			--Comparing new status determined with existing status in REA table
			INSERT INTO #StoresWithNewScanDataStatus
			(
					 RetailstoreExtendedAttributesGUID
					,RetailStoreGUID
					,CategoryCompanyObjectGUID
					,ScanDataStatus
			)
			SELECT   REA.RetailstoreExtendedAttributesGUID
					,SS.RetailStoreGUID
					,SS.CategoryCompanyObjectGUID
					,SS.ScanDataStatus
			FROM  #ScanRecords SS
			LEFT JOIN DBO.SEP_RetailStoreExtendedAttributes REA  WITH (NOLOCK)
				ON SS.RetailStoreGUID = REA.RetailStoreGUID
				AND SS.CategoryCompanyObjectGUID = REA.CompanyObjectGUID
				AND REA.AttributeTypeGUID		 = @l_AttributeTypeGUID
				AND @l_RunDate BETWEEN REA.EffectiveDate AND REA.TerminationDate
				AND REA.Active = 1
			WHERE SS.ScanDataStatus <> ISNULL(REA.AttributeValue,'') 

			--Get the Effective date for new status 
			--If STATUS = 'S','O','T' set effectiveDate= Rundate
			--If STATUS = 'C' set effective = Rundate if Rundate is Cyclestartdate otherwise next cycle start date
			UPDATE SSD
			SET SSD.NewStatusEffectiveDate = CASE WHEN SSD.ScanDataStatus = 'S' OR SSD.ScanDataStatus = 'O' OR  SSD.ScanDataStatus = 'T'  THEN @l_RunDate			--20/02/2017
											      WHEN SSD.ScanDataStatus = 'C' THEN
													 Case WHEN CAST(@l_RunDate AS DATE)= CAST(SC.ProcessingCycleStartDate AS DATE) THEN SC.ProcessingCycleStartDate 
													 ELSE SC.NextCycleStartDate  
													 END
												END							
			FROM #StoresWithNewScanDataStatus SSD
			CROSS JOIN #SalesCycleDates SC
				
			--Index
			CREATE INDEX IDX_#StoresWithNewScanDataStatus ON #StoresWithNewScanDataStatus(RetailstoreExtendedAttributesGUID) INCLUDE (RetailStoreGUID,CategoryCompanyObjectGUID,ScanDataStatus,NewStatusEffectiveDate)
						
			--Terminate Records whose status has changed
			UPDATE REA
			SET  REA.TerminationDate = DATEADD(SS,-1,DR.NewStatusEffectiveDate) 
				,REA.UpdateUser		= @l_UserName
				,REA.UpdateDate		= @l_RunDate
			FROM #StoresWithNewScanDataStatus DR 
			INNER JOIN DBO.SEP_RetailStoreExtendedAttributes REA  
				ON REA.RetailstoreExtendedAttributesGUID = DR.RetailstoreExtendedAttributesGUID
				AND REA.Active = 1

			--Inserting stores with new ScanData Status
			INSERT INTO DBO.SEP_RetailStoreExtendedAttributes
			(
					 RetailStoreGUID
					,CompanyObjectGUID
					,AttributeTypeGUID
					,AttributeValue
					,EffectiveDate
					,TerminationDate
					,CreateDate
					,UpdateDate
					,CreateUser
					,UpdateUser
					,Active
			)
			SELECT   RetailStoreGUID
					,CategoryCompanyObjectGUID 
					,@l_AttributeTypeGUID AS AttributeTypeGUID
					,ScanDataStatus AS AttributeValue
					,NewStatusEffectiveDate AS EffectiveDate																						
					,@l_TerminationDate AS TerminationDate
					,@l_RunDate AS CreateDate
					,@l_RunDate AS UpdateDate
					,@l_UserName AS CreateUser
					,@l_UserName AS UpdateUser
					,1 AS Active
			FROM #StoresWithNewScanDataStatus 
			

END TRY

	BEGIN CATCH
		--Log the exceptions
		INSERT INTO [ETL].[SEP_Interface_Proc_Exception] (
			[InterfaceName]
			,[ProcedureName]
			,[ErrorMessage]
			,[ErrorLine]
			,[ErrorState]
			,[ErrorSeverity]
			,[CreatedUser]
			,[CreatedDate]
			)
		SELECT @InterfaceName
			,ERROR_PROCEDURE()
			,ERROR_MESSAGE()
			,ERROR_LINE()
			,ERROR_STATE()
			,ERROR_SEVERITY()
			,SUSER_SNAME()
			,GETDATE();
	END CATCH

		
		--Droping Temp tables
			IF OBJECT_ID('TEMPDB..#ScanRecords') IS NOT NULL
				DROP TABLE #ScanRecords
			IF OBJECT_ID('TEMPDB..#OPCO') IS NOT NULL
				DROP TABLE #OPCO
			IF OBJECT_ID('TEMPDB..#StoresWithNewScanDataStatus') IS NOT NULL
				DROP TABLE #StoresWithNewScanDataStatus
			IF OBJECT_ID('TEMPDB..#RPOEffectiveData') IS NOT NULL
				DROP TABLE #RPOEffectiveData
			IF OBJECT_ID('TEMPDB..#SalesCycleDates') IS NOT NULL
				DROP TABLE #SalesCycleDates

	SET NOCOUNT OFF;
END

