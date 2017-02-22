USE [SEPDB_SCAN_DATA]
GO

SET NOCOUNT ON 
SET XACT_ABORT ON
GO
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
GO

DECLARE @SERVERNAME NVARCHAR(100)
SELECT @SERVERNAME = @@SERVERNAME

IF @SERVERNAME	! = 'PMURTPSDIQC02V2\SQLQ002' --QA
--IF @SERVERNAME	! = 'PMURTPSDID03\SQLD001' --DEV
--IF @SERVERNAME	! = 'PMURTPSDIPC02V2\SQLP002' --PROD   
	BEGIN
		  PRINT 'ERROR: INCORRECT SERVER !!'
	END
ELSE
	BEGIN
	
	DECLARE @ROWCNT1 NVARCHAR(10);
	DECLARE @l_UpdateUser NVARCHAR(20)
	SET @l_UpdateUser	= SUSER_SNAME()

	IF EXISTS (SELECT * FROM tempdb..sysobjects WHERE id=OBJECT_ID('tempdb..#tmpErrors')) DROP TABLE #tmpErrors
	CREATE TABLE #tmpErrors (Error int)

	BEGIN TRANSACTION
		
		DECLARE @ConfigUnmatchedMaterialIDXML  XML
		
		--Droping Temp tables
		IF OBJECT_ID('tempdb..#UnmatchedMaterialID') IS NOT NULL	
			DROP TABLE #UnmatchedMaterialID
		IF OBJECT_ID('tempdb..#MSAMaterialID') IS NOT NULL	
			DROP TABLE #MSAMaterialID
		IF OBJECT_ID('tempdb..#ScanReporting') IS NOT NULL	
			DROP TABLE #ScanReporting
		IF OBJECT_ID('tempdb..#ScanRetailReporting') IS NOT NULL	
			DROP TABLE #ScanRetailReporting

		--Creating Temp tables
		CREATE TABLE #MSAMaterialID
		(
			 MSAMaterialID				INTEGER
			,CategoryCompanyObjectGUID 	UNIQUEIDENTIFIER
			,OPCOSkuInd					BIT
		)

		CREATE TABLE #UnmatchedMaterialID								
		(
			 CategoryCompanyObjectCode  NVARCHAR(20)
			,CategoryCompanyObjectGUID  UNIQUEIDENTIFIER 
			,OPCOMaterialID				INTEGER
			,AOMMaterialID				INTEGER
		)
		
		CREATE TABLE #ScanReporting						
		(
			Submitter   				INTEGER
			,RetailStoreGUID            UNIQUEIDENTIFIER
			,WeekEndDate	  			DATETIME
			,ChainSubmittedDate  		DATETIME
			,MaterialID				 	INTEGER
		)

		CREATE TABLE #ScanRetailReporting						
		(
			 Submitter   				INTEGER
			,RetailStoreGUID            UNIQUEIDENTIFIER
			,WeekEndDate	  			DATETIME
			,ChainSubmittedDate  		DATETIME
			,CategoryCompanyObjectGUID 	UNIQUEIDENTIFIER
			,OPCOReportingInd     		BIT
			,AOMReportingInd      		BIT
		)		

		--Fetching System Setting values for configured SCAN-UNMATCHED-MATERIALID
		SELECT @ConfigUnmatchedMaterialIDXML = SystemSettingXMLValue
		FROM DBO.OLTP_SEP_SystemSetting WITH(NOLOCK)
		WHERE Active = 1
			AND SystemSettingName = 'SCAN-UNMATCHED-MATERIALID'
		
		--Get the configured CategoryCompanyObjectCode, with OPCOMaterialIDs and AOMMaterialIds
		INSERT INTO #UnmatchedMaterialID (CategoryCompanyObjectCode,OPCOMaterialID,AOMMaterialID)										
		SELECT t.c.value('@CategoryCompanyObjectCode', 'NVARCHAR(100)') AS CategoryCompanyObjectCode										
			  ,t.c.value('@OPCOMaterialID', 'INTEGER') AS OPCOMateriaID
			  ,t.c.value('@AOMMaterialID', 'INTEGER') AS AOMMaterialID
		FROM @ConfigUnmatchedMaterialIDXML.nodes('//ScanData/Config') AS t(c)
         
		--Identify the CategoryCompanyObject from CompanyObjectCode
		UPDATE SUB														
		SET SUB.CategoryCompanyObjectGUID = CO.CompanyObjectGUID
		FROM #UnmatchedMaterialID SUB
		INNER JOIN dbo.OLTP_SEP_CompanyObject CO
			ON CO.CompanyObjectCode = SUB.CategoryCompanyObjectCode
			AND CO.ACTIVE = 1
                 
		--Fetching all MaterialIDs having Opco SKU Indicator = 1
		INSERT INTO #MSAMaterialID
		(
		     MSAMaterialID    
		 	,CategoryCompanyObjectGUID 	
			,OPCOSkuInd 
		)
		SELECT  Material_ID AS MSAMaterialID    
		 	   ,CategoryCompanyObjectGUID 		
			   ,OPCOSkuInd																
		FROM DBO.OLTP_UVW_SKU_MSA_Details WITH (NOLOCK)				
		WHERE OPCOSkuInd = 1

		--Fetching all MaterialIDs having Opco SKU Indicator = 0 excluding MaterialIDs from previous insert
		INSERT INTO #MSAMaterialID
		(
		     MSAMaterialID    
		 	,CategoryCompanyObjectGUID 	
			,OPCOSkuInd 
		)
		SELECT  SM.Material_ID AS MSAMaterialID    
		 	   ,SM.CategoryCompanyObjectGUID 		
			   ,SM.OPCOSkuInd																
		FROM DBO.OLTP_UVW_SKU_MSA_Details SM WITH (NOLOCK)	
		LEFT JOIN #MSAMaterialID MSA	
			ON MSA.MSAMaterialID=SM.Material_ID
		WHERE MSA.MSAMaterialID IS NULL AND SM.OPCOSkuInd = 0		
		
		--Insert records from SEP_ScanData_Unmatched and SEP_ScanData tables
		INSERT INTO #ScanReporting
		(   
			Submitter
			,RetailStoreGUID
			,WeekEndDate
			,ChainSubmittedDate
			,MaterialID
		)
		SELECT DISTINCT 
				Submitter
				,RetailStoreGUID
				,WeekEndDate
				,ChainSubmittedDate
				,MaterialID
		FROM DBO.SEP_ScanData_Unmatched WITH (NOLOCK)
		WHERE RetailStoreGUID IS NOT NULL AND Active = 1 
		
		INSERT INTO #ScanReporting
		(   Submitter
			,RetailStoreGUID
			,WeekEndDate
			,ChainSubmittedDate
			,MaterialID
		)
		SELECT DISTINCT 
				Submitter
				,RetailStoreGUID
				,WeekEndDate
				,ChainSubmittedDate
				,MaterialID
		FROM DBO.SEP_ScanData  WITH (NOLOCK)
		WHERE Active = 1 
	
		--Identifying the OPCOReportingInd AND AOMReportingInd for Matched MaterialIds
		INSERT INTO #ScanRetailReporting
		(
		     Submitter   			
			,RetailStoreGUID            
			,WeekEndDate	  			
			,ChainSubmittedDate  		
			,CategoryCompanyObjectGUID 	
			,OPCOReportingInd     		
			,AOMReportingInd 		
		)
		SELECT  DSR.Submitter   	
				,DSR.RetailStoreGUID            
				,DSR.WeekEndDate	  			
				,DSR.ChainSubmittedDate  		
				,MSA.CategoryCompanyObjectGUID 	
				,CAST(MAX(CAST((CASE WHEN MSA.OPCOSkuInd = 1 THEN 1 ELSE 0 END) AS INT)) AS BIT) AS OPCOReportingInd     		
				,CAST(MAX(CAST((CASE WHEN UM.CategoryCompanyObjectGUID IS NOT NULL AND MSA.OPCOSkuInd = 0 THEN 1 ELSE 0 END) AS INT)) AS BIT) AS AOMReportingInd 
		FROM #ScanReporting DSR
		INNER JOIN #MSAMaterialID MSA
				ON DSR.MaterialID = MSA.MSAMaterialID
		LEFT JOIN #UnmatchedMaterialID UM
				ON MSA.CategoryCompanyObjectGUID = UM.CategoryCompanyObjectGUID				
		GROUP BY DSR.Submitter   	
				,DSR.RetailStoreGUID            
				,DSR.WeekEndDate	  			
				,DSR.ChainSubmittedDate  		
				,MSA.CategoryCompanyObjectGUID 	
		
		--Identifying the OPCOReportingInd AND AOMReportingInd for Unmatched MaterialIds
		--Update the reporting indicators for the stores having records with same WeekEndDate, ChainSubmittedDate and of same CategoryCompanyObject
		UPDATE SP
		SET SP.OPCOReportingInd = CASE WHEN UM.OPCOMaterialID = DSR.MaterialID THEN 1 ELSE SP.OPCOReportingInd END			
		FROM #ScanRetailReporting SP
		INNER JOIN #ScanReporting DSR
		ON  SP.RetailStoreGUID  = DSR.RetailStoreGUID
			AND SP.WeekEndDate = DSR.WeekEndDate
			AND SP.ChainSubmittedDate   = DSR.ChainSubmittedDate	
		INNER JOIN #UnmatchedMaterialID UM
			ON UM.CategoryCompanyObjectGUID = SP.CategoryCompanyObjectGUID
			AND UM.OPCOMaterialID = DSR.MaterialID
		
		UPDATE SP
		SET SP.AOMReportingInd = CASE WHEN UM.AOMMaterialID = DSR.MaterialID THEN 1 ELSE SP.AOMReportingInd  END
		FROM #ScanRetailReporting SP
		INNER JOIN #ScanReporting DSR
		ON  SP.RetailStoreGUID  = DSR.RetailStoreGUID
			AND SP.WeekEndDate = DSR.WeekEndDate
			AND SP.ChainSubmittedDate   = DSR.ChainSubmittedDate	
		INNER JOIN #UnmatchedMaterialID UM
			ON UM.CategoryCompanyObjectGUID = SP.CategoryCompanyObjectGUID
			AND UM.AOMMaterialID = DSR.MaterialID
		
		--Identifying the OPCOReportingInd AND AOMReportingInd for Unmatched MaterialIds
		INSERT INTO #ScanRetailReporting
		(
		     Submitter   			
			,RetailStoreGUID     			
			,WeekEndDate	  			
			,ChainSubmittedDate  		
			,CategoryCompanyObjectGUID 	
			,OPCOReportingInd     		
			,AOMReportingInd 		
		) 
		SELECT   DSR.Submitter   			
				,DSR.RetailStoreGUID    			
				,DSR.WeekEndDate	  			
				,DSR.ChainSubmittedDate  		
				,UM.CategoryCompanyObjectGUID 	
				,CAST(MAX(CAST((CASE WHEN UM.OPCOMaterialID = DSR.MaterialID THEN 1 ELSE 0 END) AS INT)) AS BIT) AS  OPCOReportingInd     		
				,CAST(MAX(CAST((CASE WHEN UM.AOMMaterialID = DSR.MaterialID THEN 1 ELSE 0 END) AS INT)) AS BIT)  AS AOMReportingInd 
		FROM #ScanReporting DSR
		INNER JOIN #UnmatchedMaterialID UM
			ON ((UM.OPCOMaterialID = DSR.MaterialID) OR (UM.AOMMaterialID = DSR.MaterialID))
		LEFT JOIN #ScanRetailReporting SP
		ON DSR.RetailStoreGUID  = SP.RetailStoreGUID
			AND DSR.WeekEndDate = SP.WeekEndDate
			AND DSR.ChainSubmittedDate = SP.ChainSubmittedDate			
			AND UM.CategoryCompanyObjectGUID = SP.CategoryCompanyObjectGUID
		WHERE SP.RetailStoreGUID IS NULL 
			AND SP.WeekEndDate IS NULL 
			AND SP.ChainSubmittedDate IS NULL
			AND SP.CategoryCompanyObjectGUID IS NULL
		GROUP BY DSR.Submitter   			
				,DSR.RetailStoreGUID    			
				,DSR.WeekEndDate	  			
				,DSR.ChainSubmittedDate  		
				,UM.CategoryCompanyObjectGUID 		 	 
		
		--Populating in SEP_ScanData_RetailReporting
		--Ignore records already present in SEP_ScanData_RetailReporting at Submitter, RetailStore, WeekEndDate and ChainSubmittedDate level
		INSERT INTO dbo.SEP_ScanData_RetailReporting
		(
				 Submitter   			
				,RetailStoreGUID  
				,WeekEndDate
				,ChainSubmittedDate
				,CategoryCompanyObjectGUID
				,OPCOReportingInd
				,AOMReportingInd
				,CreateDate
				,CreateUser
				,UpdateDate
				,UpdateUser
				,Active		
		)
		SELECT      
				 SP.Submitter   			
				,SP.RetailStoreGUID  
				,SP.WeekEndDate
				,SP.ChainSubmittedDate
				,SP.CategoryCompanyObjectGUID
				,SP.OPCOReportingInd
			    ,SP.AOMReportingInd
				,GETDATE()		AS CreateDate
				,@l_UpdateUser	AS CreateUser
				,GETDATE()		AS UpdateDate
				,@l_UpdateUser	AS UpdateUser
				,1 AS Active
		FROM #ScanRetailReporting SP
		LEFT JOIN DBO.SEP_ScanData_RetailReporting SDR	WITH (NOLOCK)		
			ON SP.Submitter = SDR.Submitter  			
				AND SP.RetailStoreGUID  = SDR.RetailStoreGUID
				AND SP.WeekEndDate = SDR.WeekEndDate
				AND SP.ChainSubmittedDate   = SDR.ChainSubmittedDate
				AND SDR.Active = 1			
		WHERE  SDR.Submitter IS NULL 
			AND SDR.RetailStoreGUID IS NULL 
			AND SDR.WeekEndDate IS NULL 
			AND SDR.ChainSubmittedDate IS NULL 
			
		
		SET @ROWCNT1= @@ROWCOUNT;
			
		IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
		IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END

		IF EXISTS (SELECT * FROM #tmpErrors) ROLLBACK TRANSACTION
		IF @@TRANCOUNT>0 BEGIN
		PRINT 'The database update succeeded with count of rows: ' 
		PRINT 'SEP_ScanData_SubmitterReporting:'+ @ROWCNT1
		
		COMMIT TRANSACTION
		END
		ELSE PRINT 'The database update failed'
		
		--Droping Temp tables
		IF OBJECT_ID('tempdb..#tmpErrors') IS NOT NULL
              DROP TABLE #tmpErrors
        IF OBJECT_ID('tempdb..#UnmatchedMaterialID') IS NOT NULL	
			DROP TABLE #UnmatchedMaterialID
		IF OBJECT_ID('tempdb..#MSAMaterialID') IS NOT NULL	
			DROP TABLE #MSAMaterialID
		IF OBJECT_ID('tempdb..#ScanReporting') IS NOT NULL	
			DROP TABLE #ScanReporting
		IF OBJECT_ID('tempdb..#ScanRetailReporting') IS NOT NULL	
			DROP TABLE #ScanRetailReporting     
SET NOCOUNT OFF
  
END	
