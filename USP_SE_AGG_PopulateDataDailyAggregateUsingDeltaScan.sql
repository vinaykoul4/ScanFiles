USE [SEPDB_SCAN_DATA]
GO
/****** Object:  StoredProcedure [dbo].[USP_SE_AGG_PopulateDataDailyAggregateUsingDeltaScan]    Script Date: 1/27/2017 11:52:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/*  
=============================================================================================================================================             
 OBJECT       : STORED PROCEDURE  [dbo].[USP_SE_AGG_PopulateDataDailyAggregateUsingDeltaScan]  
-=============================================================================================================================================               
*********************************************************************        
**   Name         : EXEC USP_SE_AGG_PopulateDataDailyAggregateUsingDeltaScan
**   Desc         : Process to aggregate the delta records created after last successful
					run date from Scan Data to SEP_AGG_ScanDataDailyAggregate table 
					and SEP_ScanData_Reporting table.
**   AUTHOR       : iGATE     
**   CREATED DATE : 2015-10-08      
********************************************************************************************************************************************         
**              CHANGE HISTORY          
*********************************************************************************************************************************************         
** Date:            Author:                  Description:
   02/08/2017       Capgemini				 Added logic to populate aggregate table from SEP_ScanData_Delta table and populate SEP_ScanData_RetailReporting 
                                             table from SEP_ScanData_Delta and SEP_ScanData_Unmatched_Delta tables  
   02/14/2017       Capgemini                Reporting table SEP_ScanData_SubmitterReporting will be populated based on distinct transactions reported in SEP_ScanData_Delta 
											 and SEP_ScanData_Unmatched_Delta tables 												 
** *******************************************************************************************************************************************/
-- EXEC DBO.USP_SE_AGG_PopulateDataDailyAggregateUsingDeltaScan 'SE_BT_RTL_PopulateDailyAggregateDataFromScanData','2015-01-01','2015-10-01'
ALTER PROCEDURE [dbo].[USP_SE_AGG_PopulateDataDailyAggregateUsingDeltaScan]
(	
	@InterfaceName	NVARCHAR(100)
	,@LastLoadDate	NVARCHAR(25)
	,@MaxLoadDate	NVARCHAR(25)
)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.                       
	SET NOCOUNT ON;

	BEGIN TRY

		--Local Variables
		DECLARE @l_LastLoadDate					DATETIME 
				,@l_MaxLoadDate					DATETIME 
				,@l_UpdateUser	                NVARCHAR(20)
				,@ConfigUnmatchedMaterialIDXML  XML
		
		SET @l_MaxLoadDate	= CONVERT(DATETIME,@MaxLoadDate,101)		
		SET @l_LastLoadDate = CONVERT(DATETIME,@LastLoadDate,101)
		SET @l_UpdateUser	= SUSER_SNAME()
		
		
		--Droping Temp tables
		IF OBJECT_ID('tempdb..#DeltaScanData') IS NOT NULL
			DROP TABLE #DeltaScanData
		IF OBJECT_ID('tempdb..#DeltaUnmatchedScanData') IS NOT NULL
			DROP TABLE #DeltaUnmatchedScanData
		IF OBJECT_ID('tempdb..#DeltaScanReporting') IS NOT NULL
			DROP TABLE #DeltaScanReporting
		IF OBJECT_ID('tempdb..#UnmatchedMaterialID') IS NOT NULL	
			DROP TABLE #UnmatchedMaterialID
		IF OBJECT_ID('tempdb..#MSAMaterialID') IS NOT NULL	
			DROP TABLE #MSAMaterialID
		IF OBJECT_ID('tempdb..#ScanRetailReporting') IS NOT NULL	
			DROP TABLE #ScanRetailReporting
	
		--CREATE Temp Tables  
		CREATE TABLE #DeltaScanData
		(
			Submitter						INTEGER
			--,ProvidedChainAccountNumber		NVARCHAR(20)
			,ChainSubmittedDate				DATETIME
			--,IsRestatement					BIT
			,AccountNumber					NVARCHAR(20)
			,RetailStoreGUID				UNIQUEIDENTIFIER
			,TransactionID					NVARCHAR(50)
			,MaterialID						INTEGER
			,WeekEndDate					DATETIME
			,TransactionDate				DATETIME
			,UOM							NVARCHAR(15)							
			,IsLowestSellable				BIT
			,IsProductPromo					BIT			
			,IsMultiPack					BIT
			,MultipackCount					INTEGER
			,MultiPackDiscountAmount		DECIMAL(11,3)
			,AccountFundedDiscountName		NVARCHAR(100)
			,AccountFundedDiscountAmount	DECIMAL(11,3)
			,MFGDealName1					NVARCHAR(100)
			,MFGFundedDiscount1				DECIMAL(11,3)
			,MFGDealName2					NVARCHAR(100)
			,MFGFundedDiscount2				DECIMAL(11,3)
			,MFGDealName3					NVARCHAR(100)
			,MFGFundedDiscount3				DECIMAL(11,3)
			,SalesQtyReported				INTEGER
			,SalesQtyAdjusted				INTEGER
			,SalesAmount					DECIMAL(11,3)
			,UnitPrice						DECIMAL(11,3)			
			,TransactionType				NVARCHAR(1)
		)
		
		-- Code changes : START	02/07/2017
		CREATE TABLE #DeltaUnmatchedScanData
		(
			Submitter   					INTEGER
			,RetailStoreGUID				UNIQUEIDENTIFIER
			--,ProvidedChainAccountNumber		NVARCHAR(20)
			,WeekEndDate					DATETIME
			,ChainSubmittedDate				DATETIME
			,MaterialID				 	    INTEGER
			--,IsRestatement					BIT						--20/02/2016 
		)		
		
		CREATE TABLE #DeltaScanReporting						
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
         
		--identify the CategoryCompanyObject from CompanyObjectCode
		UPDATE SUB														
		SET SUB.CategoryCompanyObjectGUID = CO.CompanyObjectGUID
		FROM #UnmatchedMaterialID SUB								
		INNER JOIN DBO.OLTP_SEP_CompanyObject CO
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

		-- Code changes : END	02/07/2017
		
		----get all the delta records from ScanData table and Load it to ScanDataDailyAggregate and ScanDataReporting tables in SCAN_DATA
		INSERT INTO #DeltaScanData
		(
				Submitter
				--,ProvidedChainAccountNumber
				,ChainSubmittedDate
				--,IsRestatement
				,AccountNumber
				,RetailStoreGUID
				,TransactionID
				,MaterialID
				,WeekEndDate
				,TransactionDate
				,UOM
				,IsLowestSellable
				,IsProductPromo
				,IsMultiPack
				,MultipackCount
				,MultiPackDiscountAmount
				,AccountFundedDiscountName
				,AccountFundedDiscountAmount
				,MFGDealName1
				,MFGFundedDiscount1
				,MFGDealName2
				,MFGFundedDiscount2
				,MFGDealName3
				,MFGFundedDiscount3
				,SalesQtyReported
				,SalesQtyAdjusted
				,SalesAmount
				,UnitPrice
				,TransactionType
		)
		SELECT	SD.Submitter
				--,SD.ProvidedChainAccountNumber
				,SD.ChainSubmittedDate
				--,SD.IsRestatement
				,SD.AccountNumber
				,SD.RetailStoreGUID
				,SD.TransactionID
				,SD.MaterialID
				,SD.WeekEndDate
				,CONVERT(DATE,SD.TransactionDate) AS TransactionDate --To consider as date part
				,SD.UOM
				,SD.IsLowestSellable
				,SD.IsProductPromo
				,SD.IsMultiPack
				,SD.MultiPackCount
				,SUM(SD.MultiPackDiscountAmount) AS MultiPackDiscountAmount
				,SD.AccountFundedDiscountName
				,SUM(SD.AccountFundedDiscountAmount) AS AccountFundedDiscountAmount
				,SD.MFGDealName1
				,SUM(SD.MFGFundedDiscount1) AS MFGFundedDiscount1
				,SD.MFGDealName2
				,SUM(SD.MFGFundedDiscount2) AS MFGFundedDiscount2
				,SD.MFGDealName3
				,SUM(SD.MFGFundedDiscount3) AS MFGFundedDiscount3
				,SUM(SD.SalesQtyReported) AS SalesQtyReported
				,SUM(SD.SalesQtyAdjusted) AS SalesQtyAdjusted
				,SUM(SD.SalesAmount) AS SalesAmount
				,CASE WHEN SUM(SD.SalesQtyAdjusted) <> 0 THEN SUM(SD.SalesAmount)/SUM(SD.SalesQtyAdjusted) ELSE 0 END UnitPrice
				,CASE WHEN SUM(SD.SalesQtyAdjusted) < 0 THEN 'R' ELSE 'P' END AS TransactionType				
		FROM DBO.SEP_ScanData_Delta SD WITH (NOLOCK)							--Referring SEP_ScanData_Delta table    01/31/2017
		WHERE SD.Active=1
			AND SD.CreateDate BETWEEN @l_LastLoadDate AND @l_MaxLoadDate			--Condition Added to fetch delta records 02/07/2017
		GROUP BY SD.TransactionID
				,SD.Submitter
				--,SD.ProvidedChainAccountNumber
				,SD.ChainSubmittedDate
				--,SD.IsRestatement
				,SD.AccountNumber
				,SD.RetailStoreGUID
				,SD.MaterialID
				,SD.WeekEndDate
				,SD.TransactionDate
				,SD.UOM
				,SD.IsLowestSellable
				,SD.IsProductPromo
				,SD.IsMultiPack
				,SD.MultiPackCount
				,SD.AccountFundedDiscountName
				,SD.MFGDealName1
				,SD.MFGDealName2
				,SD.MFGDealName3

		--Insert the Aggregated records into #ScanDataDailyAggregate table
		INSERT INTO DBO.SEP_AGG_ScanDataDailyAggregate 
		(
				AccountNumber
				,RetailStoreGUID
				,TransactionDate
				,MaterialID
				,UOM
				,TransactionType
				,IsLowestSellable
				,IsProductPromo
				,IsMultiPack
				,UnitPrice
				,SalesQtyReported
				,SalesQtyAdjusted
				,SalesAmount
				,TransactionCount
				,MultiPackCount
				,MultiPackDiscountAmount
				,AccountFundedDiscountName
				,AccountFundedDiscountAmount
				,MFGDealName1
				,MFGFundedDiscount1
				,MFGDealName2
				,MFGFundedDiscount2
				,MFGDealName3
				,MFGFundedDiscount3
				,WeekEndDate
				,ChainSubmittedDate
				,CreateDate
				,CreateUser
				,UpdateDate
				,UpdateUser
				,Active
		)
		SELECT	SD.AccountNumber
				,SD.RetailStoreGUID
				,SD.TransactionDate
				,SD.MaterialID
				,SD.UOM
				,SD.TransactionType
				,SD.IsLowestSellable
				,SD.IsProductPromo
				,SD.IsMultiPack
				,SD.UnitPrice
				,SUM(SD.SalesQtyReported) AS SalesQtyReported
				,SUM(SD.SalesQtyAdjusted) AS SalesQtyAdjusted
				,SUM(SD.SalesAmount) AS SalesAmount
				,COUNT(1) AS TransactionCount
				,SD.MultipackCount
				,SUM(SD.MultiPackDiscountAmount)
				,SD.AccountFundedDiscountName
				,SUM(SD.AccountFundedDiscountAmount)
				,SD.MFGDealName1
				,SUM(SD.MFGFundedDiscount1) AS MFGFundedDiscount1
				,SD.MFGDealName2
				,SUM(SD.MFGFundedDiscount2) AS MFGFundedDiscount2
				,SD.MFGDealName3
				,SUM(SD.MFGFundedDiscount3) AS MFGFundedDiscount3
				,WeekEndDate
				,ChainSubmittedDate
				,GETDATE()		AS CreateDate
				,@l_UpdateUser	AS CreateUser
				,GETDATE()		AS UpdateDate
				,@l_UpdateUser	AS UpdateUser
				,1 AS Active
		FROM #DeltaScanData SD
		GROUP BY SD.AccountNumber
				,SD.RetailStoreGUID
				,SD.TransactionDate
				,SD.MaterialID
				,SD.UOM
				,SD.TransactionType
				,SD.IsLowestSellable
				,SD.IsProductPromo
				,SD.IsMultiPack
				,SD.UnitPrice
				,SD.MultipackCount
				,SD.AccountFundedDiscountName
				,SD.MFGDealName1
				,SD.MFGDealName2
				,SD.MFGDealName3
				,WeekEndDate
				,ChainSubmittedDate

		
		-- Code changes : START	02/07/2017
		--Get all new records from Unmatched delta 
		INSERT INTO #DeltaUnmatchedScanData
		(
			Submitter
			,RetailStoreGUID
			--,ProvidedChainAccountNumber
			,WeekEndDate
			,ChainSubmittedDate
			,MaterialID
			--,IsRestatement															--20/02/2016 
		)
		SELECT DISTINCT 
		        Submitter
				,RetailStoreGUID
				--,ProvidedChainAccountNumber
				,WeekEndDate
				,ChainSubmittedDate
				,MaterialID
				--,IsRestatement														--20/02/2016 
		FROM dbo.SEP_ScanData_Unmatched_Delta WITH (NOLOCK)
		WHERE CreateDate BETWEEN @l_LastLoadDate AND @l_MaxLoadDate			
			AND Active=1 
			--AND RetailStoreGUID IS NOT NULL  
		
		--Insert the new records into DBO.SEP_ScanData_SubmitterReporting table with distinct records from SEP_ScanData_Delta and SEP_ScanData_Unmatched_Delta	
		INSERT INTO DBO.SEP_ScanData_SubmitterReporting
		(
				Submitter
				--,ProvidedAccountNumber
				,WeekEndDate
				,ChainSubmittedDate
				--,IsRestatement															--20/02/2016 
				,CreateDate
				,CreateUser
				,UpdateDate
				,UpdateUser
				,Active
		)
		SELECT DISTINCT 
				DS.Submitter
				--,ISNULL(NULLIF(LTRIM(RTRIM(ProvidedChainAccountNumber)),''),AccountNumber) AS ProvidedChainAccountNumber
				,DS.WeekEndDate
				,DS.ChainSubmittedDate
				--,IsRestatement															--20/02/2016 
				,GETDATE()		AS CreateDate
				,@l_UpdateUser	AS CreateUser
				,GETDATE()		AS UpdateDate
				,@l_UpdateUser	AS UpdateUser
				,1 AS Active
		FROM #DeltaScanData DS
		LEFT JOIN DBO.SEP_ScanData_SubmitterReporting  SS WITH (NOLOCK)	
			ON DS.Submitter = SS.Submitter
			AND DS.WeekEndDate = SS.WeekEndDate
			AND DS.ChainSubmittedDate = SS.ChainSubmittedDate
			AND SS.Active = 1
		WHERE SS.Submitter IS NULL 
			AND SS.WeekEndDate IS NULL
			AND SS.ChainSubmittedDate IS NULL

		INSERT INTO DBO.SEP_ScanData_SubmitterReporting
		(
				Submitter
				--,ProvidedAccountNumber
				,WeekEndDate
				,ChainSubmittedDate
				--,IsRestatement															--20/02/2016 
				,CreateDate
				,CreateUser
				,UpdateDate
				,UpdateUser
				,Active
		)
		SELECT DISTINCT 
				DU.Submitter
				--,ProvidedChainAccountNumber
				,DU.WeekEndDate
				,DU.ChainSubmittedDate
				--,IsRestatement															--20/02/2016 
				,GETDATE()		AS CreateDate
				,@l_UpdateUser	AS CreateUser
				,GETDATE()		AS UpdateDate
				,@l_UpdateUser	AS UpdateUser
				,1 AS Active
		FROM #DeltaUnmatchedScanData DU
		LEFT JOIN DBO.SEP_ScanData_SubmitterReporting  SS WITH (NOLOCK)	
			ON DU.Submitter = SS.Submitter
			AND DU.WeekEndDate = SS.WeekEndDate
			AND DU.ChainSubmittedDate = SS.ChainSubmittedDate
			AND SS.Active = 1
		WHERE SS.Submitter IS NULL 
			AND SS.WeekEndDate IS NULL
			AND SS.ChainSubmittedDate IS NULL

		--Insert records from SEP_ScanData_Unmatched_Delta and SEP_ScanData_Delta tables
		INSERT INTO #DeltaScanReporting
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
		FROM #DeltaUnmatchedScanData
		WHERE RetailStoreGUID IS NOT NULL
		
		--Insert delta scan data
		INSERT INTO #DeltaScanReporting
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
		FROM #DeltaScanData
	
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
		FROM #DeltaScanReporting DSR
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
		INNER JOIN #DeltaScanReporting DSR
		ON  SP.RetailStoreGUID  = DSR.RetailStoreGUID
			AND SP.WeekEndDate = DSR.WeekEndDate
			AND SP.ChainSubmittedDate   = DSR.ChainSubmittedDate	
		INNER JOIN #UnmatchedMaterialID UM
			ON UM.CategoryCompanyObjectGUID = SP.CategoryCompanyObjectGUID
			AND UM.OPCOMaterialID = DSR.MaterialID
		
		UPDATE SP
		SET SP.AOMReportingInd = CASE WHEN UM.AOMMaterialID = DSR.MaterialID THEN 1 ELSE SP.AOMReportingInd  END
		FROM #ScanRetailReporting SP
		INNER JOIN #DeltaScanReporting DSR
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
		FROM #DeltaScanReporting DSR
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
				--AND SP.CategoryCompanyObjectGUID = SDR.CategoryCompanyObjectGUID
		WHERE  SDR.Submitter IS NULL 
			AND SDR.RetailStoreGUID IS NULL 
			AND SDR.WeekEndDate IS NULL 
			AND SDR.ChainSubmittedDate IS NULL 
		--AND SDR.CategoryCompanyObjectGUID IS NULL	

		-- Code changes : END	02/07/2017
		
	END TRY

	BEGIN CATCH
		--Log the exceptions
		INSERT INTO [dbo].[SEP_Interface_Proc_Exception] (
			[InterfaceName]
			,[ProcedureName]
			,[ErrorMessage]
			,[ErrorLine]
			,[ErrorState]
			,[ErrorSeverity]
			,[CreatedUser]
			,[CreatedDate]
			)
		SELECT @InterfaceName,
			ERROR_PROCEDURE()
			,ERROR_MESSAGE()
			,ERROR_LINE()
			,ERROR_STATE()
			,ERROR_SEVERITY()
			,SUSER_SNAME()
			,GETDATE();
	END CATCH

		--Droping Temp tables
		IF OBJECT_ID('tempdb..#DeltaScanData') IS NOT NULL
			DROP TABLE #DeltaScanData
		IF OBJECT_ID('tempdb..#DeltaUnmatchedScanData') IS NOT NULL
			DROP TABLE #DeltaUnmatchedScanData
		IF OBJECT_ID('tempdb..#DeltaScanReporting') IS NOT NULL
			DROP TABLE #DeltaScanReporting
		IF OBJECT_ID('tempdb..#UnmatchedMaterialID') IS NOT NULL	
			DROP TABLE #UnmatchedMaterialID
		IF OBJECT_ID('tempdb..#MSAMaterialID') IS NOT NULL	
			DROP TABLE #MSAMaterialID
		IF OBJECT_ID('tempdb..#ScanRetailReporting') IS NOT NULL	
			DROP TABLE #ScanRetailReporting

	SET NOCOUNT OFF;
END

