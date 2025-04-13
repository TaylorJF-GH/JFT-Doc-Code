/*
SCENARIO:
Client has identified there are product identifiers that require updating for business reasons.  
They have provided a file with the old product code and new product code.
They require Sales data be updated with the new codes
The Sales data uses a product identifier instead of the product code.  
In the Daily and Weekly Sales tables, not only is there an index but the tables are partitioned by Weekend date.

SOLUTION:
Add two new columns to the data supplied.  Old and new product identifier.
Update the data supplied with product identifier data.
Update all production identifiers matching the old production identifier with new production identifier in Sales Transaction data.
As there are partions and indices on the Sales tables it is faster to rebuild these tables.
Truncate and re-aggregate Daily and Weekly Sales tables.
*/

CREATE OR REPLACE PROCEDURE `production-environ.Client_DataSet_StagingArea.ProductIdUpdate`(workflow_name STRING)
BEGIN

---- UPDATE ProductCodeUpdates table with Previous ProductIds
  UPDATE `production-environ.Client_DataSet_StagingArea.ProductCodeUpdates` pcu
  SET pcu.PreviousProductId = cpd.ProductId
  FROM `production-environ.Client_DataSet_Production.Client_ProductDetails` cpd
  WHERE pcu.PreviousSAPCode = cpd.UpcCode;

---- UPDATE ProductCodeUpdates table with New ProductIds
  UPDATE `production-environ.Client_DataSet_StagingArea.ProductCodeUpdates` pcu
  SET pcu.NewProductId = cpd.ProductId
  FROM `production-environ.Client_DataSet_Production.Client_Products` cpd
  WHERE pcu.NewSAPCode = cpd.UpcCode;

---- UPDATE Client_TransLog with NEW ProductIds

  UPDATE `production-environ.Client_DataSet_Production.Client_TransLog` ctl
  SET ctl.ProductId = pcu.NewProductId
  FROM `production-environ.Client_DataSet_StagingArea.ProductCodeUpdates` pcu
  WHERE ctl.ProductId = pcu.PreviousProductId;
  
---- Backup Daily Sales Table

CREATE TEMP TABLE `production-environ.Client_DataSet_StagingArea.DailySales_backup`
PARTITION BY DATE(SalesDate)
CLUSTER BY StoreID,ProductID
AS
SELECT * FROM `production-environ.Client_DataSet_Production.Client_DailySalesAggr`;

---- TRUNCATE Client_DailySalesAggr

  TRUNCATE TABLE `production-environ.Client_DataSet_Production.Client_DailySalesAggr`;

---- Re-create DailySalesAggr by aggregating updated TLog

  INSERT INTO `production-environ.Client_DataSet_Production.Client_DailySalesAggr`
  SELECT
    ctl.StoreID
    ,ctl.ProductID
    ,ctl.SalesTypeId
    ,ctl.TransactionDate                        AS SalesDate
    ,CAST(SUM(ctl.Quantity) AS NUMERIC)         AS Quantity
    ,CAST(SUM(ctl.SalesRevenue) AS NUMERIC)     AS Revenue
    ,CAST(SUM(ctl.Cost) AS NUMERIC)             AS TotalCost
    ,CAST(SUM(ctl.RegularRevenue) AS NUMERIC)   AS RegularRevenue
    ,COUNT(ctl.TransactionDate)                 AS TransactionCount
    ,CAST(SUM(ctl.VATRevenueAmount) AS NUMERIC) AS VATRevenueAmount
    ,CAST(SUM(ctl.WasRevenue) AS NUMERIC)       AS WasRevenue
  FROM `production-environ.Client_DataSet_Production.Client_TransLog` ctl
  GROUP BY
    ctl.StoreID
    ,ctl.ProductID
    ,ctl.SalesTypeId
    ,ctl.TransactionDate;

---- Backup Weekly Sales Table

CREATE TEMP TABLE `production-environ.Client_DataSet_StagingArea.WeeklySales_backup`
PARTITION BY DATE(SalesDate)
CLUSTER BY StoreID,ProductID
AS
SELECT * FROM `production-environ.Client_DataSet_Production.Client_SalesAggr`;

---- TRUNCATE Client_SalesAggr

  TRUNCATE TABLE `production-environ.Client_DataSet_Production.Client_SalesAggr`;

---- Re-create SalesAggr by aggregating DailySalesAggr

  INSERT INTO `production-environ.Client_DataSet_Production.Client_SalesAggr`
  SELECT
    dsa.StoreID
    ,dsa.ProductID
    ,dsa.SalesTypeId
    ,wd.SalesWeekEndDate                       AS SalesDate
    ,SUM(dsa.Quantity)         AS Quantity
    ,SUM(dsa.Revenue)     AS Revenue
    ,SUM(dsa.TotalCost)             AS TotalCost
    ,SUM(dsa.RegularRevenue)   AS RegularRevenue
    ,COUNT(dsa.TransactionCount)                 AS TransactionCount
    ,SUM(dsa.VATRevenueAmount) AS VATRevenueAmount
    ,SUM(dsa.WasRevenue)       AS WasRevenue
  FROM `production-environ.Client_DataSet_Production.Client_DailySalesAggr` dsa
  JOIN `production-environ.Client_DataSet_StagingArea.Client_WeekendDates` wd
  ON dsa.SalesDate = CAST(cd.DateValue AS DATETIME)
  GROUP BY
    dsa.StoreID
    ,dsa.ProductID
    ,dsa.SalesTypeId
    ,wd.SalesWeekEndDate;

END;


/*
If there was a failure during the rebuild of either the Daily or Weekly Sales the tables can be restored with the following
*/

---- Restore Daily Sales
CREATE OR REPLACE TABLE `production-environ.Client_DataSet_Production.Client_DailySalesAggr`;
PARTITION BY DATE(sale_date)
CLUSTER BY StoreID, ProductID
AS
SELECT * FROM `production-environ.Client_DataSet_Production.DailySales_backup`;

---- Restore Weekly Sales
CREATE OR REPLACE TABLE `production-environ.Client_DataSet_Production.Client_SalesAggr`
PARTITION BY DATE(sale_date)
CLUSTER BY StoreID, ProductID
AS
SELECT * FROM `production-environ.Client_DataSet_Production.WeeklySales_backup`;
