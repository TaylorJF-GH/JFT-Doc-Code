/*
Example of use of CTE:
Get the latest Sales by SalesType by Customer
*/

WITH LatestSales AS (
    SELECT 
        CustomerID,
        SaleDate,
        Revenue,
		SalesTypeID
        ROW_NUMBER() OVER (PARTITION BY CustomerID, SalesTypeID ORDER BY SaleDate DESC) AS rn
    FROM WeeklySales
)
SELECT 
    CustomerID,
    SaleDate AS LatestSaleDate,
    Amount AS LatestSaleAmount
	CASE 
		WHEN SalesTypeID = 0 THEN 'Regular'
		WHEN SalesTypeID = 1 THEN 'Promotion'
		WHEN SalesTypeID = 2 THEN 'Online'
		WHEN SalesTypeID = 2 THEN 'Regular Return'
		WHEN SalesTypeID = 3 THEN 'Promotion Return'
		WHEN SalesTypeID = 3 THEN 'Online Return'
		ELSE 'Invalid Sales Type'
	END AS Sales_Type
FROM WeeklySales
WHERE rn = 1;
