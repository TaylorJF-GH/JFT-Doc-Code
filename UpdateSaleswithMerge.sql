/*
SCENARIO:
Updates to the Sales data is required.
An update table is provided.
Any MATCHED records with be updated.
Any UNMATCHED records that are in the SOURCE table but not the TARGET table will be INSERTED.
Any UNMATCHED records that are in the TARGET table but not in the SOURCE table will be DELETED.
*/

BEGIN TRANSACTION;

BEGIN TRY
    -- Set MERGE based on Store, Product, SalesType and SalesDate
    MERGE Sales AS Target
    USING SalesUpdates AS Source
    ON Target.StoreID = Source.StoreID
       AND Target.ProductID = Source.ProductID
       AND Target.SalesTypeID = Source.SalesTypeID
       AND Target.SaleDate = Source.SaleDate

    -- Update matching records
    WHEN MATCHED THEN
        UPDATE SET 
            Target.Amount    = Source.Amount,
            Target.Cost = Source.Cost

    -- Insert new records if there is no match in target table
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (SaleID, StoreID, ProductID, SalesTypeID, SaleDate, Amount, SalesType)
        VALUES (Source.SaleID, Source.StoreID, Source.ProductID, Source.SalesTypeID, Source.SaleDate, Source.Amount, Source.SalesType)

    -- Delete records in target that are not present in source
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE

    -- Log changes (Insert, Update, Delete)
    OUTPUT 
        $action AS ActionTaken,
        COALESCE(inserted.SaleID, deleted.SaleID) AS SaleID,
        COALESCE(inserted.StoreID, deleted.StoreID) AS StoreID,
        COALESCE(inserted.ProductID, deleted.ProductID) AS ProductID,
        COALESCE(inserted.SalesTypeID, deleted.SalesTypeID) AS SalesTypeID,
        COALESCE(inserted.SaleDate, deleted.SaleDate) AS SaleDate,
        COALESCE(inserted.Amount, deleted.Amount) AS Amount,
        COALESCE(inserted.SalesType, deleted.SalesType) AS SalesType
    INTO SalesMergeLog (ActionTaken, SaleID, StoreID, ProductID, SalesTypeID, SaleDate, Amount, Cost);

    -- Commit the transaction if no errors encoutered
    COMMIT TRANSACTION;
    PRINT 'MERGE operation completed successfully.';

END TRY
BEGIN CATCH
    -- Rollback the transaction an error has been encountered
    ROLLBACK TRANSACTION;
    PRINT 'Error during MERGE operation:';
    PRINT ERROR_MESSAGE();
END CATCH;
