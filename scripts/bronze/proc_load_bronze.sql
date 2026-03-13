/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    Loads data into the 'bronze' schema from:
    - Structured CSV files (CRM & ERP)
    - Semi-structured JSON files (Web data)
    Logs every table load into bronze.load_log.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @rows INT,
        @table_name NVARCHAR(200),
        @proc_name NVARCHAR(200) = 'bronze.load_bronze';

    PRINT '======================================';
    PRINT 'Starting Bronze Layer Load';
    PRINT 'Procedure: bronze.load_bronze';
    PRINT 'Start Time: ' + CAST(GETDATE() AS NVARCHAR(50));
    PRINT '======================================';

    BEGIN TRY

        /* =======================================================
           Enable OPENROWSET if not already enabled
        ======================================================== */
        IF NOT EXISTS (
            SELECT 1
            FROM sys.configurations
            WHERE name = 'Ad Hoc Distributed Queries'
            AND value_in_use = 1
        )
        BEGIN
            PRINT '>> Enabling Ad Hoc Distributed Queries (OPENROWSET)...';

            EXEC sp_configure 'show advanced options', 1;
            RECONFIGURE;

            EXEC sp_configure 'Ad Hoc Distributed Queries', 1;
            RECONFIGURE;

            PRINT '>> OPENROWSET Enabled Successfully';
        END
        ELSE
        BEGIN
            PRINT '>> OPENROWSET Already Enabled';
        END;

        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        /* =======================================================
           CRM TABLES
        ======================================================== */
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- ------------------- bronze.crm_cust_info -------------------
        SET @table_name = 'bronze.crm_cust_info';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @table_name;
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: ' + @table_name;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\SQLDATA\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK,
            CODEPAGE = '65001'
        );

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Inserted Rows: ' + CAST(@rows AS NVARCHAR(20));
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        INSERT INTO bronze.load_log
        VALUES
        (@proc_name,@table_name,@start_time,@end_time,
        DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ------------------- bronze.crm_prd_info -------------------
        SET @table_name = 'bronze.crm_prd_info';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @table_name;
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: ' + @table_name;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\SQLDATA\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK,
            CODEPAGE = '65001'
        );

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Inserted Rows: ' + CAST(@rows AS NVARCHAR(20));
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        INSERT INTO bronze.load_log
        VALUES
        (@proc_name,@table_name,@start_time,@end_time,
        DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ------------------- bronze.crm_sales_details -------------------
        SET @table_name = 'bronze.crm_sales_details';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @table_name;
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: ' + @table_name;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\SQLDATA\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK,
            CODEPAGE = '65001'
        );

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Inserted Rows: ' + CAST(@rows AS NVARCHAR(20));
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        INSERT INTO bronze.load_log
        VALUES
        (@proc_name,@table_name,@start_time,@end_time,
        DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        /* =======================================================
           ERP TABLES
        ======================================================== */
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- ------------------- bronze.erp_loc_a101 -------------------
        SET @table_name = 'bronze.erp_loc_a101';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @table_name;
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: ' + @table_name;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\SQLDATA\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK,
            CODEPAGE = '65001'
        );

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Inserted Rows: ' + CAST(@rows AS NVARCHAR(20));
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        INSERT INTO bronze.load_log
        VALUES
        (@proc_name,@table_name,@start_time,@end_time,
        DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

         -- ------------------- bronze.erp_cust_az12 -------------------
        SET @table_name = 'bronze.erp_cust_az12';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @table_name;
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: ' + @table_name;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\SQLDATA\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK,
            CODEPAGE = '65001'
        );

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Inserted Rows: ' + CAST(@rows AS NVARCHAR(20));
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        INSERT INTO bronze.load_log
        VALUES
        (@proc_name,@table_name,@start_time,@end_time,
        DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ------------------- bronze.erp_px_cat_g1v2 -------------------
        SET @table_name = 'bronze.erp_px_cat_g1v2';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @table_name;
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: ' + @table_name;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\SQLDATA\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK,
            CODEPAGE = '65001'
        );

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Inserted Rows: ' + CAST(@rows AS NVARCHAR(20));
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        INSERT INTO bronze.load_log
        VALUES
        (@proc_name,@table_name,@start_time,@end_time,
        DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        /* =======================================================
           WEB JSON TABLES
        ======================================================== */
        PRINT '------------------------------------------------';
        PRINT 'Loading Web JSON Tables';
        PRINT '------------------------------------------------';

        -- ------------------- web_sessions -------------------
        SET @table_name = 'bronze.web_sessions';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @table_name;
        TRUNCATE TABLE bronze.web_sessions;

        PRINT '>> Inserting Data Into: ' + @table_name;
        INSERT INTO bronze.web_sessions
        SELECT *
        FROM OPENJSON(
            (SELECT BulkColumn
             FROM OPENROWSET(
                 BULK 'C:\SQLDATA\datasets\source_web\web_sessions.json',
                 SINGLE_CLOB
             ) AS j)
        )
        WITH (
            session_id NVARCHAR(50),
            customer_id INT,
            session_start DATETIME,
            session_end DATETIME,
            device_type NVARCHAR(50),
            traffic_source NVARCHAR(50),
            country NVARCHAR(50)
        );

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Inserted Rows: ' + CAST(@rows AS NVARCHAR(20));
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        INSERT INTO bronze.load_log
        VALUES
        (@proc_name,@table_name,@start_time,@end_time,
        DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ------------------- web_events -------------------
        SET @table_name = 'bronze.web_events';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @table_name;
        TRUNCATE TABLE bronze.web_events;

        PRINT '>> Inserting Data Into: ' + @table_name;
        INSERT INTO bronze.web_events
        SELECT
            TRY_CAST(event_id AS INT),
            session_id,
            TRY_CAST(customer_id AS INT),
            event_type,
            product_key,
            TRY_CAST(event_timestamp AS DATETIME)
        FROM OPENJSON(
            (SELECT BulkColumn
             FROM OPENROWSET(
                 BULK 'C:\SQLDATA\datasets\source_web\web_events.json',
                 SINGLE_CLOB
             ) AS j)
        )
        WITH (
            event_id NVARCHAR(50),
            session_id NVARCHAR(50),
            customer_id NVARCHAR(50),
            event_type NVARCHAR(50),
            product_key NVARCHAR(50),
            event_timestamp NVARCHAR(50)
        );

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Inserted Rows: ' + CAST(@rows AS NVARCHAR(20));
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        INSERT INTO bronze.load_log
        VALUES
        (@proc_name,@table_name,@start_time,@end_time,
        DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ------------------- web_conversions -------------------
        SET @table_name = 'bronze.web_conversions';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @table_name;
        TRUNCATE TABLE bronze.web_conversions;

        PRINT '>> Inserting Data Into: ' + @table_name;
        INSERT INTO bronze.web_conversions
        SELECT
            TRY_CAST(conversion_id AS INT),
            session_id,
            order_number,
            TRY_CAST(conversion_timestamp AS DATETIME),
            TRY_CAST(revenue AS DECIMAL(18,2))
        FROM OPENJSON(
            (SELECT BulkColumn
             FROM OPENROWSET(
                 BULK 'C:\SQLDATA\datasets\source_web\web_conversions.json',
                 SINGLE_CLOB
             ) AS j)
        )
        WITH (
            conversion_id NVARCHAR(50),
            session_id NVARCHAR(50),
            order_number NVARCHAR(50),
            conversion_timestamp NVARCHAR(50),
            revenue NVARCHAR(50)
        );

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Inserted Rows: ' + CAST(@rows AS NVARCHAR(20));
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        INSERT INTO bronze.load_log
        VALUES
        (@proc_name,@table_name,@start_time,@end_time,
        DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        PRINT '==========================================';
        PRINT 'Loading Bronze Layer Completed Successfully';
        PRINT 'End Time: ' + CAST(GETDATE() AS NVARCHAR(50));
        PRINT '==========================================';

    END TRY

    BEGIN CATCH
        PRINT 'ERROR OCCURRED';
        PRINT ERROR_MESSAGE();

        INSERT INTO bronze.load_log
        VALUES
        (
            @proc_name,
            @table_name,
            @start_time,
            GETDATE(),
            NULL,
            NULL,
            'FAILED',
            ERROR_MESSAGE()
        );
    END CATCH
END

EXEC bronze.load_bronze;
