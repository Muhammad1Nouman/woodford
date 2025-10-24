DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `ads_adl_by_staff`()
BEGIN
WITH appointment_col AS (
    SELECT  
        i360__Start__c AS Date,
        TRIM(LOWER(Sales_Rep_Name__c)) AS Staff, 
        supportworks__Product_Category_1__c AS prod_cat,
        SUM(supportworks__Issued_Unique__c) AS issued_unique,
        sum(supportworks__Quoted__c) as Quoted
    FROM pre_stage.appointment
    WHERE COALESCE(supportworks__Product_Category_1__c, '') <> 'Service'
      AND COALESCE(i360__Source_Type__c, '') NOT IN ('annual maintenance', 'service opportunity', 'existing database', 'service')
      AND i360__Type__c IN ('New', 'Rehash', 'Reset', 'Radon CRM Pickup')
      and supportworks__Sales_Rep_1_Department__c = "Sales"
    GROUP BY 1,2,3
),
sale_Sold AS (
    SELECT 
        CAST(i360__Sold_On__c AS DATE) AS Date,
        TRIM(LOWER(Sales_Rep_1_Name__c)) AS Staff,
        Prod_Cat_1_Sale_Shrt_List__c AS prod_cat,
        SUM(supportworks__Total_Sales__c) AS sold_total,
        SUM(Sold__c) AS sold,
        SUM(i360__Sold_Price__c) AS Price
    FROM pre_stage.sale
    WHERE supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
      AND supportworks__Department__c = 'Sales'
    GROUP BY 1,2,3
),
sale_cancelled AS (
    SELECT 
        CAST(i360__Canceled_Date__c AS DATE) AS Date,
        TRIM(LOWER(Sales_Rep_1_Name__c)) AS Staff,
        Prod_Cat_1_Sale_Shrt_List__c AS prod_cat,
        SUM(i360__Canceled__c) AS cancelled,
        SUM(i360__Canceled_Amt__c) AS cancelled_amt
    FROM pre_stage.sale
    WHERE supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
      AND supportworks__Department__c = 'Sales'
    GROUP BY 1,2,3
),

combined_results AS (
    SELECT 
        Date AS date, 
        Staff, 
        prod_cat,
        issued_unique, 
        Quoted,
        0 AS total_sold, 
        0 AS total_cancelled, 
        0 AS total_price, 
        0 AS total_sales_amount, 
        0 AS total_cancelled_amount
    FROM appointment_col

    UNION ALL

    SELECT 
        Date AS date, 
        Staff, 
        prod_cat,
        0 AS issued_unique, 
        0 AS Quoted,
        sold AS total_sold, 
        0 AS total_cancelled, 
        Price AS total_price, 
        sold_total AS total_sales_amount, 
        0 AS total_cancelled_amount
    FROM sale_Sold

    UNION ALL

    SELECT 
        Date AS date, 
        Staff, 
        prod_cat,
        0 AS issued_unique, 
        0 As Quoted,
        0 AS total_sold, 
        cancelled AS total_cancelled, 
        0 AS total_price, 
        0 AS total_sales_amount, 
        cancelled_amt AS total_cancelled_amount
    FROM sale_cancelled
)

SELECT 
    date as date,
    Staff,
    prod_cat as product_category,
    SUM(issued_unique) AS issued_unique,
    Sum(Quoted) as Quoted,
    SUM(total_sold) AS sold,
    SUM(total_cancelled) AS total_cancelled,
 --    SUM(total_price) AS total_price,
    SUM(total_sales_amount) AS total_sold,
    SUM(total_cancelled_amount) AS total_cancelled_amount,

    -- ADL Calculation
    CASE 
        WHEN SUM(issued_unique) = 0 THEN 0
        ELSE (SUM(total_sales_amount) - COALESCE(SUM(total_cancelled_amount), 0)) / SUM(issued_unique)
    END AS ADL,

    -- ADS Calculation
    CASE 
        WHEN SUM(total_price) = 0 THEN 0
        ELSE 
            (SUM(total_sales_amount) - COALESCE(SUM(total_cancelled_amount), 0)) 
            / NULLIF((SUM(total_sold) - COALESCE(SUM(total_cancelled), 0)), 0)
    END AS ADS

FROM combined_results
where date <= curdate() and year(date)>= 2019
GROUP BY 1 , Staff,prod_cat
ORDER BY 1 DESC;

END$$
DELIMITER ;
