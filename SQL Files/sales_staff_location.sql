DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sales_staff_location`()
BEGIN
WITH appointment_col AS (
    SELECT  
        i360__Start__c AS Date,
        TRIM(LOWER(Sales_Rep_Name__c)) AS Staff, 
        supportworks__Product_Category_1__c AS prod_cat,
        i360__City__c AS city,
        SUM(supportworks__Issued_Unique__c) AS issued_unique
    FROM pre_stage.appointment
    WHERE COALESCE(supportworks__Product_Category_1__c, '') <> 'Service'
      AND COALESCE(i360__Source_Type__c, '') NOT IN ('annual maintenance', 'service opportunity', 'existing database', 'service')
      AND i360__Type__c IN ('New', 'Rehash', 'Reset', 'Radon CRM Pickup')
      and supportworks__Sales_Rep_1_Department__c = "Sales"
      and appointment.supportworks__Product_Category_1__c IN (
             SELECT DISTINCT Prod_Cat_1_Sale_Shrt_List__c FROM pre_stage.sale) 
    GROUP BY 1,2,3,4
),
sale_Sold AS (
    SELECT 
        CAST(i360__Sold_On__c AS DATE) AS Date,
        TRIM(LOWER(Sales_Rep_1_Name__c)) AS Staff,
        Prod_Cat_1_Sale_Shrt_List__c AS prod_cat,
        i360__Appointment_City__c AS city,
        SUM(supportworks__Total_Sales__c) AS sold_total,
        SUM(Sold__c) AS sold,
        SUM(i360__Sold_Price__c) AS Price
    FROM pre_stage.sale
    WHERE supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
      AND supportworks__Department__c = 'Sales'
    GROUP BY 1,2,3,4
),
sale_cancelled AS (
    SELECT 
        CAST(i360__Canceled_Date__c AS DATE) AS Date,
        TRIM(LOWER(Sales_Rep_1_Name__c)) AS Staff,
        Prod_Cat_1_Sale_Shrt_List__c as prod_cat,
        i360__Appointment_City__c AS city,
        SUM(i360__Canceled__c) AS cancelled,
        SUM(i360__Canceled_Amt__c) AS cancelled_amt
    FROM pre_stage.sale
    WHERE supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
      AND supportworks__Department__c = 'Sales'
    GROUP BY 1,2,3,4
),

combined_results AS (
    SELECT 
        Date AS Year, 
        Staff, 
        prod_cat,
        city,
        issued_unique, 
        0 AS total_sold, 
        0 AS total_cancelled, 
        0 AS total_price, 
        0 AS total_sales_amount, 
        0 AS total_cancelled_amount
    FROM appointment_col

    UNION ALL

    SELECT 
        Date AS Year, 
        Staff, 
        prod_cat,
        city,
        0 AS issued_unique, 
        sold AS total_sold, 
        0 AS total_cancelled, 
        Price AS total_price, 
        sold_total AS total_sales_amount, 
        0 AS total_cancelled_amount
    FROM sale_Sold

    UNION ALL

    SELECT 
        Date AS Year, 
        Staff, 
        prod_cat,
        city,
        0 AS issued_unique, 
        0 AS total_sold, 
        cancelled AS total_cancelled, 
        0 AS total_price, 
        0 AS total_sales_amount, 
        cancelled_amt AS total_cancelled_amount
    FROM sale_cancelled
)

SELECT 
    Year as date,
    Staff,
    prod_cat as product_category,
    city AS appointment_location,
    COALESCE(SUM(issued_unique), 0) AS issued_unique,
    COALESCE(SUM(total_sold), 0) AS sold,
    COALESCE(SUM(total_cancelled), 0) AS total_cancelled,
    COALESCE(SUM(total_price), 0) AS total_price,
    COALESCE(SUM(total_sales_amount), 0) AS total_sold,
    COALESCE(SUM(total_cancelled_amount), 0) AS total_cancelled_amount
FROM combined_results
where year <= curdate()
GROUP BY Year, Staff, prod_cat,city
ORDER BY Year DESC;
END$$
DELIMITER ;
