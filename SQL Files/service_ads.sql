DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `service_ads`()
BEGIN
WITH appointment_col AS (
    SELECT  
        i360__Start__c AS Date,
        TRIM(LOWER(Sales_Rep_Name__c)) AS Staff, 
        SUM(supportworks__Issued_Unique__c) AS issued_unique
    FROM pre_stage.appointment
    where
   	i360__Issue1__c <> 0
	AND COALESCE(i360__Type__c, '') IN ('WCC', 'service opportunity', 'SaniDry Return')
    GROUP BY 1,2
),
sale_Sold AS (
 SELECT 
        CAST(i360__Sold_On__c AS DATE) AS Date,
        TRIM(LOWER(Sales_Rep_1_Name__c)) AS Staff,
        SUM(supportworks__Total_Sales__c) AS sold_total,
        sum(supportworks__Original_Sold_Price__c) as price,
        sum(Sold__c) as sold
    FROM pre_stage.sale
    WHERE  COALESCE(supportworks__Sale_Type__c, '') IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
    GROUP BY 1,2
    order by 1 desc
),
sale_cancelled AS (
     SELECT 
        CAST(i360__Canceled_Date__c AS DATE) AS Date,
       TRIM(LOWER(Sales_Rep_1_Name__c)) AS Staff,
        SUM(i360__Canceled__c) AS cancelled,
        SUM(i360__Canceled_Amt__c) AS cancelled_amt
    FROM pre_stage.sale
    WHERE supportworks__Sale_Type__c IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
    GROUP BY 1,2
),

combined_results AS (
    SELECT 
        Date, 
        Staff, 
        issued_unique, 
        0 AS sold, 
        0 AS cancelled, 
        0 AS price, 
        0 AS sold_total, 
        0 AS cancelled_amt
    FROM appointment_col

    UNION ALL

    SELECT 
        Date, 
        Staff, 
        0 AS issued_unique, 
        sold AS sold, 
        0 AS cancelled, 
        Price AS price, 
        sold_total AS sold_total, 
        0 AS cancelled_amt
    FROM sale_Sold

    UNION ALL

    SELECT 
        Date, 
        Staff, 
        0 AS issued_unique, 
        0 AS sold, 
        cancelled AS cancelled, 
        0 AS price, 
        0 AS sold_total, 
        cancelled_amt AS cancelled_amt
    FROM sale_cancelled
)
select date, staff, sum(issued_unique) as issued_unique, 
sum(sold) as sold ,sum(cancelled) as cancelled, sum(price) price, sum(sold_total) sold_total,
sum(cancelled_amt) cancelled_amt,
case
 WHEN SUM(issued_unique) = 0 THEN 0
 WHEN SUM(cancelled_amt) IS NULL THEN SUM(sold_total) / SUM(issued_unique)
 ELSE (SUM(sold_total) - SUM(cancelled_amt)) / SUM(issued_unique)
 END as ADS
 from combined_results group by 1,2;
END$$
DELIMITER ;
