DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sale_prod_service__cat_funnel`()
BEGIN
with sale_prod as(
 select 
        date(s.Foreman_Sale_Date__c) as date, 
        Prod_Cat_1_Sale_Shrt_List__c as Product_Category, 
        sum(s.Foreman_Commission_Basis__c) as prod,
        0 as service,
        0 as std
        from pre_stage.sale s 
 --  join appointment a on s.i360__appointment_id__c=a.id  
     where s.supportworks__Sale_Type__c NOT IN ('annual maintenance', 'service opportunity', 'added protection') and Foreman_Sale_Date__c is not null
    group by 1,2
    order by 1 desc),
sale_service as(
 SELECT
        s.i360__Sold_on__c AS date,
        Prod_Cat_1_Sale_Shrt_List__c as Product_Category, 
        0 as prod,
        SUM(CASE WHEN s.supportworks__Sale_Type__c IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection') 
                 THEN s.supportworks__Total_Sales__c ELSE 0 END) AS service,
        SUM(CASE WHEN s.supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection') 
                 THEN s.supportworks__Total_Sales__c ELSE 0 END) AS std
    FROM pre_stage.sale s
    LEFT JOIN pre_stage.appointment appt ON s.i360__Appointment_Id__c = appt.id
    GROUP BY 1, 2),
combined_results as(
select * from sale_prod
UNION ALL
select * from sale_service),
actual_result as(
       select 
        date as date,
        'actual' as type,
        Product_Category as product_category,
        prod as prod,
        service as service,
        std as std
        from combined_results
),
months AS (
    SELECT 1 AS month UNION ALL 
    SELECT 2 UNION ALL 
    SELECT 3 UNION ALL 
    SELECT 4 UNION ALL 
    SELECT 5 UNION ALL 
    SELECT 6 UNION ALL 
    SELECT 7 UNION ALL 
    SELECT 8 UNION ALL 
    SELECT 9 UNION ALL 
    SELECT 10 UNION ALL 
    SELECT 11 UNION ALL 
    SELECT 12
), 

rankedmonths AS (
    SELECT 
        pyg.Year AS Year,
        pyg.product_category AS product_category,
        m.month AS month,
        (pyg.prod / 12) AS prod,
        (pyg.service/ 12) AS service,
        (pyg.std / 12) AS std,
        RANK() OVER (PARTITION BY pyg.Year, pyg.product_category ORDER BY m.month) AS month_rank 
    FROM pre_stage.product_yearly_goals pyg 
    JOIN months m ON 1 = 1
), 

goal_into_percent AS (
    SELECT 
        rm.Year AS Year,
        rm.product_category AS product_category,
        rm.month AS month,
        DAYOFMONTH(LAST_DAY(CONCAT(rm.Year, '-', rm.month, '-01'))) AS days_in_month,
        rm.prod,
        rm.service,
        rm.std 
    FROM rankedmonths rm
    ORDER BY rm.Year,rm.product_category, rm.month
), 

date_dimension AS (
    SELECT date_dimension.Date AS date 
    FROM pre_stage.date_dimension 
    WHERE date_dimension.Date > '2019-01-01' 
    AND date_dimension.Date <= LAST_DAY(CONCAT(YEAR(CURDATE()), '-12-01'))
), 

daily_values AS (
    SELECT 
        gip.Year AS year,
        gip.product_category AS product_category,
        gip.month AS month,
        dd.date AS date,
        (gip.prod / gip.days_in_month) AS prod,
        (gip.service / gip.days_in_month) AS service,
        (gip.std / gip.days_in_month) AS std
    FROM goal_into_percent gip 
    JOIN date_dimension dd 
    ON dd.date BETWEEN CONCAT(gip.Year, '-', LPAD(gip.month, 2, '0'), '-01') 
    AND LAST_DAY(CONCAT(gip.Year, '-', LPAD(gip.month, 2, '0'), '-01'))
), 
goal_result as(
SELECT 
    date AS date,
    'goal' AS type,
    product_category,
    prod,
    service,
    std
    
FROM daily_values 
ORDER BY date)
select * from goal_result
UNION ALL
select * from actual_result
order by type asc, date desc;
END$$
DELIMITER ;
