DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `fact_table_goal`()
BEGIN
with wbi_goal as (
    SELECT
        a.date,
        ROUND(a.leads_nd / (b.leads_taken / 100), 5) AS lead_taken,
        @step1 := ROUND(a.leads_nd, 5) AS leads_nd,
        @step2 := ROUND(@step1 * (b.appointment_set / 100), 5) AS appointment_set,
        @step3 := ROUND(@step2 * (b.appointment_canceled / 100), 5) AS appointment_canceled,
        @step4 := ROUND(@step2 * (b.appointment_issued / 100), 5) AS appointments_issued_cc
		-- @step5 := ROUND(@step2 * (b.sales_issued / 100), 5) AS appointments_issued_sale
--         @step6 := @step5 * (b.Quoted / 100) AS quote_issued,
--         @step7 := @step6 * (b.sale_sold / 100) AS sale_sold,
--         @step8 := @step7 * (b.sale_cancelled / 100) AS sale_cancelled
    FROM pre_stage.pbi_leads_nd a
    CROSS JOIN pre_stage.yearly_goals b
    WHERE b.type = "wbi_goal"
),
 

monthvise_goal as(
SELECT  
    YEAR(date) AS year,  
    MONTH(date) AS month,
    SUM(lead_taken) AS total_leads_taken,
    SUM(leads_nd) AS total_leads_nd,
    SUM(appointment_set) AS total_Appointments_Set,
    SUM(appointment_canceled) AS total_appointments_cancelled,
    SUM(appointments_issued_cc) AS total_Appointments_Issued_cc
--     SUM(appointments_issued_sale) AS total_Appointments_Issued_sale,
--     SUM(quote_issued) AS total_Quote_Issued,
--     SUM(sale_sold) AS total_sale_sold,
--     SUM(sale_cancelled) AS total_sale_Cancelled
FROM wbi_goal
GROUP BY YEAR(date), MONTH(date)
ORDER BY YEAR(date) DESC, MONTH(date)
),


final_goal AS (
    SELECT 
        mg.year,
        mg.month,
        DAYOFMONTH(LAST_DAY(CONCAT(pg.Year, '-', mg.month, '-01'))) AS days_in_month,
        pg.product_category,
        mg.total_leads_taken * pg.leads_taken AS leads_taken,
        mg.total_leads_nd * pg.leads_nd AS leads_nd,
        mg.total_Appointments_Set * pg.Appointment_Set AS Appointments_Set,
        mg.total_appointments_cancelled * pg.Appointment_Canceled AS Appointments_Cancelled,
        mg.total_Appointments_Issued_cc * pg.appointment_issued AS Appointments_Issued_cc
     --    mg.total_Appointments_Issued_sale * pg.sales_issued AS Appointments_Issued_sale,
--         mg.total_Quote_Issued * pg.Quoted AS Quote_Issued,
--         mg.total_sale_sold * pg.sale_sold AS sale_sold,
--         mg.total_sale_Cancelled * pg.sale_cancelled AS sale_cancelled

    FROM 
        monthvise_goal mg
    JOIN
        pre_stage.product_yearly_goals pg ON mg.year = pg.year
    ORDER BY mg.year DESC, mg.month DESC
)
,
date_dimension AS (
    SELECT date_dimension.Date AS date 
    FROM pre_Stage.date_dimension 
    WHERE date_dimension.Date > '2019-01-01' 
    AND date_dimension.Date <= LAST_DAY(CONCAT(YEAR(CURDATE()), '-12-01'))
),
daily_values AS (
    SELECT 
        fr.year,
        fr.product_category,
        fr.month,
        dd.date,
        (fr.leads_taken / fr.days_in_month) AS leads_taken,
        (fr.leads_nd / fr.days_in_month) AS leads_nd,
        (fr.Appointments_Set / fr.days_in_month) AS Appointments_Set,
        (fr.Appointments_Cancelled / fr.days_in_month) AS Appointments_Cancelled,
        (fr.Appointments_Issued_cc / fr.days_in_month) AS Appointments_Issued_cc
        -- (fr.Appointments_Issued_sale / fr.days_in_month) AS Appointments_Issued_sale,
--         (fr.Quote_Issued / fr.days_in_month) AS Quote_Issued,
--         (fr.sale_sold / fr.days_in_month) AS sale_sold,
--         (fr.sale_Cancelled / fr.days_in_month) AS sale_Cancelled
    FROM final_goal fr
    JOIN date_dimension dd 
        ON dd.date BETWEEN CONCAT(fr.year, '-', LPAD(fr.month, 2, '0'), '-01') 
                     AND LAST_DAY(CONCAT(fr.year, '-', LPAD(fr.month, 2, '0'), '-01'))
                     ),
-- anoher view
 monthly_distribution as(
SELECT
    g.year,
	m.month,
    DAYOFMONTH(LAST_DAY(CONCAT(g.Year, '-', m.month, '-01'))) AS days_in_month,
    
    g.product_category,
    -- g.leads_nd * m.value AS leads_nd,
    g.issued_unique * m.sales_value AS issued_unique,
    g.quoted * m.sales_value AS quoted,
	g.sold * m.sales_value AS sold,
	g.canceled * m.sales_value AS canceled,
	g.net * m.sales_value AS net,
	g.adl * m.sales_value AS adl,
    g.sale_service * m.service_sale as sale_service,
    g.sale_service_net_amt *  m.service_sale as sale_service_net_amt
FROM
	pre_stage.goal_distribution g
JOIN
    pre_stage.monthly_distribution m
    ON g.year = m.year
ORDER BY
    g.year, g.product_category, m.month
),
-- select year, sum(issued_unique) from monthly_distribution where year= 2025
 

-- date_dimension AS (
--     SELECT date_dimension.Date AS date 
--     FROM pre_stage.date_dimension 
--     WHERE date_dimension.Date > '2019-01-01' 
--     AND date_dimension.Date <= LAST_DAY(CONCAT(YEAR(CURDATE()), '-12-01'))
-- ),
final_result AS (
    select  
--         md.year,
--         md.month,
        dd.date,
        -- md.days_in_month,
        md.product_category,
     --  (md.leads_nd / md.days_in_month) as leads_nd,-- 
       (md.issued_unique / md.days_in_month) as issued_unique,
       (md.quoted / md.days_in_month) as quoted,
       (md.sold / md.days_in_month) as sold,
       (md.canceled / md.days_in_month) as canceled,
       (md.net / md.days_in_month) as net,
       (md.adl / md.days_in_month) as adl,
	   (md.sale_service / md.days_in_month) as sale_service,
       (md.sale_service_net_amt/md.days_in_month) as sale_service_net_amt
    from monthly_distribution md
    JOIN date_dimension dd 
        ON dd.date BETWEEN CONCAT(md.year, '-', LPAD(md.month, 2, '0'), '-01') 
                     AND LAST_DAY(CONCAT(md.year, '-', LPAD(md.month, 2, '0'), '-01'))      
),


final_goal_output as(
SELECT
    dv.date as date,
--     dv.year,
--     dv.month,
    dv.product_category AS product_category_daily,
  --  fr.product_category AS product_category_final,
    
    -- Daily values
    dv.leads_taken,
    dv.leads_nd,
    dv.Appointments_Set,
    dv.Appointments_Cancelled,
    dv.Appointments_Issued_cc,

    -- Final result values
    fr.issued_unique as appointments_issued_sale,
    fr.quoted as quote_issued,
    fr.sold as sale_sold,
    fr.canceled as sale_cancelled,
    fr.net,
    fr.adl as ADL,
    fr.sale_service,
    fr.sale_service_net_amt

FROM daily_values dv
JOIN final_result fr
    ON dv.date = fr.date and
    dv.product_category =fr.product_category)
    


SELECT date,"goal" as type, null as Sale_rep, product_category_daily,null as i360__Source_Name__c,null as i360__Source_Type__c, null as Team_Lead, leads_taken,leads_nd,appointments_set,appointments_cancelled,
appointments_issued_cc,appointments_issued_sale,quote_issued, 0 as sold_total, sale_sold,sale_cancelled, 0 as sale_cacelled_amount, 0 as ADS, ADL
FROM final_goal_output
UNION ALL
SELECT date,type, Sale_rep, product_category,i360__Source_Name__c,i360__Source_Type__c,Team_Lead, leads_taken,leads_nd,appointments_set,appointments_cancelled,
appointments_issued_cc,appointments_issued_sale,quote_issued, sold_total, sale_sold,sale_cancelled, sale_cancelled_amount, ADS, ADL
FROM stage.customer_funnel;
END$$
DELIMITER ;
