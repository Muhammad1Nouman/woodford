DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `Product_Category_2`()
BEGIN

WITH marketing_agg AS (
    SELECT 
        date, 
        product_category, 
        COALESCE(SUM(per_lead_cost), 0) AS per_lead_cost,
        COALESCE(SUM(total_cost), 0) AS total_lead_cost
    FROM pre_stage.marketing
    GROUP BY date, product_category
),
sales_agg AS (
    SELECT 
        date, 
        product_category, 
        COALESCE(SUM(prod), 0) AS prod,
        COALESCE(SUM(service), 0) AS service,
        COALESCE(SUM(std), 0) AS std
    FROM pre_stage.sale_rep_prod_cat
    GROUP BY date, product_category
),
actual_result as(
SELECT
    st.date AS date,
	'actual' AS type,
     st.product_category AS product_category,
    SUM(st.Leads_Taken) AS Leads_Taken,
    SUM(st.leads_nd) AS lead_nd,
    SUM(st.appointments_set) AS appointments_set,
    SUM(st.appointments_cancelled) AS appointments_cancelled,
    SUM(st.appointments_issued_cc) AS appointments_issued,
    SUM(st.quote_issued) AS quote_issued,
    SUM(st.sold) AS sold,
    SUM(st.cancelled) AS cancelled,
    SUM(COALESCE(ma.per_lead_cost, 0)) AS per_lead_cost,       
    SUM(COALESCE(ma.total_lead_cost, 0)) AS total_lead_cost,   
    SUM(COALESCE(sa.prod, 0)) AS prod,                         
    SUM(COALESCE(sa.service, 0)) AS service,                   
    SUM(COALESCE(sa.std, 0)) AS std   
FROM stage.actual_prod_cat st
LEFT JOIN marketing_agg ma 
    ON st.date = ma.date 
    AND st.product_category = ma.product_category
LEFT JOIN sales_agg sa 
    ON st.date = sa.date 
    AND st.product_category = sa.product_category
GROUP BY st.date,  st.product_category 
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
        (pyg.Leads_Taken/12) AS Leads_Taken,
        (pyg.Leads_nd/12) AS Leads_nd,
        (pyg.appointment_set/12) AS appointment_set,
        (pyg.appointment_canceled/12) AS appointment_cancelled,
        (pyg.appointment_issued/12) AS appointment_issued,
        (pyg.Quoted/12) AS quote_issued,
        (pyg.sale_sold/12) AS sold,
        (pyg.sale_cancelled/12) AS cancelled,
        (pyg.marketing/12) as total_cost,
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
        rm.Leads_Taken,
        rm.Leads_nd,
        rm.appointment_set,
        rm.appointment_cancelled,
        rm.appointment_issued,
        rm.quote_issued,
        rm.sold,
        rm.cancelled,
        rm.total_cost,
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
        (gip.Leads_Taken / gip.days_in_month) AS Leads_Taken,
        (gip.Leads_nd / gip.days_in_month) AS Leads_nd,
        (gip.appointment_set / gip.days_in_month) AS appointment_set,
        (gip.appointment_cancelled / gip.days_in_month) AS appointment_cancelled,
        (gip.appointment_issued / gip.days_in_month) AS appointment_issued,
        (gip.quote_issued / gip.days_in_month) AS quote_issued,
        (gip.sold / gip.days_in_month) AS sold,
        (gip.cancelled / gip.days_in_month) AS cancelled,
        (gip.total_cost / gip.days_in_month) AS total_cost,
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
    Leads_Taken,
    Leads_nd,
    appointment_set,
    appointment_cancelled,
    appointment_issued,
    quote_issued,
    sold,
    cancelled,
    (total_cost/leads_nd) as per_lead_cost,
    total_cost,
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
