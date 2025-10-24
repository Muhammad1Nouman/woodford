DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `goal_by_category`()
BEGIN
with monthly_distribution as(
SELECT
    g.year,
	m.month,
    DAYOFMONTH(LAST_DAY(CONCAT(g.Year, '-', m.month, '-01'))) AS days_in_month,
    g.product_category,
    g.leads_nd * m.value AS leads_nd,
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
date_dimension AS (
    SELECT date_dimension.Date AS date 
    FROM pre_stage.date_dimension 
    WHERE date_dimension.Date > '2019-01-01' 
    AND date_dimension.Date <= LAST_DAY(CONCAT(YEAR(CURDATE()), '-12-01'))
),
final_result AS (
    select  
        md.year,
        md.month,
        dd.date,
        md.days_in_month,
        md.product_category,
       (md.leads_nd / md.days_in_month) as leads_nd,
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


)
SELECT 
date,
product_category, 
leads_nd,
issued_unique,  
quoted,
sold,
canceled,
net,
adl,
sale_service,
sale_service_net_amt
FROM final_result;
END$$
DELIMITER ;
