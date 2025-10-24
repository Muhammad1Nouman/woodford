DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `sale_amount_goal`()
BEGIN
WITH monthly_distribution AS (
    SELECT
        g.year,
        m.month,
        DAYOFMONTH(LAST_DAY(CONCAT(g.Year, '-', m.month, '-01'))) AS days_in_month,
        g.product_category,
        g.name,
        g.net_sale * m.sales_value AS net_sale_goal,
        g.net_sale_count * m.sales_value AS net_sale_count_goal
    FROM
        pre_stage.goal_distribution_name g
    JOIN
        pre_stage.monthly_distribution m
        ON g.year = m.year
),
date_dimension AS (
    SELECT 
        date_dimension.Date AS date_col
    FROM pre_stage.date_dimension 
    WHERE date_dimension.Date > '2016-01-01' 
      AND date_dimension.Date <= LAST_DAY(CONCAT(YEAR(CURDATE()), '-12-01'))
),
final_result AS (
    SELECT  
        md.year,
        md.month,
        dd.date_col AS date,
        md.days_in_month,
        md.name,
        md.product_category,
        (md.net_sale_goal / md.days_in_month) AS goal,
        (md.net_sale_count_goal / md.days_in_month) AS net_sale_count
    FROM monthly_distribution md
    JOIN date_dimension dd 
        ON dd.date_col BETWEEN CONCAT(md.year, '-', LPAD(md.month, 2, '0'), '-01') 
                           AND LAST_DAY(CONCAT(md.year, '-', LPAD(md.month, 2, '0'), '-01'))      
)
SELECT 
    date,
    name,
    product_category, 
    goal,
    net_sale_count
FROM final_result
ORDER BY date, name, product_category;

END$$
DELIMITER ;
