DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `goal_marketing_expenditure`()
BEGIN
with monthly_goals_with_percentage AS (
    SELECT 
        mp.year,
        mp.month,
        mp.value AS value
    FROM pre_stage.monthly_distribution mp
),
goal_into_percent AS (
    SELECT 
        yg.Year as year,
        mp.month as month,
        DAYOFMONTH(LAST_DAY(CONCAT(yg.Year, '-', mp.month, '-01'))) AS days_in_month,
        yg.product_category,
        (yg.leads_nd * mp.value) AS leads_count,
        (yg.marketing * mp.value) AS expenditure
        
    FROM pre_stage.goal_distribution yg
    JOIN monthly_goals_with_percentage mp 
        ON yg.Year = mp.year
),
date_dimension AS (
    SELECT date_dimension.Date AS date 
    FROM pre_stage.date_dimension 
    WHERE date_dimension.Date > '2019-01-01' 
    AND date_dimension.Date <= LAST_DAY(CONCAT(YEAR(CURDATE()), '-12-01'))
),
daily_values as(
    SELECT 
        dd.date,
        gd.year,
        gd.month,
        gd.product_category as categories,
        gd.leads_count / gd.days_in_month AS leads_count,
        gd.expenditure / gd.days_in_month AS marketing_expenditure

    FROM goal_into_percent gd 
    JOIN date_dimension dd 
        on dd.date BETWEEN CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01') 
                     AND LAST_DAY(CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01'))

)
SELECT 
    date,
    categories,
    "goal" as type,
    sum(leads_count) as leads_count ,
    sum(marketing_expenditure) as expenditure
  
FROM daily_values
group by 1,2
ORDER BY 1 DESC;
END$$
DELIMITER ;
