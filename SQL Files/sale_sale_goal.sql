DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `sale_sale_goal`()
BEGIN

with monthly_goals_with_percentage AS (
    SELECT 
        mp.year,
        mp.month,
        mp.sales_value AS value
    FROM pre_stage.monthly_distribution mp
),
goal_into_percent AS (
    SELECT 
        yg.Year as year,
        mp.month as month,
        DAYOFMONTH(LAST_DAY(CONCAT(yg.Year, '-', mp.month, '-01'))) AS days_in_month,
        (yg.sale_count * mp.value) AS sale_count,
        (yg.sold_amount * mp.value) AS sold_amount
    FROM pre_stage.yearly_goals yg
    JOIN monthly_goals_with_percentage mp 
        ON yg.Year = mp.year
	WHERE yg.Type <> 'wbi_goal'
),
date_dimension AS (
    SELECT date_dimension.Date AS date 
    FROM date_dimension 
    WHERE date_dimension.Date > '2019-01-01' 
    AND date_dimension.Date <= LAST_DAY(CONCAT(YEAR(CURDATE()), '-12-01'))
),
daily_values as(
    SELECT 
        dd.date,
        gd.year,
        gd.month,
        gd.sale_count / gd.days_in_month AS sale_count,
        gd.sold_amount / gd.days_in_month AS sold_amount
    FROM goal_into_percent gd 
    JOIN date_dimension dd 
        on dd.date BETWEEN CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01') 
                     AND LAST_DAY(CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01'))

)
SELECT 
    date,
    "goal" as type,
    sum(sale_count) as "SALE #",
    sum(sold_amount) as  "Sale $"
FROM daily_values
group by 1
ORDER BY 1 DESC;
END$$
DELIMITER ;
