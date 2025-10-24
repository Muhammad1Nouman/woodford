DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `prod_goal`()
BEGIN
WITH monthly_goals_with_percentage AS (
    SELECT 
        mp.year,
        mp.month,
        mp.prod_prod AS value
    FROM pre_stage.monthly_distribution mp
),
goal_into_percent AS (
    SELECT 
        yg.year,
        mp.month,
        yg.product_category,   -- include category
        DAYOFMONTH(LAST_DAY(CONCAT(yg.year, '-', mp.month, '-01'))) AS days_in_month,
        (yg.prod * mp.value) AS prod_value   -- use product_yearly_goals.prod
    FROM pre_stage.product_yearly_goals yg
    JOIN monthly_goals_with_percentage mp 
        ON yg.year = mp.year
    -- removed: WHERE yg.type <> 'wbi_goal'  (no such column in product_yearly_goals)
),
date_dimension AS (
    SELECT date_dimension.date AS date 
    FROM date_dimension 
    WHERE date_dimension.date > '2019-01-01' 
      AND date_dimension.date <= LAST_DAY(CONCAT(YEAR(CURDATE()), '-12-01'))
),
prod_daily_values AS (
    SELECT 
        dd.date,
        gd.year,
        gd.month,
        gd.product_category,
        gd.prod_value / gd.days_in_month AS prod_value
    FROM goal_into_percent gd 
    JOIN date_dimension dd 
        ON dd.date BETWEEN CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01') 
                       AND LAST_DAY(CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01'))
)
SELECT 
    date,
    product_category,
    'goal' AS type,
    SUM(prod_value) AS prod_goal
FROM prod_daily_values
GROUP BY date, product_category
ORDER BY date DESC, product_category;
END$$
DELIMITER ;
