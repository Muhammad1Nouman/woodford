DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `service_prod_goal`()
BEGIN

with monthly_goals_with_percentage AS (
    SELECT 
        mp.year,
        mp.month,
        mp.service_prod AS value
    FROM pre_stage.monthly_distribution mp
),
goal_into_percent AS (
    SELECT 
        yg.Year as year,
        mp.month as month,
        DAYOFMONTH(LAST_DAY(CONCAT(yg.Year, '-', mp.month, '-01'))) AS days_in_month,
        (yg.service_prod_stop * mp.value) AS service_prod_stop,
		(yg.service_prod_sale * mp.value) AS service_prod_sale
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
        gd.service_prod_stop / gd.days_in_month AS service_prod_stop,
		gd.service_prod_sale / gd.days_in_month AS service_prod_sale
        
    FROM goal_into_percent gd 
    JOIN date_dimension dd 
        on dd.date BETWEEN CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01') 
                     AND LAST_DAY(CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01'))

)
SELECT 
    date,
    "goal" as type,
    sum(service_prod_stop) AS service_prod_stop,
	sum(service_prod_sale) AS service_prod_sale
FROM daily_values
group by 1
ORDER BY 1 DESC;
END$$
DELIMITER ;
