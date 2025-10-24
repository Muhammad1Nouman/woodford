DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `prod_prod_goal`()
BEGIN
with monthly_goals_with_percentage AS (
    SELECT 
        mp.year,
        mp.month,
        mp.prod_prod AS value
    FROM pre_stage.monthly_distribution mp
),
goal_into_percent AS (
    SELECT 
        yg.Year as year,
        mp.month as month,
        DAYOFMONTH(LAST_DAY(CONCAT(yg.Year, '-', mp.month, '-01'))) AS days_in_month,
        (yg.prod_prod * mp.value) AS prod_prod
    FROM pre_stage.yearly_goals yg
    JOIN monthly_goals_with_percentage mp 
        ON yg.Year = mp.year
	WHERE yg.Type <> 'wbi_goal'
),
crew_goal_into_percent AS (
    SELECT 
        yg.Year as year,
        mp.month as month,
        DAYOFMONTH(LAST_DAY(CONCAT(yg.Year, '-', mp.month, '-01'))) AS days_in_month,
        (yg.crew_goals * mp.value) AS crew_goal
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
crew_daily_values as(
    SELECT 
        dd.date,
        gd.year,
        gd.month,
        gd.crew_goal / gd.days_in_month AS crew_goal_m
    FROM crew_goal_into_percent gd 
    JOIN date_dimension dd 
        on dd.date BETWEEN CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01') 
                     AND LAST_DAY(CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01'))
),
prod_daily_values as(
    SELECT 
        dd.date,
        gd.year,
        gd.month,
        gd.prod_prod / gd.days_in_month AS prod_prod
    FROM goal_into_percent gd 
    JOIN date_dimension dd 
        on dd.date BETWEEN CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01') 
                     AND LAST_DAY(CONCAT(gd.year, '-', LPAD(gd.month, 2, '0'), '-01'))
),
final_cte as(
select t1.date, t1.year, t1.prod_prod as prod_prod, t2.crew_goal_m as Crew_Goal from prod_daily_values t1
inner join crew_daily_values t2
on t1.date = t2.date
)
SELECT 
    date,
    "goal" as type,
    sum(prod_prod) as prod_goal,
    sum(Crew_Goal) AS Crew_goal
FROM final_cte
group by 1
ORDER BY 1 DESC;
END$$
DELIMITER ;
