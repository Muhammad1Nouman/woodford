DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sale_prod_service_funnel`()
BEGIN
with sale_prod as(
 select 
        date(s.Foreman_Sale_Date__c) as date, 
        sum(s.Foreman_Commission_Basis__c) as prod
        from pre_stage.sale s 
     where s.supportworks__Sale_Type__c NOT IN ('annual maintenance', 'service opportunity', 'added protection') and Foreman_Sale_Date__c is not null
    group by 1
    order by 1 desc),
sale_service as(
 SELECT
        s.i360__Sold_on__c AS date,
 
        SUM(CASE WHEN s.supportworks__Sale_Type__c IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection') 
                 THEN s.supportworks__Total_Sales__c ELSE 0 END) AS service,
        SUM(CASE WHEN s.supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection') 
         and s.supportworks__Department__c = 'Sales'
                 THEN s.supportworks__Total_Sales__c ELSE 0 END) AS sold
    FROM pre_stage.sale s
    LEFT JOIN pre_stage.appointment appt ON s.i360__Appointment_Id__c = appt.id
    GROUP BY 1),
sale_cancelled AS (
    SELECT 
        CAST(i360__Canceled_Date__c AS DATE) AS Date,
        SUM(i360__Canceled_Amt__c) AS cancelled
    FROM pre_stage.sale
    WHERE supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
      AND supportworks__Department__c = 'Sales'
    GROUP BY 1
),

datedimension AS (
    SELECT date_dimension.Date AS Date
    FROM pre_stage.date_dimension
    WHERE date_dimension.Date > '2019-01-01' 
          AND date_dimension.Date <= LAST_DAY(CONCAT(YEAR(CURDATE()), '-12-01'))
),
actual_result as(
    select 
        dd.date as date,
        "actual" as type,
        COALESCE(Sum(p.prod),0) as prod,
        COALESCE(Sum(ss.service),0) as service,
         coalesce(sum(ss.sold),0) -  coalesce(sum(cs.cancelled),0) as std
        from datedimension dd
        left join sale_prod p on dd.date = p.date
        left join sale_service ss on dd.date = ss.date
        left join sale_cancelled cs on dd.date = cs.date
        group by 1
        order by 1 desc
        
) ,
monthly_value as(
    select 
    YEAR(date) AS year,
    MONTH(date) AS month,
    sum(prod) as prod,
    sum(service) as service,
    sum(std) as std
    from actual_result
    GROUP BY 1, 2
),
averages_calculation as (
    select  year, month, 
 AVG(prod) OVER (PARTITION BY month ORDER BY year ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS avg_prod,
 AVG(service) OVER (PARTITION BY month ORDER BY year ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS avg_service,
 AVG(std) OVER (PARTITION BY month ORDER BY year ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS avg_std
 from monthly_value
),
combined_avg as(
    select  
    mv.year as year,
    mv.month as month,
    ac.avg_prod as prod,
    ac.avg_service as service,
    ac.avg_std as std
    from monthly_value mv
    join averages_calculation ac
    ON mv.month = ac.month AND mv.year = ac.year
    ORDER BY year DESC, month DESC
),
yearly_goals_avg as(
    select 
    year,
    sum(prod) as prod,
    sum(service) as service,
    sum(std) as std
    from combined_avg
GROUP BY year   
ORDER BY year
),
monthlypercentages as(
    SELECT cm.year AS year,
           cm.month AS month,
    (cm.prod/yt.prod) * 100 as prod_percentage,
    (cm.service/yt.service) * 100 as service_percentage,
    (cm.std/yt.std) * 100 as std_percentage
    FROM combined_avg cm
    JOIN yearly_goals_avg yt ON cm.year = yt.year
),
monthly_sale_with_percentage AS (
    SELECT mp.year AS year,
           mp.month AS month,
           mp.prod_percentage as prod,
           mp.service_percentage as service,
           mp.std_percentage as std
    FROM monthlypercentages mp
),
sale_into_percent as(
   SELECT yg.Year AS Year,
    mp.month AS month,
    DAYOFMONTH(LAST_DAY(CONCAT(yg.Year, '-', mp.month, '-01'))) AS days_in_month,
    (yg.prod * (mp.prod / 100)) AS prod,
    (yg.service* (mp.service / 100)) AS service,
    (yg.std * (mp.std / 100)) AS std
    FROM pre_stage.yearly_goals yg
    JOIN monthly_sale_with_percentage mp ON yg.Year = mp.year
    where yg.type ="goal"
)
,
final_result AS (
    SELECT sp.Year AS Year,
           sp.month AS month,
           sp.days_in_month AS days_in_month,
           sp.prod AS prod,
           sp.service as service,
           sp.std as std
    FROM sale_into_percent sp
),
daily_values AS (
    SELECT fr.Year AS year,
           fr.month AS month,
           dd.Date AS date,
           (fr.prod / fr.days_in_month) AS prod,
           (fr.service / fr.days_in_month) AS service,
           (fr.std / fr.days_in_month) AS std
    FROM final_result fr
    JOIN datedimension dd ON dd.Date BETWEEN CONCAT(fr.Year, '-', LPAD(fr.month, 2, '0'), '-01')
                           AND LAST_DAY(CONCAT(fr.Year, '-', LPAD(fr.month, 2, '0'), '-01'))
), goal_result as(
select  dy.date as date,
'goal' as type,
dy.prod as prod,
dy.service as service,
dy.std as std
FROM daily_values dy
)
select * from goal_result 
UNION ALL
select * from actual_result
order by date desc, type asc;

END$$
DELIMITER ;
