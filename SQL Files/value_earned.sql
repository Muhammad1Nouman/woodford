DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `value_earned`()
BEGIN
with prod_ve as (
 select   i360__Start_Date__c as date,
 sum(VE_This_Period__c) as prod_ve
 from pre_stage.project
 where  
 (
	i360__Status__c <> "Canceled" 
	and i360__Job_Type__c <> "021 - Service Work"
	and i360__Completed_On__c >="2025-01-01"
	and i360__Completed_On__c <="2025-12-31"
	and Percentage_of_Completion__c > 0

  )
  OR 
  (
	i360__Status__c <> "Canceled"  
	and i360__Job_Type__c <> "021 - Service Work"
	and Percentage_of_Completion__c > 0
    and Percentage_of_Completion__c < 100
  )
 group by 1
) ,service_ve as (
select
i360__Sold_On__c as date,
sum(supportworks__Net_Sales_dollars__c) as service_ve
from pre_stage.sale
where  (supportworks__Sale_Type__c in ('Annual Maintenance', 'Service Opportunity', 'Added Protection') and
Project_Completed_By_Dept__c = "Service Dept."
) OR (supportworks__Sale_Type__c in ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
and Project_Completed_By_Dept__c is null)
group by 1
),
date_dimension AS (
    SELECT date_dimension.Date AS date 
    FROM pre_stage.date_dimension 
    WHERE date_dimension.Date > '2019-01-01' 
    AND date_dimension.Date <= curdate()
)
select d.date as date,
coalesce(Sum(prod_ve),0) as prod_ve,
coalesce(sum(service_ve),0) as service_ve,
COALESCE(Sum(prod_ve), 0) + COALESCE(SUM(service_ve), 0) AS GT_total
from date_dimension d
left join prod_ve on d.date = prod_ve.date
left join service_ve on d.date = service_ve.date
group by 1
order by 1 desc;
END$$
DELIMITER ;
