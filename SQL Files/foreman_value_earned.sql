DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `foreman_value_earned`()
BEGIN
with prod_ve as (
 select  i360__Start_Date__c as date,
 COALESCE(i360__Completed_On__c, CURRENT_DATE) as complete_date,
 Prod_Cat_1_Proj_Shrt_List__c as product_Category,
Project_Manager_Name__c  Foreman,
 sum(VE_This_Period__c) as prod_ve,
 0 as service_ve
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
 group by 1,2,3,4
) ,
service_ve as (
select
i360__Sold_On__c as date,
 NULL AS complete_date,
Prod_Cat_1_Sale_Shrt_List__c as product_Category,
Project_Completed_By_Fman__c as Foreman,
0 as prod_ve,
sum(supportworks__Net_Sales_dollars__c) as service_ve
from pre_stage.sale
where  (supportworks__Sale_Type__c in ('Annual Maintenance', 'Service Opportunity', 'Added Protection') and
Project_Completed_By_Dept__c = "Service Dept."
) OR (supportworks__Sale_Type__c in ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
and Project_Completed_By_Dept__c is null)
group by 1,2,3,4),
 date_dimension AS (
    SELECT date_dimension.Date AS date 
    FROM pre_stage.date_dimension 
    WHERE date_dimension.Date > '2019-01-01' 
    AND date_dimension.Date <= (SELECT GREATEST(MAX(i360__Start_Date__c),MAX(i360__Completed_On__c)) FROM project)
),
combined_results AS (
    SELECT * FROM prod_ve
    UNION ALL
    SELECT * FROM service_ve
)
    SELECT 
        d.date AS date,
        c.complete_date as completed_date,
        c.product_Category AS product_Category,
        c.Foreman AS Foreman,
        COALESCE(SUM(c.prod_ve), 0) AS prod_ve,
        COALESCE(SUM(c.service_ve), 0) AS service_ve,
        COALESCE(SUM(c.prod_ve), 0) + COALESCE(SUM(c.service_ve), 0) AS GT_total
    FROM date_dimension d
    LEFT JOIN combined_results c ON d.date = c.date
    GROUP BY 1, 2,3,4
    ORDER BY 1 DESC
;
END$$
DELIMITER ;
