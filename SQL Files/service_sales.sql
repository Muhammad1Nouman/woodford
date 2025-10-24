DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `service_sales`()
BEGIN
with appoint as (
    select 
    i360__Start__c as date,
    Consolidated_Sales_Rep_1__c as staff,
    supportworks__Product_Category_1__c as prod_cat,
    sum(supportworks__Issued_Unique__c) as issued_unique,
    sum(i360__Sold__c) as sold,
    0 as price,
    0 as total_sales,   
    0 as cancelled_amt
from pre_stage.appointment
where 	i360__Issue1__c <> 0 and
	i360__Type__c in ("WCC","Service Opportunity", "SaniDry Return")
GROUP BY 1,2,3
),
sales as (
    select i360__Sold_on__c as date,
    Sales_Rep_1_Name__c as staff,
    Prod_Cat_1_Sale_Shrt_List__c as prod_cat,
    0 as issued_unique,
    0 as sold,
    sum(i360__Sold_Price__c) as price,
    sum(supportworks__Total_Sales__c) as total_sales,
    0 as cancelled_amt
from pre_stage.sale
where  supportworks__Sale_Type__c IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection') 
GROUP BY 1,2,3
),
cancelled as 
(select
i360__Canceled_Date__c as date,
    Sales_Rep_1_Name__c as staff,
    Prod_Cat_1_Sale_Shrt_List__c as prod_cat,
    0 as issued_unique,
	0 as sold,
    0 as price,
    0 as total_sales,
    sum(i360__Canceled_Amt__c) as cancelled_amt
from pre_stage.sale
where 
supportworks__Sale_Type__c IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection') 
GROUP BY 1,2,3
),
combined as (
    select * from appoint
    union all
    select * from sales
    union all
    select * from cancelled
),
datedimension AS (
    SELECT Date AS date
    FROM date_dimension
    WHERE Date > '2018-01-01'
      AND Date <= CURDATE()
)
select d.date as date,
		 c.staff as staff,
      c.prod_cat as product_category,
         sum(c.issued_unique) as issued_unique,
         sum(c.sold) as sold,
         SUM(c.price) as price,
         sum(c.total_sales) as total_sales,
         sum(c.cancelled_amt) as cancelled_amt,
         sum(c.total_sales) - sum(c.cancelled_amt) as Net_Sales

         
from datedimension d
left join combined c on d.date = c.date
group by 1 , 2, 3
order by 1 , 2, 3;
END$$
DELIMITER ;
