DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sale_sold_canceled`()
BEGIN
WITH sales_data AS (
    SELECT 
        sales_Rep_1_Name__c AS staff,
        DATE(COALESCE(i360__Sold_on__c, i360__Canceled_Date__c)) AS date,
        SUM(CASE WHEN i360__Sold_on__c IS NOT NULL THEN Sold__c ELSE 0 END) AS sold,
		SUM(CASE WHEN i360__Sold_on__c IS NOT NULL THEN i360__Sold_Price__c ELSE 0 END) AS sold_price,
		SUM(CASE WHEN i360__Sold_on__c IS NOT NULL THEN supportworks__Total_Sales__c ELSE 0 END) AS total_Sale,
        SUM(CASE WHEN i360__Canceled_Date__c IS NOT NULL THEN i360__Canceled__c ELSE 0 END) AS cancelled,
		SUM(CASE WHEN i360__Canceled_Date__c IS NOT NULL THEN i360__Canceled_Amt__c ELSE 0 END) AS cancelled_amt
    FROM 
        sale
    WHERE 
        supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
    GROUP BY 
        sales_Rep_1_Name__c, 
        DATE(COALESCE(i360__Sold_on__c, i360__Canceled_Date__c))
),
DateDimension AS (
    SELECT 
        date
    FROM 
        date_dimension
    WHERE 
        date > '2018-01-01' 
        AND date <= CURRENT_DATE
)
select 
d.date as Date,
sd.staff as Staff,
sum(sd.sold) as Sale_Sold,
sum(sd.cancelled) as Sale_cancled,
sum(sd.sold_price) as sold_price,
sum(sd.cancelled_amt) as cancelled_amt,
sum(sd.total_Sale) as total_sale

from DateDimension d
left join sales_data sd on d.date = sd.date
group by 1 ,2
order by date desc;
END$$
DELIMITER ;
