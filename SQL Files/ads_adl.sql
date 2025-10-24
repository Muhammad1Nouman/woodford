DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `ads_adl`()
BEGIN
with appointment_col as (
select 	i360__Start__c as Date,
sum(supportworks__Issued_Unique__c) issued_unique
from pre_stage.appointment
where coalesce(supportworks__Product_Category_1__c,'') <> "Service"
AND coalesce(i360__Source_Type__c,'') NOT IN ('annual maintenance', 'service opportunity', 'existing database', 'service')
AND i360__Type__c IN ('New', 'Rehash', 'Reset', 'Radon CRM Pickup')
and supportworks__Sales_Rep_1_Department__c = "Sales"
group by 1 order by 1 desc
),
sale_Sold as (
SELECT 
		CAST(i360__Sold_On__c AS DATE) AS date,
		SUM(supportworks__Total_Sales__c) AS sold_total ,
        sum(Sold__c) as sold,
        sum(i360__Sold_Price__c) as Price
        FROM pre_stage.sale
        WHERE 	supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
        and supportworks__Department__c = "Sales"
        GROUP by 1
),
sale_cancelled as (
SELECT 
		CAST(i360__Canceled_Date__c AS DATE) AS date,
		sum(i360__Canceled__c) as cancelled,
		sum(i360__Canceled_Amt__c) AS cancelled_amt
        FROM pre_stage.sale
        WHERE 	supportworks__Sale_Type__c NOT IN ('Annual Maintenance', 'Service Opportunity', 'Added Protection')
        and supportworks__Department__c = "Sales"
        GROUP by 1
),
datedimension AS (
        SELECT Date AS date 
        FROM pre_stage.date_dimension 
        WHERE Date > '2018-01-01' 
        AND Date <= CURDATE()
)
SELECT 
	dd.date as date,
    CASE 
        WHEN SUM(ac.issued_unique) = 0 THEN 0
        ELSE (SUM(ss.sold_total) - COALESCE(SUM(sc.cancelled_amt), 0)) / SUM(ac.issued_unique)
    END AS ADL,
    CASE 
        WHEN SUM(ss.Price) = 0 THEN 0
        ELSE 
            (SUM(ss.sold_total) - COALESCE(SUM(sc.Cancelled_Amt), 0)) 
            / (SUM(ss.Sold) - COALESCE(SUM(sc.Cancelled), 0))
    END AS ADS
    
    from datedimension dd
    left join appointment_col ac on dd.date = ac.date
    left join sale_Sold ss on  dd.date = ss.date
    left join  sale_cancelled sc on dd.date = sc.date
    group by 1 order by 1 desc;


END$$
DELIMITER ;
