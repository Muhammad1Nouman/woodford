DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sale_appt`()
BEGIN
with apptointments as (
SELECT 

    Sales_Rep_Name__c as Staff, 
    i360__Start__c AS Date,
    sum(supportworks__Quoted__c) AS Quoted,
   sum(i360__Issue1__c) AS Issued,
   sum(supportworks__issued_Unique__c) as unique_issued, 
   sum(i360__Sold__c) as appt_sold
FROM 
    appointment
WHERE 
    supportworks__Product_Category_1__c NOT IN ('Service')
    AND i360__Source_Type__c NOT IN ('annual maintenance', 'service opportunity', 'existing database', 'service')
    AND i360__Type__c IN ('New', 'Rehash', 'Reset', 'Radon CRM Pickup')
    group by 1,2
    
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
d.date as date,
appt.staff as Staff,
Sum(coalesce(appt.Issued,0)) as Issued,
Sum(coalesce(appt.Quoted,0)) as Qouted,
Sum(coalesce(appt.unique_issued,0)) as unique_issued,
Sum(coalesce(appt.appt_sold,0)) as appt_sold
from 
DateDimension d
left join
 apptointments appt on d.date = appt.date
 
GROUP BY 
1,2
ORDER BY 
    date desc;

END$$
DELIMITER ;
