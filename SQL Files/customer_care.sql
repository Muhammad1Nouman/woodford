DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `customer_care`()
BEGIN
WITH leadcounts AS (
    SELECT lead_source.i360__Taken_On__c AS date, 
           lead_source.supportworks__Product_Category_1__c AS prod_cat, 
           i360__Source_Type__c as source_type,
           COUNT(0) AS Leads_Taken, 
           0 AS Leads_nd, 
           0 AS appt_set, 
           0 AS appt_cancelled, 
           0 AS quote, 
           0 AS sold, 
           0 AS cancelled
    FROM lead_source 
    WHERE lead_source.i360__Taken_On__c <= CURDATE() 
      AND lead_source.i360__Source_Type__c <> 'Existing Database' 
    GROUP BY lead_source.i360__Taken_On__c, lead_source.supportworks__Product_Category_1__c,i360__Source_Type__c
), 
prospect AS (
    SELECT prospect.Id AS id 
    FROM prospect 
    WHERE prospect.i360__Not_Qualified_Reason__c LIKE '%Bad or Old Data%' 
       OR prospect.i360__Not_Qualified_Reason__c LIKE '%Spam%' 
       OR prospect.i360__Not_Qualified_Reason__c LIKE '%Not the type of work we do%' 
       OR prospect.i360__Not_Qualified_Reason__c LIKE '%Out of Area%'
), 
lead_nd AS (
    SELECT ls.i360__Taken_On__c AS date, 
           COALESCE(ls.supportworks__Product_Category_1__c, '') AS prod_cat, 
           COALESCE(ls.i360__Source_Type__c, '') AS source_type,
           0 AS Leads_Taken, 
           COUNT(0) AS Leads_nd, 
           0 AS appt_set, 
           0 AS appt_cancelled, 
           0 AS quote, 
           0 AS sold, 
           0 AS cancelled
    FROM lead_source ls
    LEFT JOIN prospect p ON ls.i360__Prospect__c = p.id
    WHERE NOT EXISTS (
        SELECT 1 
        FROM prospect sub_p 
        WHERE sub_p.id = ls.i360__Prospect__c
    ) 
      AND COALESCE(ls.i360__Source_Type__c, '') <> 'Existing Database' 
      AND COALESCE(ls.supportworks__Product_Category_1__c, '') <> 'service work' 
    GROUP BY ls.i360__Taken_On__c, ls.supportworks__Product_Category_1__c, ls.i360__Source_Type__c
), 
appointmentcounts_set AS (
    SELECT CAST(SUBSTRING_INDEX(appointment.i360__Appt_Set_On__c, 'T', 1) AS DATE) AS date, 
           appointment.supportworks__Product_Category_1__c AS prod_cat, 
           i360__Source_Type__c as source_type,
           0 AS Leads_Taken, 
           0 AS Leads_nd, 
           SUM(
               CASE 
                   WHEN COALESCE(appointment.i360__Type__c, '') IN ('New', 'Reset', 'Radon CRM Pickup') 
                    AND appointment.i360__Appt_Set_By__c IS NOT NULL 
                   THEN 1 
                   ELSE 0 
               END
           ) AS appt_set, 
           0 AS appt_cancelled, 
           0 AS quote, 
           0 AS sold, 
           0 AS cancelled
    FROM appointment
    WHERE COALESCE(appointment.supportworks__Product_Category_1__c, '') <> 'service work' 
      AND COALESCE(appointment.i360__Source_Type__c, '') <> 'Existing Database' 
    GROUP BY CAST(SUBSTRING_INDEX(appointment.i360__Appt_Set_On__c, 'T', 1) AS DATE), 
             appointment.supportworks__Product_Category_1__c, i360__Source_Type__c
), 
appointmentcounts_canceled AS (
    SELECT CAST(SUBSTRING_INDEX(appointment.supportworks__Canceled_On__c, 'T', 1) AS DATE) AS date, 
           appointment.supportworks__Product_Category_1__c AS prod_cat, 
           i360__Source_Type__c as source_type,
           0 AS Leads_Taken, 
           0 AS Leads_nd, 
           0 AS appt_set, 
           SUM(
               CASE 
                   WHEN COALESCE(appointment.i360__Type__c, '') IN ('New', 'Reset', 'Radon CRM Pickup') 
                    AND appointment.i360__Canceled__c = 1 
                   THEN 1 
                   ELSE 0 
               END
           ) AS appt_cancelled, 
           0 AS quote, 
           0 AS sold, 
           0 AS cancelled
    FROM appointment
    WHERE appointment.supportworks__Canceled_On__c IS NOT NULL 
      AND COALESCE(appointment.supportworks__Product_Category_1__c, '') <> 'service work' 
      AND COALESCE(appointment.i360__Source_Type__c, '') <> 'Existing Database' 
    GROUP BY CAST(SUBSTRING_INDEX(appointment.supportworks__Canceled_On__c, 'T', 1) AS DATE), 
             appointment.supportworks__Product_Category_1__c, i360__Source_Type__c
),
combined_results AS (
SELECT * FROM leadcounts
UNION ALL
SELECT * FROM lead_nd
UNION ALL
SELECT * FROM appointmentcounts_set
UNION ALL
SELECT * FROM appointmentcounts_canceled
), 
datedimension AS (
    SELECT date_dimension.Date AS date 
    FROM pre_stage.date_dimension 
    WHERE date_dimension.Date > '2018-01-01' 
      AND date_dimension.Date <= CURDATE()
)
SELECT d.date AS date, 
       c.prod_cat AS product_category, 
        c.source_type as source_type,
       COALESCE(SUM(c.Leads_Taken), 0) AS Leads_Taken, 
       COALESCE(SUM(c.Leads_nd), 0) AS Leads_nd, 
       COALESCE(SUM(c.appt_set), 0) AS appointments_set, 
       COALESCE(SUM(c.appt_cancelled), 0) AS appointments_cancelled, 
       COALESCE(SUM(c.appt_set - c.appt_cancelled), 0) AS appointments_issued
FROM datedimension d
LEFT JOIN combined_results c ON d.date = c.date
GROUP BY 1 , c.prod_cat, c.source_type
ORDER BY 1 DESC;

END$$
DELIMITER ;
