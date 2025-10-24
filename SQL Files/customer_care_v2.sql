DELIMITER $$
CREATE DEFINER=`pbiRefresh`@`%` PROCEDURE `customer_care_v2`()
BEGIN
-- 1. Filter appointments
DROP TEMPORARY TABLE IF EXISTS date_filtered_appointments;
CREATE TEMPORARY TABLE date_filtered_appointments AS
SELECT *
FROM pre_stage.appointment
WHERE i360__Lead_Source__c IS NOT NULL
  AND CAST(SUBSTRING_INDEX(i360__Appt_Set_On__c, 'T', 1) AS DATE)
      BETWEEN DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND CURDATE();

-- 2. Get first appointment per lead
DROP TEMPORARY TABLE IF EXISTS first_appt_per_lead;
CREATE TEMPORARY TABLE first_appt_per_lead AS
SELECT 
    i360__Lead_Source__c AS lead_id,
    i360__Appt_Set_By__c AS staff_id
FROM (
    SELECT 
        i360__Lead_Source__c,
        i360__Appt_Set_By__c,
        ROW_NUMBER() OVER (PARTITION BY i360__Lead_Source__c ORDER BY i360__Appt_Set_On__c) AS rn
    FROM date_filtered_appointments
) t
WHERE rn = 1;

-- 3. Lead counts
DROP TEMPORARY TABLE IF EXISTS leadcounts;
CREATE TEMPORARY TABLE leadcounts AS
SELECT 
    l.i360__Taken_On__c AS date, 
    COALESCE(s.name, 'Unassigned') AS name,
    s.Team_Leader_Appointments__c AS Team_Leader_Appointment__c,
    COALESCE(l.supportworks__Product_Category_1__c, '') AS prod_cat, 
    COALESCE(l.i360__Source_Type__c, '') AS source_type,
    COUNT(*) AS Leads_Taken, 
    0 AS Leads_nd, 
    0 AS appt_set, 
    0 AS appt_cancelled
FROM pre_stage.lead_source l
LEFT JOIN first_appt_per_lead fap ON l.id = fap.lead_id
LEFT JOIN pre_stage.staff s ON fap.staff_id = s.id
WHERE l.i360__Taken_On__c IS NOT NULL
  AND l.i360__Taken_On__c BETWEEN DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND CURDATE()
  AND l.i360__Source_Type__c <> 'Existing Database'
GROUP BY 1, 2, 3, 4, 5;

-- 4. Disqualified prospects
DROP TEMPORARY TABLE IF EXISTS disqualified_prospects;
CREATE TEMPORARY TABLE disqualified_prospects AS
SELECT id 
FROM prospect 
WHERE i360__Not_Qualified_Reason__c LIKE '%Bad or Old Data%' 
   OR i360__Not_Qualified_Reason__c LIKE '%Spam%' 
   OR i360__Not_Qualified_Reason__c LIKE '%Not the type of work we do%' 
   OR i360__Not_Qualified_Reason__c LIKE '%Out of Area%';

-- 5. Leads ND
DROP TEMPORARY TABLE IF EXISTS lead_nd;
CREATE TEMPORARY TABLE lead_nd AS
SELECT 
    ls.i360__Taken_On__c AS date,
    COALESCE(s.name, 'Unassigned') AS name,
    s.Team_Leader_Appointments__c AS Team_Leader_Appointment__c,
    COALESCE(ls.supportworks__Product_Category_1__c, '') AS prod_cat,
    COALESCE(ls.i360__Source_Type__c, '') AS source_type,
    0 AS Leads_Taken,
    COUNT(*) AS Leads_nd,
    0 AS appt_set,
    0 AS appt_cancelled
FROM pre_stage.lead_source ls
LEFT JOIN disqualified_prospects p ON ls.i360__Prospect__c = p.id
LEFT JOIN first_appt_per_lead fap ON ls.id = fap.lead_id
LEFT JOIN pre_stage.staff s ON fap.staff_id = s.id
WHERE p.id IS NULL
  AND ls.i360__Source_Type__c <> 'Existing Database' 
  AND COALESCE(ls.supportworks__Product_Category_1__c, '') <> 'service work'
  AND ls.i360__Taken_On__c IS NOT NULL
  AND ls.i360__Taken_On__c BETWEEN DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND CURDATE()
GROUP BY 1, 2, 3, 4, 5;

-- 6. Appointment Set Counts
DROP TEMPORARY TABLE IF EXISTS appointmentcounts_set;
CREATE TEMPORARY TABLE appointmentcounts_set AS
SELECT 
    CAST(SUBSTRING_INDEX(a.i360__Appt_Set_On__c, 'T', 1) AS DATE) AS date, 
    COALESCE(s.name, 'Unassigned') AS name,
    a.Team_Leader_Appointment__c AS Team_Leader_Appointment__c,
    COALESCE(a.supportworks__Product_Category_1__c, '') AS prod_cat,
    COALESCE(a.i360__Source_Type__c, '') AS source_type,
    0 AS Leads_Taken,
    0 AS Leads_nd,
    SUM(CASE 
            WHEN COALESCE(a.i360__Type__c, '') IN ('New', 'Reset', 'Radon CRM Pickup') 
            AND a.i360__Appt_Set_By__c IS NOT NULL 
            THEN 1 ELSE 0 
        END) AS appt_set,
    0 AS appt_cancelled
FROM date_filtered_appointments a
LEFT JOIN pre_stage.staff s ON a.i360__Appt_Set_By__c = s.id
WHERE COALESCE(a.supportworks__Product_Category_1__c, '') <> 'service work'
  AND a.i360__Source_Type__c <> 'Existing Database'
GROUP BY 1, 2, 3, 4, 5;

-- 7. Appointment Canceled Counts
DROP TEMPORARY TABLE IF EXISTS appointmentcounts_canceled;
CREATE TEMPORARY TABLE appointmentcounts_canceled AS
SELECT 
    CAST(SUBSTRING_INDEX(a.supportworks__Canceled_On__c, 'T', 1) AS DATE) AS date, 
    COALESCE(s.name, 'Unassigned') AS name,
    a.Team_Leader_Appointment__c AS Team_Leader_Appointment__c,
    COALESCE(a.supportworks__Product_Category_1__c, '') AS prod_cat,
    COALESCE(a.i360__Source_Type__c, '') AS source_type,
    0 AS Leads_Taken,
    0 AS Leads_nd,
    0 AS appt_set,
    SUM(CASE 
            WHEN COALESCE(a.i360__Type__c, '') IN ('New', 'Reset', 'Radon CRM Pickup') 
            AND a.i360__Canceled__c = 1 
            THEN 1 ELSE 0 
        END) AS appt_cancelled
FROM pre_stage.appointment a
LEFT JOIN pre_stage.staff s ON a.i360__Appt_Set_By__c = s.id
WHERE a.supportworks__Canceled_On__c IS NOT NULL
  AND COALESCE(a.supportworks__Product_Category_1__c, '') <> 'service work'
  AND COALESCE(a.i360__Source_Type__c, '') <> 'Existing Database'
  AND CAST(SUBSTRING_INDEX(a.supportworks__Canceled_On__c, 'T', 1) AS DATE)
      BETWEEN DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND CURDATE()
GROUP BY 1, 2, 3, 4, 5;

-- 8. Combine all
DROP TEMPORARY TABLE IF EXISTS combined_results;
CREATE TEMPORARY TABLE combined_results AS
SELECT * FROM leadcounts
UNION ALL
SELECT * FROM lead_nd
UNION ALL
SELECT * FROM appointmentcounts_set
UNION ALL
SELECT * FROM appointmentcounts_canceled;

-- 9. Final Aggregation
SELECT 
  CAST(date AS DATE) AS date,
  name,
  Team_Leader_Appointment__c,
  prod_cat AS product_category,
  source_type,
  SUM(Leads_Taken) AS Leads_Taken,
  SUM(Leads_nd) AS Leads_nd,
  SUM(appt_set) AS appointments_set,
  SUM(appt_cancelled) AS appointments_cancelled,
  SUM(appt_set) - SUM(appt_cancelled) AS appointments_issued
FROM combined_results
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC;

END$$
DELIMITER ;
