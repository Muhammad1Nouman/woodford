DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `staff_location`()
BEGIN
WITH leadcounts AS (
    SELECT
        DATE(ls.i360__Taken_On__c) AS date,
        '' AS Staff,
        COALESCE(ls.supportworks__Product_Category_1__c, '') AS prod_cat,
        CAST(NULL AS CHAR) AS city,
        CAST(NULL AS CHAR) AS state,
        CAST(NULL AS CHAR) AS zip,
        COUNT(1) AS Leads_Taken,
        0 AS Leads_nd,
        0 AS appt_set,
        0 AS appt_cancelled,
        0 AS issued_sale,
        0 AS quote,
        0 AS sold,
        0 AS cancelled,
        '' AS Location,
        CAST(NULL AS CHAR(255)) AS i360__Position_Title__c 
    FROM lead_source ls
    WHERE DATE(ls.i360__Taken_On__c) <= CURDATE()
      AND COALESCE(ls.i360__Source_Type__c, '') <> 'Existing Database'
    GROUP BY DATE(ls.i360__Taken_On__c), Staff, prod_cat, city, state, zip
),

lead_nd AS (
    SELECT
        DATE(ls.i360__Taken_On__c) AS date,
        '' AS Staff,
        COALESCE(ls.supportworks__Product_Category_1__c, '') AS prod_cat,
        CAST(NULL AS CHAR) AS city,
        CAST(NULL AS CHAR) AS state,
        CAST(NULL AS CHAR) AS zip,
        0 AS Leads_Taken,
        COUNT(1) AS Leads_nd,
        0 AS appt_set,
        0 AS appt_cancelled,
        0 AS issued_sale,
        0 AS quote,
        0 AS sold,
        0 AS cancelled,
        '' AS Location,
        CAST(NULL AS CHAR(255)) AS i360__Position_Title__c 
    FROM lead_source ls
    LEFT JOIN prospect p ON ls.i360__Prospect__c = p.id
    WHERE NOT EXISTS (SELECT 1 FROM prospect sub_p WHERE sub_p.id = ls.i360__Prospect__c)
      AND COALESCE(ls.i360__Source_Type__c, '') <> 'Existing Database'
      AND COALESCE(ls.supportworks__Product_Category_1__c, '') <> 'service work'
    GROUP BY DATE(ls.i360__Taken_On__c), Staff, prod_cat, city, state, zip
),

appointmentcounts_set AS (
    SELECT
        CAST(SUBSTRING_INDEX(a.i360__Appt_Set_On__c, 'T', 1) AS DATE) AS date,
        TRIM(LOWER(a.Sales_Rep_Name__c)) AS Staff,
        COALESCE(a.supportworks__Product_Category_1__c, '') AS prod_cat,
        COALESCE(a.i360__City__c, '') AS city,
        COALESCE(a.i360__State__c, '') AS state,
        COALESCE(a.i360__Zip__c, '') AS zip,
        0 AS Leads_Taken,
        0 AS Leads_nd,
        SUM(CASE WHEN a.i360__Type__c IN ('New', 'Reset', 'Radon CRM pickup')
                 AND a.i360__Appt_Set_By__c IS NOT NULL THEN 1 ELSE 0 END) AS appt_set,
        0 AS appt_cancelled,
        0 AS issued_sale,
        0 AS quote,
        0 AS sold,
        0 AS cancelled,
        COALESCE(a.i360__Location_Address__c, '') AS Location,
        CAST(NULL AS CHAR(255)) AS i360__Position_Title__c 
    FROM pre_stage.appointment a
    WHERE COALESCE(a.supportworks__Product_Category_1__c, '') <> 'service work'
      AND COALESCE(a.i360__Source_Type__c, '') <> 'Existing Database'
    GROUP BY date, Staff, prod_cat, city, state, zip, Location
),

appointmentcounts_canceled AS (
    SELECT
        CAST(SUBSTRING_INDEX(a.supportworks__Canceled_On__c, 'T', 1) AS DATE) AS date,
        TRIM(LOWER(a.Sales_Rep_Name__c)) AS Staff,
        COALESCE(a.supportworks__Product_Category_1__c, '') AS prod_cat,
        COALESCE(a.i360__City__c, '') AS city,
        COALESCE(a.i360__State__c, '') AS state,
        COALESCE(a.i360__Zip__c, '') AS zip,
        0 AS Leads_Taken,
        0 AS Leads_nd,
        0 AS appt_set,
        SUM(CASE WHEN COALESCE(a.i360__Type__c, '') IN ('New', 'Reset', 'Radon CRM pickup')
                 AND a.i360__Canceled__c = 1 THEN 1 ELSE 0 END) AS appt_cancelled,
        0 AS issued_sale,
        0 AS quote,
        0 AS sold,
        0 AS cancelled,
        COALESCE(a.i360__Location_Address__c, '') AS Location,
        CAST(NULL AS CHAR(255)) AS i360__Position_Title__c 
    FROM pre_stage.appointment a
    WHERE a.supportworks__Canceled_On__c IS NOT NULL
      AND COALESCE(a.supportworks__Product_Category_1__c, '') <> 'service work'
      AND COALESCE(a.i360__Source_Type__c, '') <> 'Existing Database'
    GROUP BY date, Staff, prod_cat, city, state, zip, Location
),

quote AS (
    SELECT
        CAST(a.i360__Start__c AS DATE) AS date,
        TRIM(LOWER(a.Sales_Rep_Name__c)) AS Staff,
        COALESCE(a.supportworks__Product_Category_1__c, '') AS prod_cat,
        COALESCE(a.i360__City__c, '') AS city,
        COALESCE(a.i360__State__c, '') AS state,
        COALESCE(a.i360__Zip__c, '') AS zip,
        0 AS Leads_Taken,
        0 AS Leads_nd,
        0 AS appt_set,
        0 AS appt_cancelled,
        0 AS issued_sale,
        SUM(CASE WHEN LOWER(a.i360__Type__c) IN ('new','reset','rehash','radon crm pickup')
                 THEN COALESCE(a.supportworks__Quoted__c,0) ELSE 0 END) AS quote,
        0 AS sold,
        0 AS cancelled,
        COALESCE(a.i360__Location_Address__c, '') AS Location,
        CAST(NULL AS CHAR(255)) AS i360__Position_Title__c 
    FROM pre_stage.appointment a
    GROUP BY date, Staff, prod_cat, city, state, zip, Location
),

issued_sale AS (
    SELECT
        CAST(a.i360__Start__c AS DATE) AS date,
        TRIM(LOWER(a.Sales_Rep_Name__c)) AS Staff,
        COALESCE(a.supportworks__Product_Category_1__c, '') AS prod_cat,
        COALESCE(a.i360__City__c, '') AS city,
        COALESCE(a.i360__State__c, '') AS state,
        COALESCE(a.i360__Zip__c, '') AS zip,
        0 AS Leads_Taken,
        0 AS Leads_nd,
        0 AS appt_set,
        0 AS appt_cancelled,
        SUM(CASE
            WHEN NOT (COALESCE(a.supportworks__Product_Category_1__c, '') LIKE '%service%')
                 AND LOWER(COALESCE(a.i360__Source_Type__c, '')) NOT IN ('annual maintenance','service','service opportunity','existing database')
                 AND LOWER(COALESCE(a.i360__Type__c, '')) IN ('new','reset','rehash','radon crm pickup')
            THEN COALESCE(a.i360__Issue1__c,0) ELSE 0 END) AS issued_sale,
        0 AS quote,
        0 AS sold,
        0 AS cancelled,
        COALESCE(a.i360__Location_Address__c, '') AS Location,
        CAST(NULL AS CHAR(255)) AS i360__Position_Title__c 
    FROM pre_stage.appointment a
    GROUP BY date, Staff, prod_cat, city, state, zip, Location
),

datedimension AS (
    SELECT dd.Date AS date
    FROM pre_stage.date_dimension dd
    WHERE dd.Date > '2018-01-01'
      AND dd.Date <= CURDATE()
),

sold_sale AS (
    SELECT
        CAST(s.i360__Sold_On__c AS DATE) AS date,
        TRIM(LOWER(s.Sales_Rep_1_Name__c)) AS Staff,
        COALESCE(s.Prod_Cat_1_Sale_Shrt_List__c, '') AS prod_cat,
        COALESCE(s.i360__Appointment_City__c, '') AS city,
        COALESCE(s.i360__Appointment_State__c, '') AS state,
        COALESCE(s.i360__Appointment_Zip__c, '') AS zip,
        0 AS Leads_Taken,
        0 AS Leads_nd,
        0 AS appt_set,
        0 AS appt_cancelled,
        0 AS issued_sale,
        0 AS quote,
        SUM(COALESCE(s.Sold__c,0)) AS sold,
        0 AS cancelled,
        COALESCE(s.supportworks__Location_Address__c, '') AS Location,
        CAST(NULL AS CHAR(255)) AS i360__Position_Title__c 
    FROM pre_stage.sale s
    WHERE s.i360__Appointment_Type__c IN ('new','rehash','reset','radon crm pickup')
    GROUP BY date, Staff, prod_cat, city, state, zip, Location
),

canceled_sale AS (
    SELECT
        CAST(s.i360__Canceled_Date__c AS DATE) AS date,
        TRIM(LOWER(s.Sales_Rep_1_Name__c)) AS Staff,
        COALESCE(s.Prod_Cat_1_Sale_Shrt_List__c, '') AS prod_cat,
        COALESCE(s.i360__Appointment_City__c, '') AS city,
        COALESCE(s.i360__Appointment_State__c, '') AS state,
        COALESCE(s.i360__Appointment_Zip__c, '') AS zip,
        0 AS Leads_Taken,
        0 AS Leads_nd,
        0 AS appt_set,
        0 AS appt_cancelled,
        0 AS issued_sale,
        0 AS quote,
        0 AS sold,
        SUM(COALESCE(s.i360__Canceled__c,0)) AS cancelled,
        COALESCE(s.supportworks__Location_Address__c, '') AS Location,
        CAST(NULL AS CHAR(255)) AS i360__Position_Title__c 
    FROM pre_stage.sale s
    WHERE s.i360__Appointment_Type__c IN ('new','rehash','reset','radon crm pickup')
    GROUP BY date, Staff, prod_cat, city, state, zip, Location
),

Project AS (
    SELECT 
        CAST(p.supportworks__Sold_On__c AS DATE) AS date,
        TRIM(LOWER(p.i360__Sale_Rep__c)) AS Staff,
        COALESCE(p.Prod_Cat_1_Proj_Shrt_List__c, '') AS prod_cat,
        COALESCE(p.i360__Appointment_City__c, '') AS city,
        COALESCE(p.i360__Appointment_State__c, '') AS state,
        COALESCE(p.i360__Appointment_Zip__c, '') AS zip,
        0 AS Leads_Taken,
        0 AS Leads_nd,
        0 AS appt_set,
        0 AS appt_cancelled,
        0 AS issued_sale,
        0 AS quote,
        0 AS sold,
        0 AS cancelled,
        COALESCE(p.supportworks__Location_Address__c, '') AS Location,
        CAST(NULL AS CHAR(255)) AS i360__Position_Title__c
    FROM pre_stage.project p
),

Staff as (
			Select 
		DATE(i360__Start__c) AS date,
        '' AS Staff,
        null AS prod_cat,
        CAST(NULL AS CHAR) AS city,
		CAST(NULL AS CHAR) AS state,
		CAST(NULL AS CHAR) AS zip,
       null as Leads_Taken,
        0 AS Leads_nd,
        0 AS appt_set,
        0 AS appt_cancelled,
        0 AS issued_sale,
        0 AS quote,
        0 AS sold,
        0 AS cancelled,
        CAST(NULL AS CHAR) as Location ,
        i360__Position_Title__c as i360__Position_Title__c 
        from staff
),

combined_results AS (
    SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM leadcounts
    UNION ALL
    SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM lead_nd
    UNION ALL
    SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM appointmentcounts_set
    UNION ALL
    SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM appointmentcounts_canceled
    UNION ALL
    SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM issued_sale
    UNION ALL
    SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM quote
    UNION ALL
    SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM sold_sale
    UNION ALL
    SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM canceled_sale
    UNION ALL
     SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM Project
    UNION ALL
    SELECT date, Staff, prod_cat, city, state, zip,
           Leads_Taken, Leads_nd, appt_set, appt_cancelled, issued_sale, quote, sold, cancelled,Location,i360__Position_Title__c
    FROM staff
)

SELECT 
    d.date AS date,
    c.Staff AS staff,
    c.prod_cat AS product_category,
    c.city AS city,
    c.state AS state,
    c.zip AS zip,
    SUM(c.Leads_Taken) AS leads_taken,
    SUM(c.Leads_nd) AS leads_nd,
    SUM(c.appt_set) AS appointments_set,
    SUM(c.appt_cancelled) AS appointments_cancelled,
    SUM(c.appt_set - c.appt_cancelled) AS appointments_issued_cc,
    SUM(c.issued_sale) AS appointments_issued_sale,
    SUM(c.quote) AS quote_issued,
    SUM(c.sold) AS sale_sold,
    SUM(c.cancelled) AS sale_cancelled,
    c.Location AS Location,
    c.i360__Position_Title__c as i360__Position_Title__c
FROM datedimension d
LEFT JOIN combined_results c ON d.date = c.date
GROUP BY d.date, c.Staff, c.prod_cat, c.city, c.state, c.zip,c.Location,c.i360__Position_Title__c
ORDER BY d.date DESC;

END$$
DELIMITER ;
