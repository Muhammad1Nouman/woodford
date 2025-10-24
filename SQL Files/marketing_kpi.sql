DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `marketing_kpi`()
BEGIN
WITH source_cost AS (
    SELECT 
        i360__Amount__c AS total_cost,
        i360__Paid_On__c AS date,
        i360__Marketing_Source__c AS Marketing_source
    FROM 
        pre_stage.source_cost  -- Corrected source
),

marketing_source AS (
    SELECT 
        id,
        i360__Type__c AS source_type
    FROM 
        pre_stage.marketing_source  -- Corrected source
),

DateDimension AS (
    SELECT 
        date
    FROM 
        pre_stage.date_dimension  -- Corrected source
    WHERE 
        date > '2018-01-01' 
        AND date <= CURRENT_DATE
),

marketing AS (
    SELECT 
        sc.date,
        t.source_type,
        SUM(sc.total_cost) AS Cost
    FROM 
        source_cost sc
    LEFT JOIN 
        marketing_source t ON t.id = sc.Marketing_source
    GROUP BY 
        sc.date, t.source_type
),

prospect AS (
    SELECT 
        prospect.Id AS id
    FROM 
        pre_stage.prospect
    WHERE 
        prospect.i360__Not_Qualified_Reason__c LIKE '%Bad or Old Data%' 
        OR prospect.i360__Not_Qualified_Reason__c LIKE '%Spam%' 
        OR prospect.i360__Not_Qualified_Reason__c LIKE '%Not the type of work we do%' 
        OR prospect.i360__Not_Qualified_Reason__c LIKE '%Out of Area%'
),

lead_nd AS (
    SELECT 
        ls.i360__Taken_On__c AS date,
        ls.i360__Source_Type__c AS source_type,
    --    ls.supportworks__Product_Category_1__c AS product_category,
        COUNT(*) AS Leads_nd
    FROM 
        pre_stage.lead_source ls
    LEFT JOIN 
        prospect p ON ls.i360__Prospect__c = p.id
    WHERE 
        NOT EXISTS (
            SELECT 1 
            FROM prospect sub_p 
            WHERE sub_p.id = ls.i360__Prospect__c
        )
        AND COALESCE(ls.i360__Source_Type__c, '') <> 'Existing Database'
        AND COALESCE(ls.supportworks__Product_Category_1__c, '') <> 'service work'
    GROUP BY 
        ls.i360__Taken_On__c, 
        ls.i360__Source_Type__c
   --     ls.supportworks__Product_Category_1__c
),

final_data AS (
    SELECT 
        d.date,
        COALESCE(l.source_type, m.source_type) AS source_type,
      --  COALESCE(l.product_category, '') AS product_category,
        COALESCE(l.Leads_nd, 0) AS Leads,
        COALESCE(m.Cost, 0) AS Expenditure
    FROM 
        DateDimension d
    LEFT JOIN 
        lead_nd l ON d.date = l.date
    LEFT JOIN 
        marketing m ON d.date = m.date AND l.source_type = m.source_type

    UNION ALL

    SELECT 
        d.date,
        m.source_type,
     --   '' AS product_category,
        0 AS Leads,
        m.Cost
    FROM 
        DateDimension d
    RIGHT JOIN 
        marketing m ON d.date = m.date
    LEFT JOIN 
        lead_nd l ON d.date = l.date AND m.source_type = l.source_type
    WHERE 
        l.date IS NULL
)

SELECT 
    date AS date,
    source_type,
  --  product_category,
    Leads AS leads_nd,
    Expenditure AS Total_Cost
FROM 
    final_data;


END$$
DELIMITER ;
