DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `marketing_cost_by_src`()
BEGIN
WITH source_cost AS (
    SELECT 
        i360__Amount__c AS total_cost,
        supportworks__Product_Category__c AS product_category,
        i360__Paid_On__c AS date,
        Name AS Marketing_source
    FROM pre_stage.source_cost
),
marketing_source AS (
    SELECT id FROM pre_stage.marketing_source
),
DateDimension AS (
    SELECT date
    FROM pre_stage.date_dimension
    WHERE date > '2018-01-01' AND date <= CURRENT_DATE
),
marketing AS (
    SELECT 
        sc.date,
        sc.Marketing_source as Source_id,
        sc.product_category,
        0 as Leads_nd,
        SUM(sc.total_cost) AS Cost
    FROM source_cost sc
    LEFT JOIN marketing_source t ON t.id = sc.Marketing_source
    GROUP BY 1,2,3
),


combined AS (
    SELECT 
        date,
        Source_id,
        product_category,

        SUM(Cost) OVER (PARTITION BY date, Source_id) AS Cost
    FROM (
        SELECT date, Source_id, product_category, Cost FROM marketing
    ) subquery
)


SELECT  
    cd.date AS date,
	c.Source_id, 
    c.product_category,
    COALESCE(SUM(c.Cost), 0) AS Cost
FROM DateDimension cd
LEFT JOIN combined c ON cd.date = c.date  
GROUP BY 1,2,3;
END$$
DELIMITER ;
