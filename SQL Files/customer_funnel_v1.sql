DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `customer_funnel_v1`()
BEGIN
WITH YearlyGoals AS (
    SELECT 
        YEAR(gd.date) AS year,
        SUM(gd.goal_Leads_all) AS total_goal_Leads_all,
        SUM(gd.goal_Leads_Taken) AS total_goal_Leads_Taken,
        SUM(gd.goal_Leads_ND) AS total_goal_Leads_ND,
        SUM(gd.goal_Appointments_Set) AS total_goal_Appointments_Set,
        SUM(gd.goal_Appointments_Cancelled) AS total_goal_Appointments_Cancelled,
        SUM(gd.goal_Appointments_Issued) AS total_goal_Appointments_Issued,
        SUM(gd.goal_Quote_Issued) AS total_goal_Quote_Issued,
        SUM(gd.goal_Sale_Sold) AS total_goal_Sale_Sold,
        SUM(gd.goal_Sale_Cancelled) AS total_goal_Sale_Cancelled,
        SUM(gd.goal_Marketing_Expenses) AS total_goal_Marketing_Expenses
    FROM 
        goal_by_date gd
    GROUP BY 
        YEAR(gd.date)
),
MonthlyPercentages AS (
    SELECT 
        gd.date,
        cf.Leads_all, 
        gd.goal_Leads_all, 
        (gd.goal_Leads_all / yg.total_goal_Leads_all) * 100 AS pct_Leads_all,
        
        cf.Leads_Taken, 
        gd.goal_Leads_Taken, 
        (gd.goal_Leads_Taken / yg.total_goal_Leads_Taken) * 100 AS pct_Leads_Taken,
        
        cf.Leads_ND, 
        gd.goal_Leads_ND, 
        (gd.goal_Leads_ND / yg.total_goal_Leads_ND) * 100 AS pct_Leads_ND,
        
        cf.Appointments_Set, 
        gd.goal_Appointments_Set, 
        (gd.goal_Appointments_Set / yg.total_goal_Appointments_Set) * 100 AS pct_Appointments_Set,
        
        cf.Appointments_Cancelled, 
        gd.goal_Appointments_Cancelled, 
        (gd.goal_Appointments_Cancelled / yg.total_goal_Appointments_Cancelled) * 100 AS pct_Appointments_Cancelled,
        
        cf.Appointments_Issued, 
        gd.goal_Appointments_Issued, 
        (gd.goal_Appointments_Issued / yg.total_goal_Appointments_Issued) * 100 AS pct_Appointments_Issued,
        
        cf.Quote_Issued, 
        gd.goal_Quote_Issued, 
        (gd.goal_Quote_Issued / yg.total_goal_Quote_Issued) * 100 AS pct_Quote_Issued,
        
        cf.Sale_Sold, 
        gd.goal_Sale_Sold, 
        (gd.goal_Sale_Sold / yg.total_goal_Sale_Sold) * 100 AS pct_Sale_Sold,
        
        cf.Sale_Cancelled, 
        gd.goal_Sale_Cancelled, 
        (gd.goal_Sale_Cancelled / yg.total_goal_Sale_Cancelled) * 100 AS pct_Sale_Cancelled,
        
        cf.Marketing_Expenses, 
        gd.goal_Marketing_Expenses, 
        (gd.goal_Marketing_Expenses / yg.total_goal_Marketing_Expenses) * 100 AS pct_Marketing_Expenses
    FROM 
        customer_funnel cf
    LEFT JOIN 
        goal_by_date gd 
    ON 
        cf.date = gd.date
    LEFT JOIN 
        YearlyGoals yg 
    ON 
        YEAR(gd.date) = yg.year
)
SELECT * FROM MonthlyPercentages;
END$$
DELIMITER ;
