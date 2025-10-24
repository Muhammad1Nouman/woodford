DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `wbi_goal_data`()
BEGIN
SELECT 
    a.date,
    a.leads_nd,
    @step1 := a.leads_nd * (b.Leads_nd / 100) AS "Raw > Taken",
    @step2 := @step1 * (b.appointment_set / 100) AS "Taken (ND) > Set ",
    @step3 := @step2 * (b.appointment_canceled / 100) AS "Set > Canceled",
    @step4 := @step3 * (b.appointment_issued / 100) AS "Set > Issued",
    @step5 := @step4 * (b.sales_issued / 100) AS "Set > Issued(sale)",
    @step6 := @step5 * (b.Quoted / 100) AS "Issued > Quoted",
    @step7 := @step6 * (b.sale_sold / 100) AS "Quoted > Sold",
    @step8 := @step7 * (b.sale_cancelled / 100) AS "Sold > Canceled"
FROM stage.customer_funnel a
CROSS JOIN pre_stage.yearly_goals b
WHERE YEAR(a.date) = 2025
AND b.type = "wbi_goal";
END$$
DELIMITER ;
