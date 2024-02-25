raw_query = """
with  OverallRequiredSalesData AS (
                SELECT
                "gastronomic_day" as "period",
                company,
	  			restaurant,
					SUM(
					  COALESCE(
						CASE
						  WHEN "cost" = 'NaN' THEN 0
						  ELSE "cost"
						END,
					  0)
					) AS "cost",
	          SUM(CASE WHEN "user_name" = 'nS3 Deliverect' THEN COALESCE("total_gross",0) * 0.25 ELSE 0 END) as "delivery_cost",
	              SUM("total_net") as "total_net"
        FROM public."SalesData"
	  group by 1,2,3
    ),
	OverallRequiredPredictionData as (
	select
	date,
	company,
	restaurant,
	total_gross as "predicted_sales",
	        ROW_NUMBER() OVER (PARTITION BY company, restaurant,date ORDER BY created_at DESC) AS rn
		from public."Predictions_predictions"
-- 		where date>= '2024-02-01'
--         and date<= '2024-02-29'
--         and company= 'Los Tacos'
	)
	,
	MonthlyTotalSales as (
	select
		orpd."date" as period,
		orpd.company,
		orpd.restaurant,
		COALESCE(total_net,predicted_sales) as "daily_sale"
		from OverallRequiredPredictionData orpd
		left join OverallRequiredSalesData orsd  on
			orsd.period=orpd.date
			and orsd.restaurant=orpd.restaurant
			and orsd.company=orsd.company
		where orpd.rn=1
	),
	MonthlyAggregation as (
		select
            TO_CHAR("period", 'YYYY MM') AS month_year,
			company,
			restaurant,
			SUM("daily_sale") as "monthly_sale"
		from MonthlyTotalSales
		group by 1,2,3
		order by 1
	),
----Monthly Total Sales
  FilteredSalesData as (
  select
	  period,
	  company,
	  restaurant,
	  SUM("cost") as "cost",
	  SUM("delivery_cost") as "delivery_cost",
	  SUM("total_net") as "total_net"
	  from OverallRequiredSalesData
	  group by 1,2,3	
	  
  ),
  EmployeeCosts as (
  select
  	date,
  	company,
	restaurant,
  	SUM(COALESCE("employee_cost",0)) as "employee_cost"
  from public."Predictions_employeecostandhoursinfo"
--   where
--   	 date>= '2024-02-01'
--         and date<= '2024-02-29'
--         and company= 'Los Tacos'
  group by 1,2,3),
  CompanyNames as (
  select
  	id,
	 name
	  from accounts_company
-- 	  where
-- 	  name='Los Tacos'
  ),
  RestaurantTemp as (
  select
	cn.name as "company",
	ar.name as "restaurant",
    ar.id as "restaurant_id"
  from CompanyNames cn
  left join public."accounts_restaurant" ar on
  	cn.id = ar.company_id
  ),
    RentCosts as (
  select
	cn.company,
	cn.restaurant,
    start_date,
    end_date,
    created_at,
  	(COALESCE("minimum_rent",0)) as "rent",
	(COALESCE("rent_variable_sum",0)) as "variable_rent",
    (COALESCE("fixed_costs",0)) as fixed_cost
  from RestaurantTemp cn
  left join public."accounts_restaurantcosts" ar on
  	cn.restaurant_id = ar.restaurant_id
  ),
  ActualRent as (
  select
    fsd.period,
	fsd.company,
	fsd.restaurant,
	fsd."cost",
	fsd."delivery_cost",
	fsd."total_net",
      r.fixed_cost/30.5 as fixed_cost,
	  case
	  	when r."rent" > COALESCE(ma."monthly_sale")*COALESCE(r."variable_rent")/100
	  	then
	  		COALESCE(r."rent",0)/30.5
	  	else
			COALESCE(ma."monthly_sale")*COALESCE(r."variable_rent")/(100*30.5)	
	  end as rent,
      ROW_NUMBER() over (partition by fsd.period,fsd.company,fsd.restaurant order by r.created_at desc) as rn
	  from FilteredSalesData fsd
	  left join RentCosts r on fsd."company"=r."company" and r.restaurant=fsd.restaurant and fsd.period between r.start_date and r.end_date
	  left join  MonthlyAggregation ma on TO_CHAR(fsd."period", 'YYYY MM') = ma."month_year"
	  and fsd."company"=ma."company" and fsd.restaurant=ma.restaurant
  ),
  TempData as (
	select
	ar.period,
	ar.company,
	ar.restaurant,
	ar."cost",
	ar."delivery_cost",
	ar."total_net",
    COALESCE(ar."rent",0) as rent,
    COALESCE(ar.fixed_cost,0) as fixed_cost,
    COALESCE(ec."employee_cost",0) as "employee_cost",
    ar."total_net" - ar."cost" as "gross_profit",
    ar."total_net" - ar."cost" - ar."delivery_cost" - COALESCE(ar."rent",0) - COALESCE(ec."employee_cost",0)-COALESCE(ar.fixed_cost,0) as "net_profit",
    CASE 
      WHEN ar."total_net" > 0 THEN (ar."total_net" - ar."cost")*100 / NULLIF(ar."total_net", 0)
      ELSE 0 
    END as "gross_profit_percentage",
    CASE 
      WHEN ar."total_net" > 0 THEN (ar."total_net"- ar."cost"-ar."delivery_cost" - COALESCE(ar."rent",0) - COALESCE(ec."employee_cost",0)-COALESCE(ar.fixed_cost,0))*100 / NULLIF(ar."total_net", 0)
      ELSE 0 
    END as "net_profit_percentage"
	  from ActualRent ar
	  left join EmployeeCosts ec on
		ec."company"=ar."company"
        and ec."restaurant"=ar."restaurant"
		and ec."date" = ar."period"
    where ar.rn=1
	),
  HistoricalData as (
	select
	ar.period,
	ar.company,
	 ar.restaurant,
    SUM(ar."cost") as cost,
    SUM(ar."delivery_cost") as delivery_cost,
    SUM(ar."total_net") as total_net,
    SUM(ar."employee_cost") as employee_cost,
    SUM(ar."rent") as rent,
    SUM(ar."fixed_cost") as fixed_cost,
    SUM(ar."gross_profit") as gross_profit,
    SUM(ar."net_profit") as net_profit,
    AVG(ar."gross_profit_percentage") as gross_profit_percentage,
    AVG(ar."net_profit_percentage") as net_profit_percentage
	  from TempData ar
    group by 1,2,3
	)
INSERT INTO public."DailyHistoricalMasterTable"(
	id, gastronomic_day, company, restaurant, total_net, cost, gross_profit, delivery_cost, rent, employee_cost, fixed_cost, net_profit, gross_profit_percentage, net_profit_percentage)
	select 
			FLOOR(RANDOM() * 9223372036854775807::bigint) + 1::BIGINT,
			hd.period,
			hd.company,
			hd.restaurant,
			hd.total_net,
			hd.cost,
			hd.gross_profit,
			hd.delivery_cost,
			hd.rent,
			hd.employee_cost,
			hd.fixed_cost,
			hd.net_profit,
			hd.gross_profit_percentage,
			hd.net_profit_percentage
		from HistoricalData hd
            ON CONFLICT (gastronomic_day,restaurant,company)
    DO UPDATE SET
        total_net = EXCLUDED.total_net,
        cost = EXCLUDED.cost,
        gross_profit = EXCLUDED.gross_profit,
        delivery_cost = EXCLUDED.delivery_cost,
        rent = EXCLUDED.rent,
        employee_cost = EXCLUDED.employee_cost,
        fixed_cost = EXCLUDED.fixed_cost,
        net_profit = EXCLUDED.net_profit,
        gross_profit_percentage = EXCLUDED.gross_profit_percentage,
        net_profit_percentage = EXCLUDED.net_profit_percentage
"""
