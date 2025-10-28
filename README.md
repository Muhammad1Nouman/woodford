> **Woodford** **Bros** **Power** **BI** **Dashboard** **Documentation**

**1.** **Problem** **Statement**

The client approached us with the need to automate their data reporting
and visualization processes. Previously, their data was scattered across
Salesforce and Excel files, making it time consuming to consolidate and
analyze performance metrics. They required a solution to centralize,
transform, and visualize key business KPIs, covering sales, marketing,
production, and service in Power BI.

Our objective was to build a seamless data pipeline that automatically
pulls data from Salesforce, stores and transforms it in MySQL, and then
feeds the processed data into Power BI for advanced visualization and
analysis.

**2.** **Project** **Overview**

The project aimed to build a fully automated data pipeline integrating
Salesforce, MySQL, and Power BI to help the client visualize key
business metrics and eliminate manual reporting. Data from Salesforce
objects such as *Appointment,* *Marketing* *Source,* *Lead* *Source,*
*Prospect,* *Sale,* *Source* *Cost,* and *Project* was extracted using
Python scripts and loaded into MySQL staging tables. These scripts
handled data ingestion, validation, and updates to ensure reliable
synchronization between Salesforce and the database. In the data
ingestion goes, windows task scheduler handles the data ingestion as it
runs after every 30 minutes.

In MySQL, data transformations were performed using CTEs, views, and SQL
queries to structure analytics-ready datasets. Power BI was then
connected directly to these processed tables, where additional modeling,
DAX measures, and visualizations were applied. The final dashboard
provided real time, automated insights across key areas, marketing,
sales, production, and service, empowering the client to track
performance, analyze trends, and make informed decisions efficiently.

**3.** **System** **Architecture**

**Components:**

> **3.1** **Salesforce** **(Source** **System)**
>
> o Data extracted from various Salesforce objects such as
> *Appointment,* *Marketing* *Source,* *Lead* *Source,* *Prospect,*
> *Sale,* *Source* *Cost,* *Project*, etc.
>
> **3.2** **Python** **ETL** **Layer**
>
> o Python scripts handle **data** **ingestion,** **transformation,**
> **and** **loading** into MySQL.

<img src="./pzhuxsmh.png"
style="width:3.62069in;height:1.60069in" />

> o The process includes:
>
>  API calls to fetch Salesforce data.
>
>  Data cleaning and handling of nulls/missing values.
>
>  Batch inserts to MySQL tables.
>
>  Logging and exception handling.
>
> **3.3** **MySQL** **Database** **(Transformation** **Layer)**
>
> o Raw data from Python ETL is first stored in staging tables.
>
> o SQL queries, **CTEs,** **and** **Views** are used for
> transformations, aggregations, and joins across multiple entities.
>
> o The processed data is optimized for reporting and used as a source
> for Power BI.
>
> **3.4** **Power** **BI** **(Visualization** **Layer)**
>
> o Power BI connects to MySQL and imports tables and views.
>
> o Further data modeling and calculated columns/measures using **DAX**
> are implemented.
>
> o Dynamic dashboards were created for performance insights.
>
> **4.Data** **Flow** **Diagram**
>
> **5.** **Database** **Architecture**

The database architecture was designed using a two-layered schema
structure to ensure scalability, clarity, and data integrity throughout
the ETL process.

> • **Pre_Stage** **Schema:**
>
> This layer serves as the raw data repository, where data extracted
> from Salesforce is directly ingested using Python scripts. Each
> Salesforce object (e.g., *Appointment,* *Marketing* *Source,* *Lead*
> *Source,* *Prospect,* *Sale,* *Source* *Cost,* and *Project*) is
> mapped to a corresponding table in this schema. The data here remains
> unaltered, preserving its original form for auditability and recovery
> purposes.
>
> • **Stage** **Schema:**
>
> This layer contains the transformed and analytics ready data. Using
> SQL transformations, CTEs, and views, data from the pre_stage schema
> is cleaned, standardized, and joined to create relational models that
> align with business logic. The stage schema is directly connected to
> Power BI, allowing dynamic data refresh and efficient reporting
> through dashboards.

This architecture enables a clear separation between data ingestion and
data transformation, ensuring both data accuracy and maintainability
across the pipeline.

**6.** **Dashboard** **Overview**

Every Power BI page includes interactive date range buttons such as R12,
YTD, Today, Yesterday, Last Week to Date, This Week to Date, and R4W
(Rolling 4 Weeks), allowing users to quickly toggle between time-based
views.

Each page also includes filters for Date Range, Year, Month, Product
Category, and Foreman, ensuring consistency and interactivity across all
visuals.

**6.1** **Funnel**

> • Provides an overall summary of the customer journey from lead
> capture to sale.
>
> • Key metrics: Leads Taken, Leads ND, Appointments Set, and other
> funnel stages.
>
> • Each KPI card displays Actual, Goal, and % Achievement, with dynamic
> formatting.
>
> • Visuals include a funnel bar, line chart, and data table for
> granular analysis.
>
> • Date buttons and filters sync with visuals to allow instant
> time-period comparisons.

**6.2** **Marketing** **Details**

> • Analyzes marketing campaigns and lead sources.
>
> • Metrics: Lead conversion rates, campaign performance, cost per lead.
>
> • Visuals: Column charts for marketing source contribution, tables for
> campaign details.

**6.3** **Customer** **Care**

> • Tracks service team performance and customer follow-ups.
>
> • KPI cards include Funnel and Conversion cards with conditional color
> formatting (e.g., green for meeting goals, red for underperformance
> etc).
>
> • Visuals: Line charts for weekly trends, supporting tables for
> detailed records.
>
> • Fully interactive with synced filters and time-based buttons for
> trend comparisons.

**6.4** **Sales** **-** **Sales**

> • Shows performance of the sales team handling sales transactions.
>
> • Metrics: Quoted, Sold, Cancelled, Net Revenue, ADL (Average Deal
> Length).
>
> • Visuals: Clustered column charts, trend lines, and sales rep tables.

**6.5** **Sales** **-** **Production**

> • Focuses on production-related sales.
>
> • Metrics: Production orders, completion percentage, VTC.
>
> • Visuals: Bar charts for monthly production vs. targets, cumulative
> VTC.

**6.6** **Sales** **-** **Service**

> • Tracks service-related sales performance.
>
> • Metrics: Service revenue, canceled service orders, net service
> sales.
>
> • Visuals: KPIs and stacked bar charts by service category.

**6.7** **Production**

> • Provides insight into production output and scheduling.
>
> • Metrics: Completed orders, pending orders, crew weeks.
>
> • Visuals: Line charts for weekly production trends, tables for crew
> allocation.

**6.8** **VTC** **(Value-to-Compete)**

> • Displays outstanding project work and value remaining to complete.
>
> • Metrics: VTC per project, VTC by status (Active, Completed).
>
> • Visuals: Stacked column charts, KPI cards for total VTC.

**6.9** **Service**

> • Focuses on service delivery metrics across teams.
>
> • Metrics: Service orders completed, pending, canceled.
>
> • Visuals: Trend charts and tables filtered by service type.

**6.10** **Location**

> • Provides geographic insights for leads, sales, and projects.
>
> • Visuals include a map by ZIP code, pie chart, and summary table.
>
> • Filters: Date range, city/region, product category, and foreman.
>
> • Enables drill-down by ZIP or region for performance comparison.

**6.11** **Product** **Category**

> • Analyzes sales and leads by product category.
>
> • Metrics: Category-wise sales, lead conversion rates, cancellations.
>
> • Visuals: Pie charts, bar charts, tables.

**6.12** **Lead** **by** **Cost**

> • Measures cost-effectiveness of leads.
>
> • Metrics: Cost per lead, total marketing spend, ROI per source.
>
> • Visuals: Column charts with drill-through to campaigns.

**7.** **Tools** **and** **Technologies**

> **Category**
>
> Data Source

**Tools** **Used**

Salesforce

> ETL / Integration Python (Salesforce API, Pandas, MySQL Connector)
>
> Database
>
> Data Modeling
>
> Visualization

MySQL

SQL (CTEs, Views, Joins)

Power BI

> **Category**
>
> Language
>
> Additional

**Tools** **Used**

Python, SQL

Excel (initial data source)

**8.** **Key** **Technical** **Features**

> • **Automated** **Data** **Pipeline**: Data extraction and loading
> automated using Python scripts.
>
> • **Incremental** **Load** **Support**: Reduces refresh time and
> avoids redundant data pulls.
>
> • **Transformation** **Logic** **in** **MySQL**: Clean, structured,
> and optimized data ready for Power BI.
>
> • **Dynamic** **Power** **BI** **Dashboards**: DAX measures and
> calculated columns for on-the-fly analytics.
>
> • **Centralized** **Data** **Warehouse**: MySQL acts as a single
> source of truth for Power BI reporting.

**9.** **Outcome**

The new automated pipeline eliminated manual data handling, improved
data accuracy, and enabled **real-time** **analytics** in Power BI.

The client now has **a** **single** **unified** **dashboard** providing
actionable insights across marketing, sales, production, and customer
care, empowering them to make faster and more informed decisions.

<img src="./zpltj1m0.png"
style="width:6.0693in;height:3.66042in" />**Sample** **Screenshots:**

<img src="./d2vcjhmc.png" style="width:6.05in;height:4.03917in" /><img src="./hj5w5dod.png"
style="width:6.16389in;height:4.11764in" />

<img src="./mptzgtwy.png"
style="width:6.17708in;height:3.84361in" />
