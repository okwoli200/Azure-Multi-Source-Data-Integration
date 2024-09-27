# Azure-Multi-Source-Data-Integration


## Project Overview
This project focused on developing a comprehensive ELTL (Extract, Load, Transform, and Load) pipeline to migrate data from multiple on-premises and external sources into Azure Data Lake and subsequently into a Production SQL Server Database. The goal was to consolidate data from various systems in alignment with business requirements, enabling efficient reporting, data analysis, and decision-making.

The pipeline utilized tools such as **Azure Data Factory (ADF), AzCopy, SQL queries, data extraction scripts,** and **stored procedures**. Additionally, reporting tables and views were created to support business reporting needs, with interactive dashboards and visualizations providing insights.

## Steps Taken to Complete the Project
### 1. Stakeholders Meeting
Regular meetings with stakeholders were held throughout the project to ensure alignment with business requirements. Key actions included:

**Gathering feedback**: Ongoing feedback was incorporated to address evolving requirements.
**Proposing changes**: Any modifications were discussed and agreed upon to ensure alignment with project goals.
**Collaboration**: A transparent approach maintained stakeholder buy-in and facilitated smooth project progress.
### 2. Data Quality Assessment
Before data migration, a data profiling process was conducted to identify anomalies, such as duplicates in flat file data. To ensure data quality:

**De-duplication**: Steps were implemented to eliminate duplicate records, ensuring clean data for migration.
### 3. Data Ingestion into Azure Data Lake
Data from on-premises OLTP databases and flat files were ingested into Azure Data Lake for two primary purposes:

**Data Backup**: In case of ETL failures, data could be easily reloaded from the Data Lake.
**Archival Storage**: The Data Lake acted as an archival repository with redundant copies of the data.
Data ingestion was automated using **Azure Data Factory (ADF)** for OLTP data and **Windows Task Scheduler** for flat files. Schedules included:

**Fact Tables**: Nightly ingestion.
**Dimension Tables**: Bimonthly ingestion.
### 4. Designing the Data Warehouse
The Data Warehouse was designed using the **dimensional modeling** technique (star schema) pioneered by Ralph Kimball. The design process included:

**Selecting Business Processes**: Identifying key business processes to be represented.
**Declaring Granularity**: Defining the level of detail for capturing data.
**Identifying Dimensions**: Choosing dimensions that provide context to the facts.
**Determining Facts**: Selecting measurable data points to store in the fact tables.
### 5. Loading Data from the Data Lake into the Data Warehouse
Data from the Data Lake was transformed and loaded into the Data Warehouse using scalable **SQL** and **stored procedure** scripts. The process included:

**Staging Environment**: Data was first loaded into a staging area for validation and transformations, including:
**Denormalization**
**Aggregation**
**Validation**
**Log Tracking**: A log table was used to track key metrics (e.g., source count, staging count, EDW count) to ensure data integrity throughout the process.
### 6. Deployment of the ELTL Model
To ensure the ELTL process ran efficiently, a regular schedule was established for moving data into the Data Lake and Data Warehouse. Two separate **ADF pipelines** were used:

**Dimension Data Pipeline**: Scheduled bimonthly due to the infrequency of changes in dimension data.
**Fact Table Pipeline**: Scheduled daily during off-peak periods for regular updates.
Flat files were loaded using Task Scheduler, and an event-driven pipeline triggered by storage events ensured data migration from the Data Lake to the **Enterprise Data Warehouse (EDW)** only when new data was available.

**Technologies and Tools Used**
**Azure Data Factory (ADF)**
**Azure Data Lake**
**SQL Server**
**Python Scripts**
**AzCopy**
**Power BI** for reporting and visualization
### Conclusion
The successful implementation of this ELTL pipeline provided the retail industry client with a scalable and efficient solution for consolidating data from multiple sources into Azure. This project improved data accessibility, facilitated better reporting and analytics, and enabled more informed business decisions.
