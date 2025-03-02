# Project 1: Fraud Detection Using Azure Data Engineering

## Overview
This project is designed to detect potentially fraudulent transactions based on the distance between a customer's registered city and the city where the transaction occurred. Fraud criticality is categorized as **Low, Medium, or High** based on the calculated distance.  

The data pipeline is built using:
- **Azure Data Factory (ADF)**
- **Azure Data Lake Storage (ADLS)**
- **Azure Databricks**
- **Azure SQL Database**

## Data Model
![Data Model](https://github.com/devanand31/DataEngineering/blob/main/Projects/FraudTransactionReport/Misc/FraudDetectionDataModel.png)

## Architecture
![Architecture Diagram](https://github.com/devanand31/DataEngineering/blob/main/Projects/FraudTransactionReport/Misc/FraudDetectionArchitecture.png)

## Report
![Report](https://github.com/devanand31/DataEngineering/blob/main/Projects/FraudTransactionReport/Misc/FraudDetectionReport.png)

## Project Link
[Fraud Detection](https://github.com/devanand31/DataEngineering/tree/main/Projects/FraudTransactionReport)


# Project 2: Retail Store Reporting Using Azure Data Engineering

## Overview
This project is designed to generate Revenue and Stock Wastage Reports for an FMCG Retail Company. The Revenue Reports provide insights on a monthly basis, categorized by product and city. Additionally, the reporting includes stock wastage analysis, calculating the financial loss incurred due to different suppliers, which helps in determining the supplier rating.

The data pipeline follows the medallion architecture and is built using:
- **Azure Data Factory (ADF)**
- **Azure Data Lake Storage (ADLS)**
- **Azure Databricks with Unity Catalog setup**

## Data Model
![Data Model](https://github.com/devanand31/DataEngineering/blob/main/Projects/FMCGRetailReporting/Misc/RetailStoreDataModel.png)

## Architecture
![Architecture Diagram](https://github.com/devanand31/DataEngineering/blob/main/Projects/FMCGRetailReporting/Misc/RetailStoreArchitecture.png)

## Report
### Revenue Reports
![Report](https://github.com/devanand31/DataEngineering/blob/main/Projects/FMCGRetailReporting/Misc/RetailStoreRevenueReport.png)
### Wastage and Other Reports
![Report](https://github.com/devanand31/DataEngineering/blob/main/Projects/FMCGRetailReporting/Misc/RetailStoreWastageAndOtherReports.png)

## Project Link
[Retail Store Reporting](https://github.com/devanand31/DataEngineering/tree/main/Projects/FMCGRetailReporting)
