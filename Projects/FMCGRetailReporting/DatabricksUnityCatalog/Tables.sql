CREATE TABLE UTILS.LOGGING.PIPELINE_RUN_LOG(
	PIPELINE_RUN_ID STRING,
	ENTITY STRING,
	PIPELINE_NAME STRING,
	PIPELINE_STAGE STRING,
	PIPELINE_STATUS STRING,
	RECORDS_PROCESSED INT,
	START_TIME TIMESTAMP,
	END_TIME TIMESTAMP
);

CREATE TABLE UTILS.CONTROL.TABLE_LIST(
	TABLE_ID INT,
	ENTITY_NAME STRING,
	SOURCE_PATH STRING,
	SOURCE_FILE_NAME STRING,
	TARGET_PATH STRING,
	TABLE_TYPE STRING,
	KEY_COLUMN STRING,
	MUTABLE_COLUMNS STRING,
	IS_ACTIVE STRING
);

CREATE TABLE RETAIL_STORE.SILVER.CUSTOMER_DIM
(
  CustomerID STRING,
  Name STRING,
  Age STRING,
  Gender STRING,
  City STRING,
  SignupDate STRING,
  LastUpdated DATE,
  FromEffectiveDate DATE,
  ToEffectiveDate DATE,
  IsActive STRING  
);

CREATE TABLE RETAIL_STORE.TEMP.CUSTOMER_NEW_DATA
(
  CustomerID STRING,
  Name STRING,
  Age STRING,
  Gender STRING,
  City STRING,
  SignupDate STRING,
  LastUpdated DATE,
  FromEffectiveDate DATE,
  ToEffectiveDate DATE,
  IsActive STRING  
);


CREATE TABLE RETAIL_STORE.SILVER.PRODUCT_DIM
(
  ProductID STRING,
  Name STRING,
  Category STRING,
  Price FLOAT,
  SupplierID INT,
  LastUpdated DATE,
  FromEffectiveDate DATE,
  ToEffectiveDate DATE,
  IsActive STRING 
);


CREATE TABLE RETAIL_STORE.TEMP.PRODUCT_NEW_DATA
(
  ProductID STRING,
  Name STRING,
  Category STRING,
  Price FLOAT,
  SupplierID INT,
  LastUpdated DATE,
  FromEffectiveDate DATE,
  ToEffectiveDate DATE,
  IsActive STRING 
);



CREATE TABLE RETAIL_STORE.SILVER.STORE_DIM
(
  StoreID STRING,
  Name STRING,
  City STRING,
  Region STRING,
  LastUpdated DATE,
  FromEffectiveDate DATE,
  ToEffectiveDate DATE,
  IsActive STRING 
);

CREATE TABLE RETAIL_STORE.TEMP.STORE_NEW_DATA
(
  StoreID STRING,
  Name STRING,
  City STRING,
  Region STRING,
  LastUpdated DATE,
  FromEffectiveDate DATE,
  ToEffectiveDate DATE,
  IsActive STRING 
);



CREATE TABLE RETAIL_STORE.SILVER.SUPPLIER_DIM
(
  SupplierID STRING,
  Name STRING,
  ContactInfo STRING,
  Rating STRING,
  LastUpdated DATE,
  FromEffectiveDate DATE,
  ToEffectiveDate DATE,
  IsActive STRING 
);


CREATE TABLE RETAIL_STORE.TEMP.SUPPLIER_NEW_DATA
(
  SupplierID STRING,
  Name STRING,
  ContactInfo STRING,
  Rating STRING,
  LastUpdated DATE,
  FromEffectiveDate DATE,
  ToEffectiveDate DATE,
  IsActive STRING 
);



CREATE TABLE RETAIL_STORE.SILVER.DATE_DIM
(
  Date DATE,
  Year INT,
  Quarter INT,
  Month INT,
  Weekday STRING,
  DayOfWeek INT,
  HolidayFlag BOOLEAN
);


CREATE TABLE RETAIL_STORE.SILVER.SALES_TRANSACTION_FACT
(
TransactionID STRING,
CustomerID INT,
StoreID INT,
ProductID INT,
Quantity INT,
TotalAmount FLOAT,
PaymentMethod STRING,
TransactionDate DATE,
IsHighestPurchaseForTheDay INT
);


CREATE TABLE RETAIL_STORE.SILVER.STOCK_MOVEMENT_FACT
(
StockID STRING,
StoreID INT,
ProductID INT,
OpeningStock INT,
ReceivedStock INT,
SoldStock INT,
ClosingStock INT,
StockDate DATE,
WasteStock INT,
LossDueToWastage FLOAT
);

CREATE TABLE retail_store.gold.city_revenue
(
    CityName STRING,
    Revenue FLOAT
);

CREATE TABLE retail_store.gold.customer_loyalty_score
(
    Name STRING,
    CustomerLoyaltyScore INT
);

CREATE TABLE retail_store.gold.loss_due_to_stock_wastage
(
    Name STRING,
    TotalLossDuetoStockWastage FLOAT
);

CREATE TABLE retail_store.gold.monthly_revenue
(
    Month STRING,
    MonthlyRevenue FLOAT
);

CREATE TABLE retail_store.gold.product_revenue
(
    ProductCategoryName STRING,
    Revenue FLOAT
);

CREATE TABLE retail_store.gold.weekday_stock_wastage
(
    Weekday STRING,
    TotalWasteStock INT
);
