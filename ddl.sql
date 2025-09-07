CREATE OR REPLACE STAGE AWS_ETL_PARQUET_STAGE
STORAGE_INTEGRATION = AWS_S3_NYCT_INT
URL = 's3://dateng-snowflake/loadingdata/parquet/'
FILE_FORMAT = PARQUET_ETL_FILEFORMAT

CREATE OR REPLACE FILE FORMAT CSV_ETL_FILEFORMAT
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('Null','NULL')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'        
TRIM_SPACE=TRUE
COMPRESSION=AUTO

CREATE OR REPLACE STAGE AWS_ETL_CSV_STAGE
STORAGE_INTEGRATION = AWS_S3_NYCT_INT
URL = 's3://dateng-snowflake/loadingdata/csv/'
FILE_FORMAT = CSV_ETL_FILEFORMAT

CREATE OR REPLACE TABLE BRONZE_NY_TAXI_RIDES (
  ride_id STRING,
  load_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  vendor_id INTEGER,
  pickup_datetime TIMESTAMP_NTZ,
  dropoff_datetime TIMESTAMP_NTZ,
  passenger_count INTEGER,
  trip_distance FLOAT,
  rate_code_id INTEGER,
  store_and_fwd_flag STRING,
  pickup_location_id INTEGER,
  dropoff_location_id INTEGER,
  payment_type INTEGER,
  fare_amount FLOAT,
  extra FLOAT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  improvement_surcharge FLOAT,
  total_amount FLOAT,
  congestion_surcharge FLOAT,
  airport_fee FLOAT,
  cbd_congestion_fee FLOAT,
  ride_month INTEGER,
  ride_type STRING,
  file_name STRING,
  base_name STRING,
  UNIQUE(ride_id) -- Ensure ride_id uniqueness
);

CREATE OR REPLACE TABLE BRONZE_NY_TAXI_RIDES_REJECTS (
  ride_id STRING,
  load_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  vendor_id INTEGER,
  pickup_datetime TIMESTAMP_NTZ,
  dropoff_datetime TIMESTAMP_NTZ,
  passenger_count INTEGER,
  trip_distance FLOAT,
  rate_code_id INTEGER,
  store_and_fwd_flag STRING,
  pickup_location_id INTEGER,
  dropoff_location_id INTEGER,
  payment_type INTEGER,
  fare_amount FLOAT,
  extra FLOAT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  improvement_surcharge FLOAT,
  total_amount FLOAT,
  congestion_surcharge FLOAT,
  airport_fee FLOAT,
  cbd_congestion_fee FLOAT,
  ride_month INTEGER,
  ride_type STRING,
  file_name STRING,
  base_name STRING,
  UNIQUE(ride_id),                 -- Ensure ride_id uniqueness 

  -- Additional audit columns
  rejection_reason STRING,         -- e.g. 'missing pickup_datetime', 'negative fare_amount'
  rejection_stage STRING,          -- e.g. 'bronze_ingest', 'schema_validation'
  rejection_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);
 

CREATE OR REPLACE TABLE SILVER_NY_TAXI_RIDES (
  vendor_id INTEGER,
  pickup_datetime TIMESTAMP_NTZ,
  dropoff_datetime TIMESTAMP_NTZ,
  passenger_count INTEGER,
  trip_distance FLOAT,
  rate_code_id INTEGER,
  store_and_fwd_flag STRING,
  pickup_location_id INTEGER,
  dropoff_location_id INTEGER,
  payment_type INTEGER,
  fare_amount FLOAT,
  extra FLOAT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  improvement_surcharge FLOAT,
  total_amount FLOAT,
  congestion_surcharge FLOAT,
  airport_fee FLOAT,
  cbd_congestion_fee FLOAT,
  ride_month INTEGER,
  ride_type STRING,
  ride_duration_minutes FLOAT,
  avg_speed_mph FLOAT,
  data_quality_flag STRING DEFAULT 'valid', -- e.g. 'valid', 'outlier', 'missing_fields'
  is_airport_trip BOOLEAN,
  is_peak_hour BOOLEAN,
  load_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  ride_id STRING UNIQUE,
  file_name STRING  
);


CREATE OR REPLACE TABLE LOCATION_REFERENCE (
    location_id INT,
    borough VARCHAR(20),
    zone VARCHAR(75),
    service_zone VARCHAR(20),
    latitude FLOAT,
    longitude FLOAT
);


CREATE OR REPLACE TABLE RATE_CODE_DIM (
    RATE_CODE_ID INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    RATECODEID INT UNIQUE,
    RATE_CODE_NAME VARCHAR(30)
);

CREATE OR REPLACE TABLE PAYMENT_TYPE_DIM (
    PAYMENT_TYPE_ID INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    PAYMENT_TYPE INT UNIQUE,
    PAYMENT_TYPE_NAME VARCHAR(30)
);


CREATE OR REPLACE TABLE DATETIME_DIM (
    DATETIME_ID INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    TPEP_PICKUP_DATETIME TIMESTAMP_NTZ,
    PICK_HOUR INT,
    PICK_DAY INT,
    PICK_MONTH INT,
    PICK_YEAR INT,
    PICK_WEEKDAY INT,
    TPEP_DROPOFF_DATETIME TIMESTAMP_NTZ,
    DROP_HOUR INT,
    DROP_DAY INT,
    DROP_MONTH INT,
    DROP_YEAR INT,
    DROP_WEEKDAY INT,
    IS_WEEKEND BOOLEAN,
    IS_PEAK_HOUR BOOLEAN,
    PICKUP_DAY_NAME VARCHAR(10)
);

CREATE OR REPLACE TABLE LOCATION_DIM (
    LOCATION_ID INT PRIMARY KEY,
    LOCATION_TYPE VARCHAR(10),
    BOROUGH VARCHAR(20),
    ZONE VARCHAR(75),
    SERVICE_ZONE VARCHAR(20),
    LATITUDE FLOAT,
    LONGITUDE FLOAT
);

CREATE OR REPLACE TABLE PASSENGER_COUNT_DIM (
    PASSENGER_COUNT_ID INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    PASSENGER_COUNT INT,
    PASSENGER_BUCKET VARCHAR(10)
);

CREATE OR REPLACE TABLE TRIP_DISTANCE_DIM (
    TRIP_DISTANCE_ID INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    TRIP_DISTANCE FLOAT,
    DISTANCE_BUCKET VARCHAR(10)
);

CREATE OR REPLACE TABLE PAYMENT_TYPE_DIM (
    PAYMENT_TYPE_ID INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    PAYMENT_TYPE INT,
    PAYMENT_TYPE_NAME VARCHAR(30)
);

CREATE OR REPLACE TABLE VENDOR_DIM (
    VENDOR_ID INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    VENDOR STRING UNIQUE
);

CREATE OR REPLACE TABLE FACT_TAXI_RIDES (
    fact_id INT AUTOINCREMENT PRIMARY KEY,
    ride_id STRING,
    vendor_id INT,
    datetime_id INT,
    passenger_count_id INT,
    trip_distance_id INT,
    rate_code_id INT,
    store_and_fwd_flag STRING,
    pickup_location_id INT,
    dropoff_location_id INT,
    payment_type_id INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
	congestion_surcharge FLOAT,
	airport_fee FLOAT,
	cbd_congestion_fee FLOAT,
	ride_month INTEGER,
	ride_type STRING,
	ride_duration_minutes FLOAT,
	avg_speed_mph FLOAT,
	is_airport_trip BOOLEAN,
	is_peak_hour BOOLEAN,

    FOREIGN KEY (datetime_id) REFERENCES DATETIME_DIM(DATETIME_ID),
    FOREIGN KEY (passenger_count_id) REFERENCES PASSENGER_COUNT_DIM(passenger_count_id),
    FOREIGN KEY (trip_distance_id) REFERENCES TRIP_DISTANCE_DIM(trip_distance_id),
    FOREIGN KEY (rate_code_id) REFERENCES RATE_CODE_DIM(rate_code_id),
    FOREIGN KEY (pickup_location_id) REFERENCES LOCATION_DIM(location_id),
    FOREIGN KEY (dropoff_location_id) REFERENCES LOCATION_DIM(location_id),
    FOREIGN KEY (payment_type_id) REFERENCES PAYMENT_TYPE_DIM(payment_type_id),
    FOREIGN KEY (vendor_id) REFERENCES VENDOR_DIM(vendor_id)
) 	CLUSTER BY (ride_month, pickup_location_id, datetime_id);

CREATE OR REPLACE TABLE DIMENSION_LOAD_LOG (
  log_id INT AUTOINCREMENT PRIMARY KEY,
  dimension_name STRING,
  load_type STRING,               -- e.g. 'full', 'incremental'
  load_start_time TIMESTAMP_NTZ,
  load_end_time TIMESTAMP_NTZ,
  row_count INT,
  status STRING,                  -- e.g. 'success', 'failed'
  error_message STRING            -- nullable
);

CREATE OR REPLACE TABLE FACT_LOAD_LOG (
  log_id INT AUTOINCREMENT PRIMARY KEY,
  fact_table_name STRING,
  load_type STRING,               -- e.g. 'full', 'incremental'
  load_start_time TIMESTAMP_NTZ,
  load_end_time TIMESTAMP_NTZ,
  row_count INT,
  status STRING,                  -- e.g. 'success', 'failed'
  error_message STRING            -- nullable
); 

CREATE OR REPLACE TABLE BRONZE_LOAD_LOG (
  log_id INT AUTOINCREMENT PRIMARY KEY,
  load_start_time TIMESTAMP_NTZ,
  load_end_time TIMESTAMP_NTZ,
  row_count INT,
  rejected_row_count INT,
  status STRING,                  -- e.g. 'success', 'failed'
  error_message STRING            -- nullable
); 

