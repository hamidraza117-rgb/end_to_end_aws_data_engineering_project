# end_to_end_aws_data_engineering_project
End-to-end AWS ETL pipeline using S3, Lambda, Step Functions, and Glue with medallion architecture
project/
│
├── README.md
├── architecture.png/
├── screenshots/
├── datasets/
│   ├── customers_raw.csv
│   ├── orders_raw.csv
│   ├── products_raw.csv
│
├── glue_jobs/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│
└── lambda/
    ├── trigger.py
