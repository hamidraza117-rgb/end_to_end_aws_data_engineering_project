print('Reading file from s3 silver bucket')
# Read file from S3

gold_sales= spark.read.parquet('s3://aws-s3-project-08apr2025/silver/')

# Transform Data
print('transforming the data')
from pyspark.sql.functions import sum,year,month,col
total_revenue= gold_sales.groupBy('product_name').agg(sum('total_price').alias('total_revenue'))
country_revenue= gold_sales.groupBy('country').agg(sum('total_price').alias('country_revenue'))
sales_year=gold_sales\
            .withColumn('year',year(col('order_dates')))\
            .withColumn('month',month(col('order_dates')))

# 5. Write transformed data back to S3
print('writing data back to final gold s3 bucket')
gold_sales_finalized= sales_year\
                        .write.mode('overwrite')\
                        .partitionBy('year','month')\
                        .parquet('s3://aws-s3-project-08apr2025/gold/')
revenue_per_product=total_revenue\
                        .write.mode('overwrite')\
                        .parquet('s3://aws-s3-project-08apr2025/gold/revenue_per_product/')
revenue_per_country=country_revenue\
                        .write.mode('overwrite')\
                        .parquet('s3://aws-s3-project-08apr2025/gold/revenue_per_country/')
