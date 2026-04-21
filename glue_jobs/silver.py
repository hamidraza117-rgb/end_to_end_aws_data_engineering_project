
# Read file from S3
print('Reading files from s3 Bronze buckets')
customers_bronze=spark.read.parquet('s3://aws-s3-project-08apr2025/bronze/b_customers/')
products_bronze=spark.read.parquet('s3://aws-s3-project-08apr2025/bronze/b_products/')
orders_bronze=spark.read.parquet('s3://aws-s3-project-08apr2025/bronze/b_orders/')
print('Files reading done !!!')
# Transform Data From All Dfs and Join
print('Joining DataFrames')

from pyspark.sql.functions import col

cu=customers_bronze.alias('cu')
ord=orders_bronze.alias('ord')
pr=products_bronze.alias('pr')


sales=ord\
      .join(cu,'customer_id','left')\
      .join(pr,'product_id','left')
print('Joining done! Cleaning joined dataframe now!!!')
sales_new= sales.select(
    'order_id','product_name',
    'customer_id','name',
    'category','quantity','country',
    'price_cast','order_dates')

sales_clean=sales_new.withColumnRenamed('price_cast','price')

total_price=sales_clean.withColumn('total_price',col('quantity')*col('price'))

print('dataframes joined and transformed')

# Write transformed data back to S3
print('writing joined and transformed data back to s3 silver bucket')
final_price=total_price.write.mode('overwrite').parquet('s3://aws-s3-project-08apr2025/silver/')
# ----------------------------
