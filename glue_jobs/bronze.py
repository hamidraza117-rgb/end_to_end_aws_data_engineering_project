
#Read file from S3----------------------------------------------------------------------------------
print('Reading files from s3 raw bucket')
customers01=spark.read.option('header','true').csv('s3://aws-s3-project-08apr2025/raw/customers/')
products01=spark.read.option('header','true').csv('s3://aws-s3-project-08apr2025/raw/products/')
orders01=spark.read.option('header','true').csv('s3://aws-s3-project-08apr2025/raw/orders/')

# TRANSFORM CUSTOMERS DATA----------------------------------------------------------------------------
print('transforming customers dataframe')
from pyspark.sql.functions import to_date,col,regexp_replace,trim,col,expr,lower
customers02=customers01.withColumn('sign_up',regexp_replace(col('signup_date'),'/','-').alias('date'))
customers03 =customers02.withColumn(
    "new_date",
    expr("""
        to_date(
          coalesce(
            try_to_timestamp(sign_up, 'yyyy-MM-dd'),
            try_to_timestamp(sign_up, 'dd-MM-yyyy')
        ))
    """)
)
strings=['name','email','country']
strings_fixed=customers03
for c in strings:
  strings_fixed=strings_fixed.withColumn(c,
                                         lower(trim(col(c))))
customer_clean= strings_fixed.drop('signup_date','sign_up')
customer_filled= customer_clean.na.fill({'email':'unknown','country':'unknown'})
de_duplicates= customer_filled.dropDuplicates(['customer_id'])
print('customers data succesfully transformed')
# TRANSFORM ORDERS DATA----------------------------------------------------------------------------
print('transforming orders data')
from pyspark.sql.functions import col,regexp_replace,to_date,expr,trim,lower,expr

orders_date_new=orders01.withColumn('order_date_new',
                             regexp_replace('order_date','/','-'))
orders_date = orders_date_new.withColumn(
    "order_dates",
    expr("to_date(try_to_timestamp(order_date_new, 'yyyy-MM-dd'))")
)
orders_date_invalid=orders_date.filter(col('order_dates').isNull())

status_trimmed=orders_date.withColumn('status',lower(trim(col('status'))))
order_id_clean = status_trimmed.dropDuplicates(['order_id'])
numerics= ['order_id','customer_id','quantity']
numerics_casted=order_id_clean
for c in numerics:
  numerics_casted=numerics_casted.withColumn(c,
                                            expr(f'try_cast({c} as int)'))
orders_clean= numerics_casted.drop('order_date','order_date_new')
orders_date_drop=orders_clean.na.drop(subset=['order_dates'])
orders_finalized= orders_date_drop.na.fill({'status':'unknown','quantity':'2'})
print('orders data succesfully transformed')
# TRANSFORM PRODUCTS DATA ----------------------------------------------------------------------------------
print('transforming products dataframe')
from pyspark.sql.functions import trim,col,lower
#cleaning strings columns
config={'string':['product_name','category']}
for c in config['string']:
  products01=products01.withColumn(
      c,lower(trim(col(c))))

from pyspark.sql.functions import cast,col,expr
products_cast= products01.withColumn('price_cast',expr('try_cast(price as double)'))
stocks_casted=products_cast.withColumn('stock_cast',col('stock').cast('double'))
products_invalid= products_cast.filter(col('price_cast').isNull())
stocks_invalid= stocks_casted.filter(col('stock_cast').isNull())
products_filled=stocks_casted.na.fill({'stock_cast':0,'category':'N/A'})
products_finalized=products_filled.drop('price','stock')
price_invalid=products_finalized.na.drop(subset=['price_cast'])

print('products dataframe succesfully transformed')

# Filtering out bad records to ERROR BUCKET

print('sending bad reacord to error bucket')
if products_invalid.count()>0:
    products_invalid.write.mode('append').parquet('s3://aws-s3-project-08apr2025/error/')
if orders_date_invalid.count()>0:
    orders_date_invalid.write.mode('append').parquet('s3://aws-s3-project-08apr2025/error/')
# -------------------------------------------------------------------------------------------------
print('writing all the transformed data back to s3 bronze buckets')
# Write transformed data back to S3
orders_write=orders_finalized.write.mode('overwrite').parquet('s3://aws-s3-project-08apr2025/bronze/b_orders/')
customers_write = de_duplicates.write.mode('overwrite').parquet('s3://aws-s3-project-08apr2025/bronze/b_customers/')
products_write = price_invalid.write.mode('overwrite').parquet('s3://aws-s3-project-08apr2025/bronze/b_products/')
# ----------------------------
