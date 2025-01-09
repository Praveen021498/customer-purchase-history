# Databricks notebook source
# DBTITLE 1,Mount  S3
# AWS Credentials


ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")

# S3 Bucket Information
BUCKET_NAME = "customer-purchase-history"
MOUNT_NAME = "/mnt/s3customer-purchase-history"  # The name of the mount point in Databricks

try:
    dbutils.fs.unmount(MOUNT_NAME)
except:
    pass

# Mount the bucket
dbutils.fs.mount(
    source=f"s3a://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{BUCKET_NAME}",
    mount_point=MOUNT_NAME
)

# Verify the mount
display(dbutils.fs.ls(MOUNT_NAME))

# COMMAND ----------

# DBTITLE 1,read csv from s3
df_original = spark.read.csv("/mnt/s3customer-purchase-history/customers/customer_purchases.csv", header=True, inferSchema=True)
df_original.show()

# COMMAND ----------

# DBTITLE 1,schema
df_original.printSchema()

# COMMAND ----------

# DBTITLE 1,describe
df_original.describe().show()

# COMMAND ----------

# DBTITLE 1,checking for missing data
from pyspark.sql.functions import count, when, col

# Check for missing values in the entire DataFrame
df_original.select([count(when(col(c).isNull(), c)).alias(c) for c in df_original.columns]).show()


# COMMAND ----------

# DBTITLE 1,count of duplicate rows in all columns
from pyspark.sql.functions import col

# Group by all columns and count occurrences
duplicate_rows = df_original.groupBy(df_original.columns).count().filter(col("count") > 1)

# Show the duplicate rows
duplicate_rows.show()

#drop duplicates
df_original.dropDuplicates().show()

# COMMAND ----------

# DBTITLE 1,filtering  invalid rows
from pyspark.sql.functions import col

df_clean = df_original.filter(col("order_amount") > 0)
df_clean.show()

# COMMAND ----------

# DBTITLE 1,date transformation
from pyspark.sql.functions import date_format

df_transformed = df_clean.withColumn("purchase_date",date_format("purchase_date", "MMM d yyyy"))

df_transformed.show()


# COMMAND ----------

# DBTITLE 1,windowing operation to rank purchases by date per customer
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification (partition by 'customer_id' and order by 'purchase_date')
window_spec = Window.partitionBy("customer_id").orderBy("purchase_date")

# Use row_number() to rank purchases by date for each customer
df_ranked = df_transformed.withColumn("purchase_rank", F.row_number().over(window_spec))

# Use rank() to assign a rank with ties (if two purchases have the same date)
#df_ranked = df_transformed.withColumn("purchase_rank", F.rank().over(window_spec))

df_ranked.show()

# COMMAND ----------

# DBTITLE 1,cummulative sum of customer purchases
# Define window specification (partition by 'customer_id' and order by 'purchase_date')
window_spec = Window.partitionBy("customer_id").orderBy("purchase_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate cumulative spending using the sum() window function
df_cumulative = df_transformed.withColumn("cumulative_spending", F.sum("order_amount").over(window_spec))

df_cumulative.show()

# COMMAND ----------

display(dbutils.fs.ls("mnt/s3customer-purchase-history/customers/"))


# COMMAND ----------

# DBTITLE 1,writing data to delta lake
# Write the transformed data into a Delta table, partitioned by customer_id
df_cumulative.write \
    .format("delta") \
    .partitionBy("customer_id") \
    .mode("overwrite") \
    .save("/mnt/s3customer-purchase-history/customers/customer_purchases_transformed")
