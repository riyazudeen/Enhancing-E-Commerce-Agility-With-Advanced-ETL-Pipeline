spark.conf.set("fs.s3a.access.key", "Your access key")
spark.conf.set("fs.s3a.secret.key", "Your secret access key")

# Define the path to the CSV file in the S3 bucket
s3_path = "s3a://processedorderdata/processed.csv"

# Read the CSV file into a Spark DataFrame
order_df = spark.read.csv(s3_path, header=True, inferSchema=True)

# Display the first few rows of the DataFrame
#order_df.show()
print(order_df.columns)

# Clean column names by replacing invalid characters
cleaned_order_df = order_df.toDF(*[c.replace(" ", "_").replace(",", "").replace(";", "").replace("(", "").replace(")", "") for c in order_df.columns])

# Now save the cleaned DataFrame to a Hive table
cleaned_order_df.write.saveAsTable("orderdata")


spark.sql("SHOW TABLES").show()
