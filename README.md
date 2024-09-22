# Enhancing-E-Commerce-Agility-With-Advanced-ETL-Pipeline

creating a pipline and stremlit application for Order and Returns
teams, performs a join operation using Glue & PySpark, stores the joined data in
Databricks, and sends notifications about the pipeline's status using SNS

## Technology I used in this project 
   1.AWS Glue
   2.pyspark 
   3.SNS 
   4.Step Funtion 
   5.S3
   6.Databricks
   7.Athena 
   8.Streamlit

## project step:

 * I create a streamlit app there user can upload return and order csv file
 * When they uploaded The csv file.  It will trigger the lambda function automaicaly
 * In that lambda function glue job will triggered
 * Aws Glue studio i have create a ETL
    -  S3 budget as source
    -  Transform (custom code)
    -  Transform (select  from collection)
    -  Data Target - s3 budget
    -  from s3 two csv file will be pass to the Transform node then data will be joined and it will stored in s3 budget 
 * From Targeted s3 budget it will share to Databricks to store the proccesed data
 * the Athena and step funtion use to creat a orchestration  to moniter the flow of the process.
   
