######KAFKA CONFIG###########
KAFKA_BROKER = "course-kafka:9092" 
KAFKA_TOPIC = 'Customer_Support'  

######Source File############
json_file_path = '/home/developer/projects/FinalProject/LoadData/Customer_support_data.json'  # Update with your JSON file path

#######S3####################
s3_bucket ='s3a://finalproject/app_source_data/'
s3_checkpoints='s3a://checkpoint/'


###########POSTGRES########
pg_url= "jdbc:postgresql://postgres:5432/postgres"
pg_dbtable="dw.customer_data_detail"
pg_user="postgres"
pg_password="postgres"
