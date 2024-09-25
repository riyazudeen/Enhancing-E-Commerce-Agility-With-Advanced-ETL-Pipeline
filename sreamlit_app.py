from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import streamlit as st
import boto3
import concurrent.futures

order_bytes_data = ""
return_bytes_data = ""
AWS_ACCESS_KEY_ID = 'Enter your access key'
AWS_SECRET_ACCESS_KEY = 'Enter your secret access key'

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='eu-north-1'
)

def upload_file_to_s3(filepath, bucket_name, file_name):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
  
    try:
        s3.upload_file(filepath, bucket_name, file_name)
        message = f"File {file_name} uploaded to {bucket_name}/{file_name}."
        print(message)
        st.toast(body=message, icon="🎉")
    except NoCredentialsError:
        print("Credentials not available.")
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
    except Exception as e:
        print(f"Error uploading file: {e}")

# File upload section
st.header("Upload Order and Return Data")
uploaded_files_order = st.file_uploader(
    "Choose order CSV files", accept_multiple_files=True, key="order", type=["csv", "xlsx"]
)
uploaded_files_return = st.file_uploader(
    "Choose return CSV files", accept_multiple_files=True, key="return", type=["csv", "xlsx"]
)

# Handling parallel uploads
if uploaded_files_order and uploaded_files_return:
    if st.button("Upload", type="primary"):
        def process_and_upload(uploaded_file, bucket_name):
            file_path = fr"your file path {uploaded_file.name}"
            upload_file_to_s3(file_path, bucket_name, uploaded_file.name)
        
        # Create a thread pool for parallel uploads
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            
            # Upload order files in parallel
            for uploaded_file in uploaded_files_order:
                futures.append(executor.submit(process_and_upload, uploaded_file, 'your budget name'))
                
            # Upload return files in parallel
            for uploaded_file in uploaded_files_return:
                futures.append(executor.submit(process_and_upload, uploaded_file, 'your budget name'))
                
            # Wait for all uploads to complete
            for future in concurrent.futures.as_completed(futures):
                future.result()  # This will raise any exceptions that occurred during upload
else:
    st.warning("You need to upload both order and return files.")
