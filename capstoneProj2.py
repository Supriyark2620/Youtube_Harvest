import requests
import zipfile 
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pandas as pd
import os
import glob
import json
import boto3
import io
from pyspark.sql.functions import explode
from botocore.exceptions import ClientError
from sqlalchemy import create_engine
import pymysql

bucket_region = 'US East (N. Virginia) us-east-1'
bucket_name = 'capstoneproj2'
bucket_key = 'Uploaded_json_files2'
database_name = 'newdb'
database_username = 'admin'
database_password = '12345678'
database_endpoint = 'newdb.c1iss2gaqyhy.us-east-1.rds.amazonaws.com'
database_port = 3306
s3_client = boto3.client('s3')
database_uri = "mysql+pymysql://{database_username}:{database_password}@{database_endpoint}:3306/{database_name}"

#Downloads the zip file from the URL

def downloadZipFile():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    url = 'https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip'
    response = requests.get(url,headers=headers,stream=True)
    if response.status_code == 200:
        with open('large_file.zip', 'wb') as file:
            for chunk in response.iter_content(chunk_size=128):
                file.write(chunk)

#Extract the data from the zip file and copy to output_dir folder
def extractFileFromFolder():
    with zipfile.ZipFile("C:/Users/kulka/temp4/BE_batch_Guvi_Aug6-5/large_file.zip", mode="r") as files:
        for file in files.namelist():
            files.extract(file, "output_dir/")

#Get all the files from the folder
def getFiles(filepath):
    all_files=[]
    for root,dirs,files in os.walk(filepath):
        files=glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files

#insert all the files in a list
file_list=[]
def insertFilesToList():
    for file in get_all_file:
        with open(file) as input:
            export=json.load(input)
            file_list.append(export)

def uploadToS3():
    s3_client=boto3.client('s3')
    data=df_filings2.to_json(orient="split")
    response=s3_client.put_object(Body=data,Bucket='capstoneproj2',Key='Uploaded_json_files2')
    print("Uploaded successfully")

#Retrieve data from s3 and store in dataframe
def create_dataframe():
    s3=s3_client
    name=bucket_name
    key=bucket_key
    try:
        get_response = s3.get_object(Bucket=name, Key=key)
        print("Object retrieved from S3 bucket successfully")
        df=json.loads(get_response['Body'].read())
        print(df)
        #df1=pd.DataFrame.from_dict(df)
        df1=pd.DataFrame(list(df.items()))
        df1
    except ClientError as e:
        print("S3 object cannot be retrieved:", e)
    return df1

#upload dataframe to rds
def uploadTords():
        table_name = 'clean_transaction'
        sql_query = f"SELECT * FROM {table_name}"
        database_uri1=database_uri
        try:
                engine = create_engine(database_uri)
                print('Database connection successful')
        except Exception as e:
                print('Database connection unsuccessful:', e)

        try:
                df_unmodified.to_sql(name=table_name, con="mysql://admin:12345678@newdb.c1iss2gaqyhy.us-east-1.rds.amazonaws.com/newdb", if_exists='replace', index=False)
                #df_unmodified.to_sql(table_name, con="sqlite3.Connection", if_exists='replace', index=False)
                print(f'Dataframe uploaded into {table_name} successfully')
                uploaded_df = pd.read_sql(sql_query, engine)
                print(uploaded_df.head())
        except Exception as e:
                print('Error happened while uploading dataframe into database:', e)


if __name__ == '__main__':

    downloadZipFile()
extractFileFromFolder()
get_all_file=getFiles("C:/Users/kulka/temp4/BE_batch_Guvi_Aug6-5/output_dir/Sample")
print(get_all_file)
insertFilesToList()
	
	#add the list contents to dataframe
df2=pd.DataFrame(file_list)
df2.head()
    #split the column addresses in 2 columns(Mailing and Business)
df3=df2["addresses"].apply(pd.Series)
df4=pd.concat([df2,df3], axis=1)
df5=df4.drop(columns="addresses")

        #further split Mailing column to multiple columns i.e street1,street2 etc
df_mailing=df5["mailing"].apply(pd.Series)
df_mailing1=pd.concat([df5,df_mailing], axis=1)
df_mailing2=df_mailing1.drop(columns="mailing")
	#rename column names
df_mailing2.rename(columns= {'street1':'Mailing_street1',
                                        'street2':'Mailing_street2',
                                        'city':'Mailing_city',
                                        'stateOrCountry':'Mailing_stateOrCountry',
                                        'zipCode':'Mailing_zipCode',
                                        'stateOrCountryDescription':'Mailing_stateOrCountryDescription'},inplace=True)

        #further split Business column to multiple columns i.e street1,street2 etc
df_business=df_mailing2["business"].apply(pd.Series)
df_business1=pd.concat([df_mailing2,df_business], axis=1)
df_business2=df_business1.drop(columns="business")

        #rename column names
df_business2.rename(columns= {'street1':'Business_street1',
                                        'street2':'Business_street2',
                                        'city':'Business_city',
                                        'stateOrCountry':'Business_stateOrCountry',
                                        'zipCode':'Business_zipCode',
                                        'stateOrCountryDescription':'Business_stateOrCountryDescription'},inplace=True)

df_filings=df_business2["filings"].apply(pd.Series)
df_filings1=pd.concat([df_business2,df_filings], axis=1)
df_filings2=df_filings1.drop(columns="filings")
df_filings2
	
uploadToS3()
df_unmodified = create_dataframe()
uploadTords()


