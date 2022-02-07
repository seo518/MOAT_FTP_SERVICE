import pandas as pd
import os
from boto3.session import Session
import sys 
sys.path.insert(0, "C:/Users/Administrator/Documents/AWS/")
import aws_library 
import base64
import re
import urllib
import datetime
import numpy as np 


# get list of objects in the email bucket
# Output = list of objects in the bucket
def get_emailBucket_list():
  bucket_name = 'xaxisemailreceipts'
  creds_directory = os.path.expanduser("~/Documents/AWS/creds/").replace("\\","/")
  s3Key = aws_library.get_aws_Key(bucket_name+'.csv', creds_directory)
  Session = aws_library.initiate_session(access_id = s3Key.iloc[0]["Access key ID"], secret_key = s3Key.iloc[0]["Secret access key"])
  your_bucket = aws_library.list_s3_files(bucket_name,Session)
  return(your_bucket)


# get the parameters from the config file
# @param = arg =  dataframe of config file
# Output:
#     download_key_word : key word to search the email for
#     default_path: directory path to save attchent file
#     rename: if file needs renaming, filename
#     time: time window in which to serch emails
#     search : search key word, to seach & return  specifc strings in the email body
def get_configs(arg):
  download_key_word = arg.loc[arg.fields == "download_key_word", "field_values"][0]
  download_key_word=re.split('&|or', download_key_word)
  default_path= arg.loc[arg.fields =='file_path','field_values'].apply(str).values
  time_window= arg.loc[arg.fields == 'time_window', 'field_values'].apply(int).values
  default_path = default_path[0].replace("\\","/")
  rename = arg.loc[arg.fields == 'rename', 'field_values'].apply(str).values
  rename=re.split(":",rename[0])
  time = datetime.datetime.today() - datetime.timedelta(np.float64(time_window[0]))
  search= arg.loc[arg.fields == 'search', 'field_values'].apply(str).values
  return (download_key_word,default_path,rename,time,search)


# filter throw bucket list and download attachment
# @params:
#     s: S3 bucket object
#     download_key_word : key word to search the email for
#     default_path: directory path to save attchent file
#     rename: if file needs renaming, filename
#     time: time window in which to serch emails
# Output: filename of the downloaded file 
def download_req_files(s,download_key_word,default_path,rename,time):
  email_key = "C:/Users/Administrator/Documents/Access Key/4c4172306742283679696f657c566c23.txt" 
  if s.last_modified.replace(tzinfo=None) > time :
    if aws_library.scan_email(s, access_key = email_key) == True:
      print('email key exist')
      #print(s)
      msg = aws_library.s3_load_email(s)
      
      sub=msg['Subject'].replace("\r\n",'')
      
      if all(x in sub.lower() for x in download_key_word):
        print(sub)
        if all(x != 'nan' for x in rename):
          for i in rename:
            if i in sub.lower().replace(" ","_"):
              title=i+"_"+('_'.join(download_key_word))
              print(title)
              break     
          
        filename = aws_library.s3emails_get_attachment(s, default_path, title)       
         
        return(filename)
    else:
      return(False)
    
    

  