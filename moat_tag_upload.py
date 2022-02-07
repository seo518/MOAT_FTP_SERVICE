import pandas as pd
import numpy as np
import ntpath
import base64
import os
import glob
import json
import requests
import sys
sys.path.insert(0, "C:/Users/shweta.anjan/OneDrive - insidemedia.net/AWS/aws_library/")
import aws_library 
import re
import datetime


def get_emailBucket_list():
  bucket_name = 'xaxisemailreceipts'
  creds_directory = os.path.expanduser("~/Documents/AWS/creds/").replace("\\","/")
  s3Key = aws_library.get_aws_Key(bucket_name+'.csv', creds_directory)
  Session = aws_library.initiate_session(access_id = s3Key.iloc[0]["Access key ID"], secret_key = s3Key.iloc[0]["Secret access key"])
  your_bucket = aws_library.list_s3_files(bucket_name,Session)
  return(your_bucket)

def get_configs(arg):
    arg = arg.fillna('')
    cols = arg.columns
    var = {}
    for i in arg[cols[0]] :
        temp = arg.loc[arg[cols[0]] == i, cols[1]].apply(str).values
        print(temp[0])
        var[i.replace(' ','')] = temp[0]    
    return(var)    

        

#Description
#This config script to specifically process and email moat tags 
#This config scriptis specific for Moat DV360 tags
#Calls upon the underlying fucntions to: 
                           #List S3 emailBucket files, 
                           #scan file list, 
                           #download files, 
                           #process files
                           #email file to requied ID list

#config parameter file
#@param: Input1: arg file with following input: @fields
                                         #1. download_key_word : file keyword to filter in email bucket
                                         #2. time window: date range for the file upload
                                         #3. rename: if the file needs to be renamed, prefix_filename
                                         #4. search: keywords in the email to search for
path ="C:/Users/shweta.anjan/OneDrive - insidemedia.net/Moat_FTP_Transfer/config files"                                       
arg = pd.read_csv(path+"/config_email_tagsheet_new.csv")
#landing_page_url = 'https://www.kindsnacks.ca/'



var = get_configs(arg)  

var['time_window'] = int(var['time_window'])                                

var['download_key_word'] = re.split('&|or', var['download_key_word'])        

var['cols'] = re.split(';', var['cols']) 


#call function that process the config file
#@input param: arg file
#@output: fileds from the arg file
# download_key_word,default_path,rename,time,search_key = download_S3_email_files.get_configs(arg)
# default_path=default_path.replace("\\","/")

# #set the file path for the temp download file
# if not default_path.endswith("/"):
#  default_path=default_path+"/"

#list the objects from the email bucket
yourBucket = get_emailBucket_list()

# Loop through the list and filter for specific file
for s in yourBucket:
 dv_360_df=pd.DataFrame()
 with open ((default_path+'tag_log_index.txt'),"r") as indexfile:
  if str(s) not in indexfile.read():
   with open ((default_path+'tag_log_index.txt'),"a+") as indexfile:
     indexfile.write("\n"+str(s)+"  #### "+datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S')+"###")
   indexfile.close()   
   
   temp_search=''
   filename = download_S3_email_files.download_req_files (s,download_key_word,default_path,rename,time)
   print(filename)
   if filename is not None:
    print("download complete")
    temp_search = aws_library.search_email_body(s, search_key[0].lower())
    if temp_search:
     landing_page_url= temp_search
     print('got key')
     ext = filename.split(".")
     print(ext)
     #print(ext[1])
     if (ext[1] == "xls" or ext[1] =="csv" or ext[1] == "xlsx"):  
      tag_sheet = pd.read_excel(default_path+filename, 'Tags',skiprows=12)
      sheet2 = default_path+ext[0]+".csv"
      print(sheet2)
      tag_sheet = tag_sheet.drop(tag_sheet.columns[0], axis='columns')
      script_insert = '\n' +"<script"+ '\n' +"src="+moat_src+"\n"+"type=\"text/javascript\"></script>"
      dv_360_df['Creative name'] = tag_sheet['Placement Name']+"_MOAT"
      dv_360_df['Requires HTML5 ("Yes" or "No")'] = 'Yes' 	
      dv_360_df = dv_360_df.replace(np.nan,'', regex=True)
      dv_360_df['Landing page URL'] = landing_page_url
      dv_360_df['Third-party tag'] = tag_sheet['Iframes/JavaScript Tag'].str.replace(replace, (replace+script_insert))
      dv_360_df['Dimensions (width x height)'] = tag_sheet['Dimensions']
      dv_360_df= dv_360_df.loc[:, col]
      dv_360_df.to_csv(sheet2, index=False)
      #dv_360_df=pd.DataFrame()
      data = open (sheet2,'rb')
      data_base64 = base64.b64encode(data.read()).decode('utf-8')
      data.close()
      
      
      to=[]
      email_list = aws_library.get_from_emailID(s, 'from',email_key)
      #to = re.findall("<.*>",to)[0].replace("<",'').replace(">",'')
      Cc= aws_library.get_from_emailID(s, 'Cc',email_key)
      To= aws_library.get_from_emailID(s, 'To',email_key)
      if Cc:
       email_list = email_list+ ',' + Cc
      if To:
       email_list = email_list+ ',' + To
       
      email_list = email_list.replace(' ','').replace('\r\n','').replace('\t','')
      email_list = email_list.split(',')
      for i in email_list:
       i=i.replace('xaxis.canadator@xaxiscanadaanalytics.com','').replace('xaxis.canadaTOR@xaxiscanadaanalytics.com','').rstrip(",")
       if "<" in i:  
        i = re.findall("(?<=\<)(.*?)(?=\>)",i)[0]
       if i:
        print(i)
        to.append(i)
      print(to)
      #print(Cc)
      
      subject = ntpath.basename(sheet2).split(".")[0]
      sheet2_basename = ntpath.basename(sheet2)
      key="xaxisemail"
      
      
      write_content = ''
      #with open ((default_path+'tag_log.txt'),"r") as myfile:
       #if filename not in myfile.read():    
      for i in to:
        headers = {"x-api-key" : "WJpjmPIx2GaDknep0bM9u1TgOJEx3gSlaGK08kJ9"}
        API_Link = "https://1whnajy22a.execute-api.ca-central-1.amazonaws.com/default/sendEmail"
        API_ENDPOINT = API_Link+"?"+"to="+i+"&"+"subject="+subject+"&"+"filename="+sheet2_basename+"&key=xaxisemail"
        data_post = {"attachment": data_base64,
          "username": "xaxis.analyticstor@gmail.com",
          "password": "xaxis.com"}
     
   
        r = requests.post(url = API_ENDPOINT, data = json.dumps(data_post), headers = headers)  
        print(r.status_code)
        print(r.content)  
        if r.status_code == 200:
         write_content = ("########### "+datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S')+"  TO:"+i+"  ##########"+"\n"
                      +filename+"\n")
         #myfile.close()
         
         if write_content:
          with open ((default_path+'tag_log.txt'),"a+") as myfile:
            myfile.write(write_content)
         myfile.close()
        
     os.remove(default_path+filename)
     os.remove(sheet2)
      
      
    else:
     print("ye")
     os.remove(max(glob.glob(default_path+"*"), key=os.path.getctime))
   


 indexfile.close()