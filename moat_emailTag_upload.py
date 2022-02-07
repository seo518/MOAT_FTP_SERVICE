# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 20:36:49 2020

@author: Shweta.Anjan
"""
import boto3
import pandas as pd
import numpy as np
import base64
import os
import json
import requests
import urllib3
import re
import datetime
import io


def get_from_emailID(s, email_part, access_key):
    msg = s3_load_email(s, access_key)
    part = msg[email_part]
    return(part)

def get_file_from_link(msg):
    import re
    url= re.search("(?P<url>https?://[^\s]+)", str(msg).replace("\n","")).group('url').replace("=","").replace("3D","=")
    #r= requests.get(url)
    http = urllib3.PoolManager()
    r = http.request("GET", url)
    d = r.headers['content-disposition']
    fname = re.findall("filename=(.+)", d)[0]
    return(r.data,fname)

def search_email_body(s, access_key, search_key):
    import re
    msg = s3_load_email(s, access_key)
    search_return=[]
    for part in msg.walk():
        if part.get_content_type() == 'text/plain':
            reg = r"(?<=" + search_key.lower() +":"+r").*"
            string= part.get_payload().lower().replace(' ','')
            search_return = re.findall(reg, string)
            if search_return:
                search_return= re.sub('\\r','',search_return[0])
                return(search_return)
            else:
                return(None)     
    
def s3emails_get_attachment(s, access_key): 
       
    msg = s3_load_email(s, access_key)
    date = format_date(s) 
    
    if 'download' in str(msg).lower():
        attachement,fname = get_file_from_link(msg)
        filename = fname
        filename=date+'_'+filename+"."+fname.split(".")[-1]
    else:
        for part in msg.walk():            
            if part.get_filename() is not None:
                name=part.get_filename().replace("\r",'').replace("\n",'').replace(' ','')
                attachement = part.get_payload(decode=True)
                filename=date+'_'+name
                filename = filename.translate ({ord(c): "_" for c in "!@#$%^&*()[]{};:,/<>?\|`~-=+"})
    if filename is None:
        print(0)  
    return(filename,attachement)



def format_date(file):
    import datetime
    date = file.last_modified.replace(tzinfo=None)
    date = str(datetime.datetime(date.year,date.month,date.day))
    date = date.replace("00:00:00","").replace("-","").replace(" ","")
    return(date)


def s3_load_email(s , access_key):
    import email     
    
    if s.key:
        body = s.get()['Body'].read()
        msg = email.message_from_bytes(body)        
        
    else:
        with open(s, 'rb') as fp:
            msg = email.message_from_binary_file(fp)
    
    if access_key in str(msg):
        return(msg)
    else:
        print("No key available")
        return(None)

def get_aws_Key(name, directory):
    print(name)
    d = os.listdir(directory)
    for line in d:
        print(line)
        if name in line:
            s3Key = pd.read_csv(directory + line)
            return s3Key
        
def initiate_session(access_id, secret_key):
    session = boto3.session.Session(aws_access_key_id = access_id,
                      aws_secret_access_key = secret_key)
    return session

def list_s3_files(bucket, session , filter_string = None):
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket) 
    if filter_string is None:
        objs = list(bucket.objects.all())
    else:
        objs = list(bucket.objects.filter(Prefix=filter_string))
    return(objs)

  

#Description
#This config script to specifically process and email moat tags 
#This config scriptis specific for Moat DV360 tags
#Calls upon the underlying fucntions to: 
                           #List S3 emailBucket files, 
                           #scan file list, 
                           #get email attachment 
                           #process files
                           #email file to requied ID list

#config parameter file
#@param: Input1: arg file with following input: @fields
                                         #1. download_key_word : file keyword to filter in email bucket
                                         #2. time window: date range for the file upload
                                         #3. rename: if the file needs to be renamed, prefix_filename
                                         #4. search: keywords in the email to search for

#File paths to the access creds to the S3 bucket
s3KeyMail = pd.read_csv("C:/Users/shweta.anjan/OneDrive - insidemedia.net/AWS/aws_library/creds/xaxismailreceipt.csv")
s3KeyMas = pd.read_csv("C:/Users/shweta.anjan/OneDrive - insidemedia.net/AWS/aws_library/creds/masterkey-apnreferencefields.csv")


#Send email lambda API link
API_Link = "https://1whnajy22a.execute-api.ca-central-1.amazonaws.com/default/sendEmail"
#API key
headers = {"x-api-key" : "7aVb78nFbl6wrKjyLArz98jdVk7VLuSP7ruqn8Mf"}


##Main()
#list the objects from the email bucket
yourBucket = list_s3_files(bucket = 'xaxisemailreceipts', session =initiate_session(access_id = s3KeyMail.iloc[0]["Access key ID"], 
                               secret_key = s3KeyMail.iloc[0]["Secret access key"]) , filter_string = None)

#list the objects from the config bucket
configList = list_s3_files(bucket = 'xax-configs1',  session = initiate_session(access_id = s3KeyMas.iloc[0]["Access key ID"], 
                               secret_key = s3KeyMas.iloc[0]["Secret access key"]) , filter_string = "moat/")

logSession = initiate_session(access_id = s3KeyMas.iloc[0]["Access key ID"], 
                               secret_key = s3KeyMas.iloc[0]["Secret access key"])

logSession = logSession.resource('s3')

logSession = logSession.Object('xax-configs1', 'moat/tag_log_index.txt')

# get the email key and the variables for the config file
for i in configList:
    if all(x in i.key for x in ['config','tag']):
        var = i.get()['Body'].read().decode('utf-8')
        var = pd.read_csv(io.StringIO(var), index_col = 'fields')
        var.index = [i.strip() for i in list(var.index)]
        var.fillna('', inplace = True)
        
    if 'email_key' in i.key:
        email_key = i.get()['Body'].read().decode('utf-8')  
    
    if all(x in i.key for x in ['log','tag']):  
        log = i.get()['Body'].read().decode('utf-8')       


#Get date range
time = datetime.datetime.now() - datetime.timedelta(days= int(var.loc['time_window', 'values']))

bucketList=[]
#filter files that are in date range
for s in yourBucket:
    if s.last_modified.replace(tzinfo=None) > time :
        if str(s) not in log:
            bucketList.append(s)
            log = log + '\n' +str(s)+"  #### "+s.last_modified.replace(tzinfo=None).strftime('%Y-%m-%d  %H:%M:%S')+"###"
            logSession.put(Body= log.encode('utf-8'))

# Loop through the list for files in date range 
for s in bucketList:
    dv_360_df=pd.DataFrame(columns=[i.strip() for i in var.loc['cols', 'values'].split(';')])  
    #get email body
    msg = s3_load_email(s, access_key=email_key)
    sub=msg['Subject']
    sub=sub.replace(' ','_').replace("\r\n",'')
    pattern = re.compile('[^A-Za-z0-9_]')
    sub = pattern.sub('',sub)
    
    #Get the files with filter ke words
    if all(x in sub.lower() for x in var.loc['download_key_word', 'values'].split('&')):
        print('header present')
        if var.loc['rename', 'values'] != '':
            print('y')
            title=var.loc['rename', 'values']+"_"+('_'.join(var.loc['download_key_word', 'values'].split('&')))
            print(title)
        else:
            title = None
            
        #Fetch attachement from filtered files    
        filename, data = s3emails_get_attachment(s, email_key)
        ext = filename.split(".")
        data = io.BytesIO(data)
        if 'xls' in ext[-1]:
            tag_sheet = pd.read_excel(data, skiprows=int(var.loc['skiprows', 'values']))
        else:
            tag_sheet = pd.read_csv(data, skiprows=int(var.loc['skiprows', 'values']))
            
        temp_search = search_email_body(s, email_key, var.loc['url_search', 'values'])
        if temp_search:
            landing_page_url= temp_search
        print('got url') 
        
        #Create the dv360 tag dataset
        dv_360_df['Creative name'] = tag_sheet['Placement Name']+"_MOAT"
        dv_360_df['Requires HTML5 ("Yes" or "No")'] = 'Yes' 	
        dv_360_df['Landing page URL'] = landing_page_url
        dv_360_df['Third-party tag'] = [i.replace(var.loc['moat_src', 'values'],var.loc['moat_replace', 'values']) for i in tag_sheet['Iframes/JavaScript Tag']]
        dv_360_df['Third-party tag'] = [i.replace(var.loc['tag_src', 'values'],var.loc['tag_replace', 'values']) for i in dv_360_df['Third-party tag']]
        dv_360_df['Dimensions (width x height)'] = tag_sheet['Dimensions']
        dv_360_df = dv_360_df.replace(np.nan,'', regex=True)
        csv_buffer = io.BytesIO()
        dv_360_df.to_excel(csv_buffer, encoding = 'utf-8', index=False)
        
        #convert data set to base64
        data_base64 = base64.b64encode(csv_buffer.getvalue()).decode('utf-8')
        
        #Create a list of emailID's to send the tag file
        to = []
        email_list = get_from_emailID(s, 'from',email_key)
        Cc= get_from_emailID(s, 'Cc',email_key)
        if Cc:
            email_list = email_list+ ',' + Cc
        email_list = email_list.replace(' ','').replace('\r\n','').replace('\t','')
        email_list = email_list.split(',')
        for i in email_list:
            if "<" in i:
                i = re.findall("(?<=\<)(.*?)(?=\>)",i)[0]
            if i:
                print(i)
                to.append(i)
                print(to)

        subject = ext[0]+'_'+'_'.join(var.loc['download_key_word', 'values'].split('&'))
        print ('filename ='+filename)
        
        #Post the attachemnt to the send email API
        for i in to:
            API_ENDPOINT = API_Link+"?"+"to="+i+"&"+"subject="+subject+"&"+"filename="+filename+"&key=xaxisemail"
            data_post = {"attachment": data_base64,
                         "username": "xaxis.analyticstor@gmail.com",
                         "password": "xaxis.com"}
 
            http = urllib3.PoolManager()

            r = http.request("POST", API_ENDPOINT, body=json.dumps(data_post), headers = headers)
            print(r.status)
            print(r.data)
            # r = requests.post(url = API_ENDPOINT, data = json.dumps(data_post), headers = headers)  
            # print(r.status_code)
            # print(r.content)  
            