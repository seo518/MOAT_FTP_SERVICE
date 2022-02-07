# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 20:36:49 2020

@author: Shweta.Anjan
"""
import boto3
import pandas as pd
import os
import urllib3
import re
import datetime
import io

def get_file_from_link(msg):
    import re
    url= re.search("(?P<url>https?://[^\s]+)", str(msg).replace("\n","")).group('url').replace("=","").replace("3D","=")
    #r= requests.get(url)
    http = urllib3.PoolManager()
    r = http.request("GET", url)
    d = r.headers['content-disposition']
    fname = re.findall("filename=(.+)", d)[0]
    return(r.content,fname)

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

def ftpS_connect (login,usn,pwd,cwd,port):
    if int(port) == 22:
        import pysftp    
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None   
        sftp = pysftp.Connection(host=login, username=usn, password=pwd, cnopts=cnopts) 
        sftp.chdir(cwd)
        return(sftp)
    else:      
        import ftplib
        ftp = ftplib.FTP(login)
        ftp.login(usn, pwd)
        ftp.cwd(cwd)
        return(ftp)
        
        


            

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
##Main()

#email_key = "C:/Users/shweta.anjan/OneDrive - insidemedia.net/AWS/4c4172306742283679696f657c566c23.txt"
s3KeyMail = pd.read_csv("C:/Users/shweta.anjan/OneDrive - insidemedia.net/AWS/aws_library/creds/xaxismailreceipt.csv")
s3KeyMas = pd.read_csv("C:/Users/shweta.anjan/OneDrive - insidemedia.net/AWS/aws_library/creds/masterkey-apnreferencefields.csv")
# bucketConfig = 'xax-configs1'
# bucketMail = 'xaxisemailreceipts'

#call function that process the config file
#@input param: arg file
#@output: fileds from the arg file 
#headers = {"x-api-key" : "WJpjmPIx2GaDknep0bM9u1TgOJEx3gSlaGK08kJ9"}
API_Link = "https://1whnajy22a.execute-api.ca-central-1.amazonaws.com/default/sendEmail"
headers = {"x-api-key" : "7aVb78nFbl6wrKjyLArz98jdVk7VLuSP7ruqn8Mf"}
#list the objects from the email bucket

yourBucket = list_s3_files(bucket = 'xaxisemailreceipts', session =initiate_session(access_id = s3KeyMail.iloc[0]["Access key ID"], 
                               secret_key = s3KeyMail.iloc[0]["Secret access key"]) , filter_string = None)

logSession = initiate_session(access_id = s3KeyMas.iloc[0]["Access key ID"], 
                               secret_key = s3KeyMas.iloc[0]["Secret access key"])
logList = list_s3_files(bucket = 'xax-configs1',  session = logSession , filter_string = "moat/")

for i in logList:
    if all(x in i.key for x in ['config','ftp']):
        var = i.get()['Body'].read().decode('utf-8')
        var = pd.read_csv(io.StringIO(var), index_col = 'fields')
        var.index = [i.strip() for i in list(var.index)]
        var.fillna('', inplace = True)
    if 'email_key' in i.key:
        email_key = i.get()['Body'].read().decode('utf-8')          

bucketList=[]
time = datetime.datetime.now() - datetime.timedelta(days= int(var.loc['time_window', 'values']))

for s in yourBucket:
    if s.last_modified.replace(tzinfo=None) > time :
        bucketList.append(s)



host = var.loc['to_host', 'values'].strip()
usn = var.loc['to_usn', 'values'].strip()
pwd = var.loc['to_pwd', 'values'].strip()
cwd = var.loc['to_dir', 'values'].strip()
port = var.loc['to_port', 'values'].strip()

# Loop through the list and filter for specific file     
for s in bucketList: 
    #load the email body
    msg = s3_load_email(s, access_key=email_key)
    #Get subject and  remove special characters
    sub=msg['Subject']
    sub=sub.replace(' ','_').replace("\r\n",'')
    pattern = re.compile('[^A-Za-z0-9_]')
    sub = pattern.sub('',sub)
    print(s)
    print(sub)
    title = None
    #Rename the file 
    if all(x in sub.lower() for x in var.loc['download_key_word', 'values'].split('&')):
        print('header present')
        for x in var.loc['rename','values'].split(':'):
                if x in sub.lower():
                    title =s.last_modified.replace(tzinfo=None).strftime('%Y%m%d')+"_"+x+"_"+('_'.join(var.loc['download_key_word', 'values'].split('&')))
                    print(title) 
                    if 'v2' in title:
                        break
        #Get attachement from email          
        filename, data = s3emails_get_attachment(s, email_key)
        ext = filename.replace(" ","").split(".")
        data_bytes = io.BytesIO(data)
        title = title+'.'+ext[-1]
        
        #connect to SFTP or FTP
        connect = ftpS_connect(host, usn, pwd, cwd, port)   
        #Add file to FTP
        connect.putfo(data_bytes,title)
        connect.close()
   
        