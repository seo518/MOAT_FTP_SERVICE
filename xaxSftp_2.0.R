### Get directory list

#Get R curl 
local({
  r <- getOption("repos")
  r["CRAN"] <- "https://cran.rstudio.com/"
  r["CRANextra"] <- "http://www.stats.ox.ac.uk/pub/RWin"
  options(repos = r)
})

if(!any(grepl("RCurl",installed.packages()))){
  install.packages("RCurl",repos = "http://cran.us.r-project.org")
}
if(!any(grepl("data.table",installed.packages()))){
  install.packages("data.table",repos = "http://cran.us.r-project.org")
  
}

if(!any(grepl("zeallot",installed.packages()))){
  install.packages("zeallot",repos = "http://cran.us.r-project.org")
  
}

if(!any(grepl("tools",installed.packages()))){
  install.packages("tools",repos = "http://cran.us.r-project.org")
  
}

if(!any(grepl("R.utils",installed.packages()))){
  install.packages("R.utils",repos = "http://cran.us.r-project.org")
  
}

if(!any(grepl("gsubfn",installed.packages()))){
  install.packages("gsubfn",repos = "http://cran.us.r-project.org")
  
}




#Get argument from command line
args = commandArgs(trailingOnly=TRUE)
file_path <- args[1]
# From_Path <- args[2]
# default_path<- args[3]
#file_path <- "C:/Users/Administrator/Documents/rscripts/XaxFTPRLib/config files/ftp_config.txt"
file_path <- gsub("\\\\", "/",file_path)

# #to_ftp_key_file <- "C:\\Users\\shweta.anjan\\Desktop\\FTP_Form_SFTP_XaXCan.txt"
# to_ftp_key_file <- gsub("\\\\", "/",to_ftp_key_file)
# #From_Path <- "C:\\Users\\shweta.anjan\\Desktop\\FTP_Form_SFTP_Videology.txt"
# 
# #default_path <- "C:\\Users\\shweta.anjan\\Documents\\test_temp"
# default_path <- gsub("\\\\", "/",default_path)
# cat(default_path)

#### Function Defination #####

####1. Upload a file to ftp #####
xax.Upload <- function(file,
                       url, key, dir){
  require(RCurl)
  require(bitops)
  
  ### tryCatch statement for errors usually would be caused by how people would paste their files (if they paste manually)
  tryCatch({
    
    ### Split the file name with backslash or forward slash, then unlisting it to make it a normal vector
    file.name <- unlist(strsplit(file,split = "/|\\\\"))
    
    ### Get the last item in the vector 
    file.name <- file.name[length(file.name)]
    
    ### Load required package RCurl for FTP commands
    require(RCurl)
    
    ### Create options, putting logical state to TRUE to create missing dirs if stated within the filename
    opts <- list(ftp.create.missing.dirs = TRUE)
    
    ### If upload file is a directory/folder, return an error print
    if(dir.exists(file)){
      return(print("Cannot upload directories"))
    } else if(dir == 0){
      dir <- paste0(url,"/",file.name)
    } else {
      ### If todir is added, construct the string to add the todir .arg and the file.name together
      if (startsWith(dir,"/")) {
        dir <- paste0(url,dir,"/")
      } else {
        dir <- paste0("/",url,dir,"/")
        
      }  
      
    }
    
    ### Push upload request to the FTP and return a logical
    x <- ftpUpload(what = file,
                   userpwd = key, asText = F,
                   to = paste0(dir,file.name),
                   .opts = opts)
    
    ### FTP says that 0 = TRUE and 1 = FALSE, simply invert the output
    ### If upload is successful, it will return a TRUE
    ### If upload fails, it will return a FALSE
    return(!as.logical(x))
    
  }, error = function(x){
    
    ### Return invalid file name to user
    print("Invalid file name")
  })
}




#### 2. Get the FTP credentials from form into a hash##########

get_Credentials <- function(file, action) {
  config <- read.csv(file)
  config[,1] <- tolower(config[,1])
  row.names(config) <- config[,1]
  colnames(config) <- tolower(colnames(config))
  
  #ftp <- "moat"
  ## check if its a SFTP or FTP 
  if(!grepl("ftp://", config[action,"host"])){
    warning("[url] must have [ftp://] in the beginning")
    if(config[action,"port"]==22){
      url <- paste0("sftp://", config[action,"host"]) 
    } else {
      url <- paste0("ftp://", config[action,"host"])   
    }
    
  } else {
    if(config[action,"port"] == 22){
      gsub("ftp", "sftp", config[action,"host"])
    }
  }
  
  if(is.null(url)){
    warning("url = NULL")
  }
  
  
  if (is.na(config[action,"directory"])) {
    todir <- 0
  } else {
    todir <- config[action,"directory"]
  }
  
  # key <- paste0(config[ftp,"username"],":",config[ftp,"password"])
  
  
  return(list(gsub(" ","",url), paste0(toString(config[action,"username"]),":",toString(config[action,"password"])), 
              toString(todir), toString(config[action,"pattern"]),toString(config[action,"days"]),
              toString(config[action,"header"]), toString(config[action,"consolidate"]),
              gsub("\\\\","/",toString(config[action,"download_path"]))))
  
}

##3 Create a Log and delete the files from the Outlook drop
xax.log<- function(url, path){
  require(RCurl)
  require(bitops)
  
  if (is.null(path)){
    path <- path.expand("~/")
  } else{
    path <- gsub("\\\\", "/",path)
  }
  # ### If upload file is a directory/folder, return an error print
  # if(dir == 0){
  #   dir <- paste0(url,"/")
  # } else {
  #   ### If dir is added, construct the string to add the todir .arg and the file.name together
  #   if (startsWith(dir,"/")) {
  #     dir <- paste0(url,dir,"/")
  #   } else {
  #     dir <- paste0(url,"/",dir,"/")
  #     
  #   }  
  # }
  
  #x <- getURL(dir, userpwd=key, dirlistonly=TRUE)
  x <- grep(list.files(path, full.names = FALSE), pattern = 'log.txt', inv=T, value=T)
  #x <- unlist(strsplit(x,split="\n"))
  #x <- x[grepl(paste0(letters,collapse = "|"),x, ignore.case = TRUE)]
  
  log_file <- paste0(path,"/", "log.txt")
  g <- paste0('\n',"######", Sys.Date(),"  TO:  ",url,"  ######")
  write(g, log_file, append= TRUE)
  y <- unlist(readLines(log_file))
  y <- y[!grepl("######", y)]
  x <- gsub("\\r", "",x)
  diff  <- setdiff(x,y)
  write(diff, log_file, append = TRUE)
  for (i in x){
    print (i)
    if (file.exists(paste0(path,"/",i))) {file.remove(paste0(path,"/",i))}
  }
}  



######### 4. Pull files from FTP/ Folder ######
xax.Pull <- function(url, key, dir, pattern, days, header, path, consolidate){
  require(RCurl)
  require(bitops)
  require(data.table)
  require(tools)
  require(R.utils)
  require(gsubfn)
  print(url)
  print(dir)
  print(key)
  print(pattern)
  print(days)
  print(header)
  print(path)
  print(consolidate)
  
  ### If upload file is a directory/folder, return an error print
  if(dir == 0){
    dir <- paste0(url,"/")
  } else {
    ### If todir is added, construct the string to add the todir .arg and the file.name together
    if (startsWith(dir,"/")) {
      dir <- paste0(url,dir,"/")
    } else {
      dir <- gsub(" ","",paste0(url,"/",dir,"/"))
      
    }  
  }
  print(dir)
  
  x <- grep(pattern,unlist(strsplit(getURL(dir, userpwd=key, dirlistonly=TRUE),split = "\\\r\\\n")), value =TRUE)
  x <- sort(x, decreasing = TRUE)
  if (!is.null(x)){
    print('y')
    
  }
   
  if (days == 'latest'){
    x <- x[1:2]
  } else{
    x <- x[1:days]
  } 
  x<-x[!is.na(x)]
  
  if (is.null(path)){
    path <- path.expand("~/")
  } else{
    path <- gsub("\\\\", "/",path)
  }
  
  header <- gsub(" ", ".", unlist(strsplit(header, ",")))
  y <- data.frame(colnames(header))
  #consolidate = "yes"
  num <- c()
  consolidate <- tolower(consolidate)
  
  for(i in x){
    bin <- getBinaryURL(paste0(dir,i), userpwd = key)
    writeBin(bin,paste0(path,"/",i))
    name.file <- paste0(path,"/",i)
    base <-file_path_sans_ext(file_path_sans_ext(i))
    num <- append(num,strapply(base, "\\d+", as.numeric))
    if (grepl(".csv", name.file) == TRUE) {
      df <- read.csv(file = name.file, header = TRUE)
      file.remove(name.file)
      
    } else {
      df <- read.delim2(file = name.file, header = TRUE)
      file.remove(name.file)
    }
    
    if (!is.null(header)){
      df <- unique(df[,header])
    }
    y <- unique(rbind(y,df))
    
  }
  
  if (grepl("yes", consolidate) == TRUE){
    write.csv(y,file = paste0(path,"/",base,"_",num[[1]],".csv"))
    
  } else {
    write.csv(df,file = paste0(path,"/",base,".csv"))
  }
  print ("Done Pull")
  return()
  
  
}



#####check if need to push from FTP or from local folder
if (dir.exists(file_path) && !file.exists(file_path)){ 
  if (grepl("*\\w$",file_path)){
    file_path<- paste0(file_path,"/")
  }
  file_list <- list.files(path = file_path, full.names = TRUE)
  
} else if(file.exists(file_path) && !dir.exists(file_path)) {
  require(zeallot)
  c(fromUrl, fromKey, fromDir,fromPattern, fromDays,fromHeader, fromConsolidate, fromDownload_path) %<-% get_Credentials(file_path, "from")
  print (fromDownload_path)
  print(fromUrl)
  print(fromKey)
  print(fromDir)
  print(fromPattern)
  print(fromDays)
  print(fromHeader)
  print(fromConsolidate)
  xax.Pull(fromUrl,fromKey,fromDir,fromPattern,fromDays,fromHeader,fromDownload_path,fromConsolidate)
  
  file_list <- list.files(path = fromDownload_path, full.names = TRUE)
}
  

file_list <- file_list[lapply(file_list,function(x) length(grep("log.txt",x,value=FALSE))) == 0]

c(toUrl, toKey, toDir,toPattern, toDays, toHeader, toConsolidate, toDownload_path)  %<-% get_Credentials(file_path,"to")

for (i in file_list){
  print(i)
  x <- xax.Upload(i, toUrl, toKey, toDir)
  if (isTRUE(x)){
    print ("upload successfull")
  }else{
    "failed:Invalid file name"
  }
}



#Log the push
xax.log(toUrl,fromDownload_path)





# 
# ###does the pc have a connection?
# connectivity.test <- function() {
#   
#   ### Test OS type, if windows then use ipconfig, if not then use ifconfig
#   ifelse(.Platform$OS.type=="windows", 
#          ipmessage.2 <- system("ipconfig", intern = TRUE),
#          ipmessage.2 <- system("ifconfig", intern = TRUE))
#   
#   ### Create valid IP numbers to search in the ipconfig response
#   validIP <- "((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)[.]){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
#   
#   ### If any grep is true, then return a TRUE
#   ### TRUE = computer is connected to the internet
#   ### FALSE = computer has no internet
#   return(any(grep(validIP, ipmessage.2)))
# }
# 




