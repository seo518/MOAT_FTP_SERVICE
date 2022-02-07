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
  install.packages("zeallot",repos = "http://cran.us.r-project.org")  
    
}

require(zeallot)
require(tools)  

path <- "C:\\Users\\miguel.ajon\\Documents\\Outlook_Drop"  
  
#path <- 'C:\\Users\\shweta.anjan\\Documents\\moat_reports'

file.list <- list.files(path =path, pattern = "MOAT|Mapping" , full.names = FALSE)
prefix <- "Xax_Can_Moat_Mapping_Report_"


for ( i in file.list){
  id <- as.numeric(gsub("([0-9]+).*$","\\1", i))
  ext <- file_ext(i)

if (grepl("v2",i,ignore.case = TRUE)) {
  new_name <- paste0( prefix,"DBM_V2_", id, ".",ext) 
} else if (grepl("dbm" ,i,ignore.case = TRUE)){
  new_name <- paste0( prefix,"DBM_", id, ".",ext)
} else if (grepl("appnexus" ,i,ignore.case = TRUE)) {
  new_name <- paste0( prefix,"AppNexus_", id, ".",ext)
} else if (grepl("ttd" ,i,ignore.case = TRUE)) {
  new_name <- paste0( prefix,"TTD", id,".",ext)
}

old_file <- paste0(path,"\\",i)
new_file <- paste0(path,"\\","Moat_Api_report","\\",new_name)
#new_file <- paste0(path,"\\",new_name)
file.rename(from = old_file, to = new_file)

}
