#!/usr/bin/Rscript --vanilla
# file extracts holiday from quotes table

Sys.setenv(TZ="UTC")
scriptname <- "nightlyRun_feedToDB.R" # for logging

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")
downloadbasedir <- "/home/voellenk/osorno_workdir/download"

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))

# ------------- some functions -------------------------------
echoStopMark <- function() {
  stop.time <- Sys.time()
  cat (paste(stop.time, scriptname, "stopped, duration:", round(as.numeric(difftime(stop.time, start.time, units="mins")),1), "mins\n"))
  cat ("----------------------------------------------------------------------\n")
}

option_list <- list(
  make_option(c("--exchange"), action="store", default="",
              help="download XTSX (ventures) or XTSE data [default %default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

if(opt$exchange == "XTSX") {
  osornodb <- osornodb_xtsx
  cat("processing data from toronto ventures exchange (XTSX).\n")
} else if (opt$exchange == "XTSE") {
  osornodb <- osornodb_xtse
  cat("processing data from toronto stock exchange (XTSE).\n")
} else {
  stop("exchange is not defined. Choose either --exchange=XTSX or --exchange=XTSE.\n")
}

# connect to database (keys are in secret file)
result <- tryCatch({ # on success, result contains the connection var con
  con <- dbConnect(MySQL(), 
                   user=osornodb$user, 
                   password=osornodb$password, 
                   dbname=osornodb$db, 
                   host=osornodb$host)
}, warning = function(w) {
  cat(paste0("WARNING: ", w$message, "\n"))
}, error = function(e) {
  cat(paste0("ERROR: ", e$message, "\n"))
}, finally = {
  if(exists("con")) {
    cat (paste(Sys.time(), "Connect to db", osornodb$db), "on host", osornodb$host, "\n")
  } else {
    cat ("unable to connect to database, stopping now.\n")
    echoStopMark()
    stop()
  }
})

# create volumesum table if not exists
createVolumeSumTable(con)

dates <- getDistinctDates(con)
dates$vsum <- 0
for (i in 1:nrow(dates)) { # this loop takes hours
  volsum <-  round(getVolumeSum(con, dates[i,1])[1,1]/1E6)
  cat(paste0("Date ", dates[i,1], " : ", volsum, "\n"))
  dates[i,"vsum"] <- volsum
  sql <- insertDayVolumeSumLine(con, dates[i,1], dates[i,2])
  dbSendQuery(con, sql)
}

# diconnect from DB
cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
dbres <- dbDisconnect(con)

echoStopMark()
