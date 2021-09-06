#!/usr/bin/Rscript --vanilla
# Script calculates VolumeSum of each date in quotes Table

Sys.setenv(TZ="UTC")
scriptname <- "s4_calcVolumeSum.R" # for logging

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/osorno_lib.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")
downloadbasedir <- "/home/voellenk/osorno_workdir/download"

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))

option_list <- list(
  make_option(c("--truncatevolumesum"), action="store_true", default=FALSE,
              help="empty volumesum table before new calculation [default %default]"),
  make_option(c("--exchange"), action="store", default="XTSX",
              help="download XTSX (ventures) or XTSE or XLON data [default %default]"),
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,
              help="Print extra output [default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

if(opt$exchange == "XTSX") {
  cat("processing data from Toronto Ventures exchange (XTSX).\n")
  osornodb <- osornodb_xtsx
} else if (opt$exchange == "XTSE") {
  cat("processing data from Toronto stock exchange (XTSE).\n")
  osornodb <- osornodb_xtse
} else if (opt$exchange == "XLON") {
  cat("processing data from London stock exchange (XLON).\n")
  osornodb <- osornodb_xlon
} else if (opt$exchange == "OTCB") {
  cat("processing data from OTCB stock exchange (OTCB).\n")
  osornodb <- osornodb_otcb
} else {
  stop("exchange is not defined. Choose either --exchange=XTSX or XTSE or XLON.\n")
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
if (opt$verbose) {
  cat("creating volumesum table if not exists.\n")
}
createVolumeSumTable(con)

if (opt$truncatevolumesum){
  if (opt$verbose) {
    cat("truncating volumesum table.\n")
  }
  truncateVolumeSumTable(con)
}

# fill the missing volumesums
dates <- getDistinctDates(con)
vsdates <- getDistinctVolsumDates(con)
missingdates <- setdiff(dates[,1], vsdates[,1])
cat("Inserting Volume Sum of all days in quote table:\n")
for (missingdate in missingdates) {
  volsum <-  round(getVolumeSum(con, missingdate)[1,1]/1E6)
  cat(paste0("Date ", missingdate, " : ", volsum, "\n"))
  sql <- insertDayVolumeSumLine(con, missingdate, volsum)
  dbSendQuery(con, sql)
}

# diconnect from DB
if (opt$verbose) {
  cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
}
dbres <- dbDisconnect(con)

echoStopMark()
