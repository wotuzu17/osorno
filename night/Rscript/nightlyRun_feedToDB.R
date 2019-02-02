#!/usr/bin/Rscript --vanilla
# Script downloads data from Quandl and stores it on download directory
# credentials in file ~/.osornodb.R
Sys.setenv(TZ="UTC")
scriptname <- "nightlyRun_feedToDB.R" # for logging

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")
downloadbasedir <- "/home/voellenk/osorno_workdir/download"

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))

option_list <- list(
  make_option(c("--truncatequotes"), action="store_true", default=FALSE,
              help="empty quotes table before data import [default %default]"),
  make_option(c("--exchange"), action="store", default="",
              help="download XTSX (ventures) or XTSE data [default %default]"),
  make_option(c("-v", "--verbose"), action="store_true", default=FALSE,
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
} else {
  stop("exchange is not defined. Choose either --exchange=XTSX or XTSE or XLON.\n")
}

# ------------- some functions -------------------------------
echoStopMark <- function() {
  stop.time <- Sys.time()
  cat (paste(stop.time, scriptname, "stopped, duration:", round(as.numeric(difftime(stop.time, start.time, units="mins")),1), "mins\n"))
  cat ("----------------------------------------------------------------------\n")
}
# returns numerically sorted files in downloaddir
orderFiles <- function(downloaddir) {
  files <- list.files(downloaddir, pattern="^\\d+_")
  if(length(files) >0) {
    nums <- as.numeric(sub("_.*", "", files))
    return(files[order(nums)])
  } else {
    cat ("ERROR in function orderFiles: downloaddir contains no valid symbol files.\n")
    return(NULL)
  }
}
# ------------------------------------------------------------

if (opt$verbose == TRUE) {
  str(opt)
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

# create quotes table in db if not exists
dbres <- createQuotesTable(con)

# empty quotes table
if (opt$truncatequotes == TRUE) {
  cat ("emptying quotes table now!\n")
  dbres <- truncateQuotesTable(con)
}

# see how many ticker symbols are already in db
tickers <- getDistinctSymbolsinQuotes(con)
if (opt$verbose == TRUE) {
  cat (paste0("There are ", nrow(tickers), " tickers included in quotes table\n"))
}

# use most recent download dir
ddirs <- dir(downloadbasedir, pattern=sprintf("^%s_\\d{8}_\\d{6}$", opt$exchange))
if (length(ddirs > 0)) {
  mrddir <- sort(ddirs, decreasing=TRUE)[1]
  this.downloaddir <- paste(downloadbasedir, mrddir, sep="/")
  if (opt$verbose == TRUE) {
    cat (paste("using dir", this.downloaddir, "for database import\n"))
  }
  symfiles <- orderFiles(this.downloaddir)
  for (i in 1:length(symfiles)) {
    this.symfile <- symfiles[i]
    this.sym <- sub(".Rdata$", "", sub("^\\d+_", "", symfiles[i]))
    ts <- data.frame()
    load(paste(this.downloaddir, this.symfile, sep="/")) # this loads a ts data.frame into environment
    if (nrow(ts) > 0) {
      # feed to db
      if (opt$verbose == TRUE) {
        cat(paste0("feed ", this.sym, " to quotes table.\n"))
      }
      for(j in 1:nrow(ts)) {
        dbSendQuery(con, insertQuoteLine(this.sym, ts[j,]))
      }
    } else {
      cat(paste0("ERROR: file ", this.sym, " doesn't contain time series xts.\n"))
    }
  }
} else {
  cat (paste("ERROR: Could not find any download directories in dir", downloadbasedir, "\n"))
  dbres <- dbDisconnect(con)
  echoStopMark()
  stop("quitting now\n")
}


# diconnect from DB
if (opt$verbose) {
  cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
}
dbres <- dbDisconnect(con)

echoStopMark()
