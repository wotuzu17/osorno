#!/usr/bin/Rscript --vanilla
# script downloads unadjusted data for all tickers in DB
# credentials in file ~/.osornodb.R
Sys.setenv(TZ="UTC")
scriptname <- "s3_retrieveUADJ.R" # for logging

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/osorno_lib.R")
source("/home/voellenk/osorno_workdir/osorno/lib/raw_data_clean.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(Quandl))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))

option_list <- list(
  make_option(c("--truncatequotes"), action="store_true", default=FALSE,
              help="empty quotes_UADJ table before data import [default %default]"),
  make_option(c("--exchange"), action="store", default="XTSX",
              help="download XTSX (ventures) or XTSE data [default %default]"),
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
  stop("exchange is not defined. Choose either --exchange=XTSX, XTSE, XLON or OTCB.\n")
}

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
dbres <- createQuotesTable(con, "UADJ")

# empty quotes table
if (opt$truncatequotes == TRUE) {
  cat ("Emptying quotes_UADJ table now!\n")
  dbres <- truncateQuotesTable(con, "UADJ")
}

# see how many ticker symbols are already in adjusted quotes table
tickerdf <- getDistinctSymbolsinQuotes(con)
if(nrow(tickerdf) > 0) {
  if (opt$verbose == TRUE) {
    cat (paste0("There are ", nrow(tickerdf), " tickers included in (adjusted) quotes table\n"))
  }
  tickers <- tickerdf[,1]
} else {
  stop("There are no tickers in adjusted quotes table.")
}

# set Quandl api key
Quandl.api_key(quandlAPIkey)

# loop through each ticker and fill missing data into db
for (ticker in tickers) {
  compli <- getFullUADJQuandlData(ticker, opt$exchange)
  if(compli$success == TRUE) {
    removeSymFromUADJQuoteTbl(con, ticker)
    cat(paste0("deleted sym ", ticker, " from quotes_UADJ. "))
    # fill newly received cleaned data to table
    insertFullSymTS(con, ticker, compli$data, FALSE)
    cat(paste0("Filled ", nrow(compli$data), " rows from ", min(index(compli$data)), " into ", 
               max(index(compli$data))," to quotes_UADJ.\n"))
  } else {
    cat(paste0("ERROR: Could not receive valid full unadjusted data for sym ", ticker))
    cat(paste0(compli$desc, "\n"))
    cat("Leave quotes_UADJ table untouched.\n")
  }
}

# diconnect from DB
if (opt$verbose) {
  cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
}
dbres <- dbDisconnect(con)

echoStopMark()
