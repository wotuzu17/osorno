#!/usr/bin/Rscript --vanilla
# script to get feed missing data into quotes table
Sys.setenv(TZ="UTC")
scriptname <- "nightlyRun_feedToDB.R" # for logging

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")
source("/home/voellenk/osorno_workdir/osorno/lib/raw_data_clean.R")
downloadbasedir <- "/home/voellenk/osorno_workdir/download"

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(Quandl))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(TTR))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))

option_list <- list(
  make_option(c("--numberofsyms"), type="integer", default=0, 
              help="For testing. Only download numberofsyms symbols [default %default]",
              metavar="number"),
  make_option(c("--exchange"), action="store", default="",
              help="download XTSX (ventures) or XTSE or XLON data [default %default]"),
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,
              help="Print extra output [default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

if(opt$exchange == "XTSX") {
  osornodb <- osornodb_xtsx
  cat("processing data from toronto ventures exchange (XTSX).\n")
} else if (opt$exchange == "XTSE") {
  osornodb <- osornodb_xtse
  cat("processing data from toronto stock exchange (XTSE).\n")
} else if (opt$exchange == "XLON") {
  osornodb <- osornodb_xlon
  cat("processing data from London stock exchange (XLON).\n")
} else if (opt$exchange == "OTCB") {
  osornodb <- osornodb_otcb
  cat("processing data from OTCB.\n")
} else {
  stop("exchange is not defined. Choose either --exchange=XTSX or XTSE, XLON or OTCB.\n")
}

# ------------- some functions -------------------------------------------------------
echoStopMark <- function() {
  stop.time <- Sys.time()
  cat (paste(stop.time, scriptname, "stopped, duration:", round(as.numeric(difftime(stop.time, start.time, units="mins")),1), "mins\n"))
  cat ("----------------------------------------------------------------------\n")
}

# ------------------------------------------------------------------------------------
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

# get most recent date in quotes table
mrdate <- getMostRecentDateInQuotes(con)
if (is.null(mrdate)) {
  cat ("can't determine most recent date in quotes table, stopping now.\n")
  echoStopMark()
  stop()
}
if (opt$verbose == TRUE) {
  cat (paste0("The most recent date in quotes table is ", mrdate, "\n"))
}

# see how many ticker symbols in db have dates until 30 days before mrdate
tickers <- getActiveTickers(con, as.character(as.Date(mrdate)-30))
if (opt$verbose == TRUE) {
  cat (paste0("There are ", length(tickers), " active tickers included in quotes table\n"))
}

# set Quandl api key
Quandl.api_key(quandlAPIkey)

# create download base directory if not exists
if (!dir.exists(downloadbasedir)) {
  dir.create(downloadbasedir)
}

# create download dir for this download 
this.downloaddir <- paste(downloadbasedir, 
                          paste0(opt$exchange, "_", format(start.time, "%Y%m%d_%H%M%S_INC")), 
                          sep="/")
dir.create(this.downloaddir)

# loop through each ticker and fill missing data into db
for (ticker in tickers) {
  this.mrdate <- getMostRecentTickerDate(con, ticker)
  this.mrdate.plus1 <- as.character(as.Date(this.mrdate) + 1)
  cat (paste0("Sym ", ticker, " last date :\t", this.mrdate, ". "))
  # get data for this ticker 
  from <- as.character(as.Date(this.mrdate)-5)
  to <- as.character(as.Date(start.time))
  li <- getPartCleanedQuandlData(ticker, opt$exchange, from, to)
  if (li$success == TRUE) {
    if (nrow(li$data[!is.na(li$data[,6]),]) == 0) {
      # no adjustment in data set. Only fill remaining rows
      ts <- li$data[paste0(this.mrdate.plus1, "/")] # subset for missing days
      if (nrow(ts) > 0) {
        insertFullSymTS(con, ticker, ts)
        cat(paste0("Filled missing ", nrow(ts), " rows from ", min(index(ts)), " to ", max(index(ts)), " to quotes table.\n"))
      } else {
        cat(paste0("No new data for ", ticker, ". Last day was ", max(index(li$data)), ".\n"))
      }
    } else {
      # adjustment took place recently. Delete entire sym from quotes table and re-fill everything
      compli <- getFullCleanedQuandlData(ticker, opt$exchange)
      if (compli$success == TRUE) {
        # delete sym data in table
        removeSymFromQuoteTbl(con, ticker)
        cat(paste0("deleted sym ", ticker, " from quotes."))
        # fill newly received cleaned data to table
        insertFullSymTS(con, ticker, compli$data)
        cat(paste0("refilled ", nrow(compli$data), " rows from ", min(index(compli$data)), " to ", max(index(compli$data))," to quotes.\n"))
      } else {
        cat(paste0("ERROR: Could not receive valid full data for sym ", ticker))
        cat(paste0(compli$desc, "\n"))
        cat("Leave quotes table untouched.\n")
      }
    }
  } else {
    cat(paste0(" ERROR: No Data from ", from, " to ", to, ".\n"))
    cat(paste0(li$desc, "\n."))
  }
}

# fill the missing volumesums
dates <- getDistinctDates(con)
vsdates <- getDistinctVolsumDates(con)
missingdates <- setdiff(dates[,1], vsdates[,1])
cat("Inserting Volume Sum of newly received days:\n")
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

