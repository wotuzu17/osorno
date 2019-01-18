#!/usr/bin/Rscript --vanilla
# script to create and fill stockquality table 
foo <- Sys.setlocale("LC_ALL", "en_US.UTF-8")
Sys.setenv(TZ="UTC")
scriptname <- "makeReport.R" # for logging
osornobasedir <- "/home/voellenk/osorno_workdir"

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(TTR))

option_list <- list(
  make_option(c("--exchange"), action="store", default="",
              help="download XTSX (ventures) or XTSE data [default %default]"),
  make_option(c("--day"), action="store", default="most_recent",
              help="make analyze for given YYYY-MM-DD [default %default]")
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

# ------------- some functions -------------------------------------------------------
echoStopMark <- function() {
  stop.time <- Sys.time()
  cat (paste(stop.time, scriptname, "stopped, duration:", round(as.numeric(difftime(stop.time, start.time, units="mins")),1), "mins\n"))
  cat ("----------------------------------------------------------------------\n")
}

databasedisconnect <- function(con) {
  cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
  dbres <- dbDisconnect(con)
}
# ------------------------------------------------------------------------------------

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

if (opt$day == "most_recent") {
  mrdate <- getMostRecentDateInQuotes(con)
  cat(paste0("Most recent date in quote is: ", mrdate, ".\n"))
  day <- NULL
  cat("Delete stockquality table.\n")
  dbres <- dropStockQualityTable(con)
  # create stockquality table if not exists
  cat ("Create stockquality table.\n")
  dbres <- createStockQualityTable(con)
} else {
  if (!(as.Date(opt$day) >= "2001-01-01")) {
    stop("Invalid day given.\n")
  }
  day <- opt$day
  cat(paste0("Delete stockquality_", day, " table.\n"))
  dbres <- dropStockQualityDayTable(con, day)
  cat(paste0("Create stockquality_", day, " table.\n"))
  dbres <- createStockQualityDayTable(con, day)
}

# get distinct symbols
syms <- getDistinctSymbolsinQuotes(con)[,1]

for (i in 1:length(syms)) {
  # get ts
  cat(paste0(syms[i], " "))
  ts <- getTickerDF(con, syms[i], NULL, day)
  if (nrow(ts) > 1) {
    from <- ts[1, "date"]
    to <- ts[nrow(ts), "date"]
    entries <- nrow(ts)
    adjustments <- nrow(ts[!is.na(ts[,"adj_factor"]), ])
    ts$ROC <- ROC(ts[,"close"])
    ts$zero <- ts$volume == 0 & (ts$ROC == 0 | is.na(ts$ROC))
    nr <- ts$ROC[!is.na(ts$ROC) & ts$ROC!=0]
    lpnr <- sum(nr>0)     # number of positive days
    lnnr <- sum(nr<0)     # number of negative days
    nr <- nr[nr<1 & nr>(-1)] # filter outliers
    spnr <- sum(nr[nr>0]) # sum of positive returns
    snnr <- sum(nr[nr<0]) # sum of negative returns
    if (sum(ts$zero) == nrow(ts)) {
      sql <- insertStockQualityLine(syms[i], day, from, '1000-01-01', to, '1000-01-01', 
                                    entries, 0, adjustments, sum(ts$zero), 
                                    0, 0, 0, 0, 1)
      dbSendQuery(con, sql)
    } else {
      # filter out leading and trailing zeros
      ts <- ts[min(which(ts$zero != TRUE)):max(which(ts$zero != TRUE)),]
      fromd <- ts[1, "date"]
      tod <- ts[nrow(ts), "date"]
      entriesd <- nrow(ts)
      zerod <- sum(ts$zero)
      tooshort <- ifelse(entriesd > 300, 0, 1)
      sql <- insertStockQualityLine(syms[i], day, from, fromd, to, tod, 
                                    entries, entriesd, adjustments, zerod, 
                                    lpnr, lnnr, spnr, snnr, tooshort)
      dbSendQuery(con, sql)
    }
    if (i %% 10 == 0) {
      cat("\n")
    }
  }
}

# diconnect from DB
cat ("Disconnect from DB.\n")
databasedisconnect(con)

# finish
echoStopMark()