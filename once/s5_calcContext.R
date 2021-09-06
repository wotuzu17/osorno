#!/usr/bin/Rscript --vanilla
# Script calculates metrics for each symbol for each day
# this table gonna gets quite big

Sys.setenv(TZ="UTC")
scriptname <- "s5_calcContext.R" # for logging

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
suppressPackageStartupMessages(library(TTR))

option_list <- list(
  make_option(c("--truncatecontext"), action="store_true", default=FALSE,
              help="empty context table before new calculation [default %default]"),
  make_option(c("-x", "--exchange"), action="store", default="",
              help="download XTSX (ventures) or XTSE or XLON data [default %default]"),
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,
              help="Print extra output [default]")
)

opt <- parse_args(OptionParser(option_list=option_list))
first.day <- as.Date("2006-01-01")

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

# for debug
#osornodb <- osornodb_xtsx
#opt <- list()
#opt$verbose <- TRUE

# connect to database (keys are in secret file)
con <- connectToOsornoDb(osornodb)

# create volumesum table if not exists
if (opt$verbose) {
  cat("creating context table if not exists.\n")
}
createContextTable(con, context)

if (opt$truncatecontext == TRUE) {
  if (opt$verbose) {
    cat("truncating context table.\n")
  }
  truncateContextTable(con)
}

dates <- getDistinctDates(con)
dt1 <- as.Date(dates[,1])
dt <- dt1[dt1 > first.day]
holidays <- getLowVolumeDays(con)

# for (i in 1:length(dt)) {
for (i in 1:3) {
  this.tk <- getTickersOnDate(con, dt[i])
  cat(paste0(dt[i], ": ", length(this.tk), " tickers.\n"))
  dminus52w <- dt[i] - 365
  dminus30d <- dt[i] - 30
  for(j in 1:length(this.tk)) {
    tk <- this.tk[j]
    this <- sapply(context, names) # list of all elements in context list
    this$ticker <- tk
    this$date <- format(dt[i], "%Y-%m-%d")
    if ((j-1) %% 100 == 0) { # print every one-hundreth ticker 
      cat(paste0(j, ":", tk, " "))
    }
    usable <- TRUE # assume the symbol is usable, may be overridden later on
    ts <- getTickerDF1(con, tk, adjust=TRUE, NULL, dt[i])
    if(nrow(ts) > 0) {
      ts.xts <- xts(ts[,c("close", "volume")], order.by=as.Date(ts[,"date"])) 
    } else {
      usable <- FALSE
    }
    tsu <- getTickerDF1(con, tk, adjust=FALSE, NULL, dt[i])
    if (nrow(tsu) > 0) {
      tsu.xts <- xts(tsu[,c("close", "volume")], order.by=as.Date(tsu[,"date"]))
    } else {
      usable <- FALSE
    }
    if(abs(nrow(ts) - nrow(tsu)) > 2) {
      usable <- FALSE
    }
    if (usable) {
      this$usable <- 1
      this$first <- ts[1, "date"]
      this$last <- ts[nrow(ts), "date"]
      this$entries <- nrow(ts)
      this$adjustments <- nrow(ts[!is.na(ts[,"adj_factor"]), ])
      ts$ROC <- ROC(ts[,"close"])
      ts$zero <- ts$volume == 0 & (ts$ROC == 0 | is.na(ts$ROC))
      this$zerodata <- sum(ts$zero)
      if (sum(ts$zero) == nrow(ts)) { # only rows with no action
        this$usable <- 0
      }
      nr <- ts$ROC[!is.na(ts$ROC) & ts$ROC!=0]
      this$lpnr <- sum(nr>0)     # number of positive days
      this$lnnr <- sum(nr<0)     # number of negative days
      this$entries52w <- nrow(ts.xts[paste0(dminus52w, "/")])
      this$entries30d <- nrow(ts.xts[paste0(dminus30d, "/")])
      insertContextLine(this)
    } else {
      this$usable <- 0
      this$first <- "1900-01-01"
      this$firstdata <- "1900-01-01"
      this$last <- "1900-01-01"
      this$lastdata <- "1900-01-01"
      insertContextLine(this)
    }
    stop()
  }
  cat("\n\n")
}

# diconnect from DB
cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
dbres <- dbDisconnect(con)

echoStopMark()
