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
source("/home/voellenk/osorno_workdir/osorno/lib/db_01_connect_disconnect.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_02_quotes_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_03_volumesum_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_06_context_table.R")

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

#for debug
#opt <- list()
#opt$exchange <- "XTSX"

# determine osornodb and tickers file from exchange parameter
source("/home/voellenk/osorno_workdir/osorno/lib/select_exchange.R")

# for debug
#osornodb <- osornodb_xtsx
#opt <- list()
#opt$verbose <- TRUE

# connect to database (keys are in secret file)
con <- connectToOsornoDb(osornodb)

# create context table if not exists
if (opt$verbose) {
  cat("creating context table if not exists.\n")
}
createContextTable(con, context)

if (opt$truncatecontext == TRUE) {
  cat("truncating context table.\n")
  truncateContextTable(con)
}

dates <- getDistinctDates(con)
dt1 <- as.Date(dates[,1])
dt <- dt1[dt1 > first.day]
holidays <- getLowVolumeDays(con)

for (i in 1:length(dt)) {
#for (i in 1:3) {
  this.tk <- getTickersOnDate(con, dt[i])
  cat(paste0(dt[i], ": ", length(this.tk), " tickers.\n"))
  dminus52w <- dt[i] - 365
  dminus30d <- dt[i] - 30
  for(j in 1:length(this.tk)) {
    tk <- this.tk[j]
    this <- sapply(context, names) # list of all elements in context list
    this$ticker <- tk
    this$date <- format(dt[i], "%Y-%m-%d")
    if ((j-1) %% floor(length(this.tk)/10) == 0) { # print every 400th item to reach 10 printouts 
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
        this$firstdata <- "1900-01-01"
        this$lastdata <- "1900-01-01"
      } else {
        nr <- ts$ROC[!is.na(ts$ROC) & ts$ROC!=0]
        this$lpnr <- sum(nr>0)     # number of positive days
        this$lnnr <- sum(nr<0)     # number of negative days
        this$entries52w <- nrow(ts.xts[paste0(dminus52w, "/")])
        this$entries30d <- nrow(ts.xts[paste0(dminus30d, "/")])
        tsnozero <- ts[ts$zero == FALSE, ]
        this$firstdata <- tsnozero[1, "date"]
        this$lastdata <- tsnozero[nrow(tsnozero), "date"]
        this$entriesdata <- nrow(ts[ts$date >= this$firstdata & ts$date <= this$lastdata, ])
      }
    } else {
      this$usable <- 0
      this$first <- "1900-01-01"
      this$firstdata <- "1900-01-01"
      this$last <- "1900-01-01"
      this$lastdata <- "1900-01-01"
    }
    replaceContextLine(con, this)
  }
  cat("\n\n")
}

# diconnect from DB
cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
dbres <- dbDisconnect(con)

echoStopMark()
