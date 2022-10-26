#!/usr/bin/Rscript --vanilla
# script to create and fill fxexchange data
foo <- Sys.setlocale("LC_ALL", "en_US.UTF-8")
Sys.setenv(TZ="UTC")
scriptname <- "makeReport.R" # for logging
osornobasedir <- "/home/voellenk/osorno_workdir"

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_01_connect_disconnect.R")
#source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")

majors <- c("USD", "EUR", "GBP", "CAD", "CHF")
currencies <- c("AUD", "ARS", "BRL", "CLP", "DKK", "CNY", "RUB",
                "HKD", "MXN", "NOK", "SEK", "JPY", "NZD", "TRY")

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(TTR))
suppressPackageStartupMessages(library(quantmod))

option_list <- list(
  make_option(c("--truncatefxexchange"), action="store_true", default=FALSE,
              help="empty fxexchange table before data import [default %default]"),
  make_option(c("--day"), action="store", default="most_recent",
              help="make analyze for given YYYY-MM-DD [default %default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

# ------------- some functions -------------------------------------------------------
echoStopMark <- function() {
  stop.time <- Sys.time()
  cat (paste(stop.time, scriptname, "stopped, duration:", round(as.numeric(difftime(stop.time, start.time, units="mins")),1), "mins\n"))
  cat ("----------------------------------------------------------------------\n")
}

xtsToDataFrame <- function(ts) {
  df <- data.frame("date" = as.character(index(ts)), "pair" = ts[,1], row.names = NULL, stringsAsFactors = FALSE)
  colnames(df) <- sub("\\.", "", colnames(df))
  return(df)
}
# ------------------------------------------------------------------------------------

# connect to database va_osorno_lib
cat ("Connect to database ...\n")
conlib <- connectToOsornoDb(osornodb_lib)

# create fxexchange table if not exists
dbres <- createFxExchangeTable(conlib)

if (opt$truncatefxexchange == TRUE) {
  cat ("emptying fxexchange table now.")
  truncateFxExchangeTable(conlib)
  cat ("... done!\n")
}

# download quotes and feed to db
for (i in 1:length(majors)) {
  rest <- c(majors[majors!=majors[i]], currencies)
  for (j in 1:length(rest)) {
    pair <- paste(majors[i], rest[j], sep="/")
    cat(paste0("pair ", pair, ": "))
    ts <- getFX(pair, auto.assign=FALSE)
    if (nrow(ts) > 0) {
      cat (paste0("got ", nrow(ts), " days."))
      tsdf <- xtsToDataFrame(ts)
      # see what is already in db
      indb <- getFxExchangePair(conlib, sub("/", "", pair))
      chunk <- tsdf[!(tsdf$date %in% indb$date), ]
      if (nrow(chunk) > 0) {
        cat (paste0(" Insert ", nrow(chunk), " new rows to DB... "))
        insertFxExchangeChunk(conlib, chunk)
        cat ("done!\n")
      } else {
        cat ("no new data. Inserting nothing.\n")
      }
    } else {
      cat(paste0("ERROR: Could not retrieve ", pair, "quotes.\n"))
    }
  }
}

cat ("Disconnect from database ...\n")
disconnectFromOsornoDb(conlib, osornodb_lib)
echoStopMark()