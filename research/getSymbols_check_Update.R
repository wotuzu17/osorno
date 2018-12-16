#!/usr/bin/Rscript --vanilla
# Script downloads selected syms from quandl to check nightly update time
# credentials in file ~/.osornodb.R
Sys.setenv(TZ="UTC")
scriptname <- "getSymbols_check_Update.R" # for logging

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
XTSXtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSX_tickers.csv.gz"
downloadbasedir <- "/home/voellenk/osorno_workdir/download_updates"

suppressPackageStartupMessages(library(Quandl))
suppressPackageStartupMessages(library(xts))

# ------------- some functions -------------------------------
echoStopMark <- function() {
  stop.time <- Sys.time()
  cat (paste(stop.time, scriptname, "stopped, duration:", round(as.numeric(difftime(stop.time, start.time, units="mins")),1), "mins\n"))
  cat ("----------------------------------------------------------------------\n")
}

# tries to download one symbol, returns success and data
robustDownload <- function(sym, from=NULL, to=NULL) {
  success <- TRUE   # download successful?
  rows <- 0
  dd <- data.frame()
  desc <-""
  cat(paste0(sym,"\t"))
  result <- tryCatch({
      dd <- Quandl(paste("XTSX", sym, sep="/"), type="xts", order="asc", start_date=from, end_date=to)
    }, warning = function(w) {
      desc <- paste0("WARNING @ download of sym ", sym,": ", w$message, "\n")
      success <- FALSE
    }, error = function(e) {
      desc <- paste0("ERROR @ download of sym ", sym,": ", e$message, "\n")
      success <- FALSE
    }, finally = {
      if (nrow(dd) > 0) {
        success <- TRUE
        rows <- nrow(dd)
      }
    })
  return(list("sym"= sym,
              "success" = success,
              "rows" = rows,
              "desc" = desc,
              "data" = dd))
}

robustGetMultipleSymbols <- function(syms, downloaddir, from, to, retry=3, verbose=TRUE) {
  ts <- data.frame()
  for(i in 1:length(syms)) {
    rep <- retry
    while(rep > 0) {
      li <- robustDownload(syms[i], from, to)
      if (verbose == TRUE) {
        cat (paste0(li$desc))
      }
      if(li$success == TRUE) {
        ts <- li$data
        rep <- 0
      } else {
        rep <- rep - 1
        if (verbose == TRUE) {
          cat (paste0("Failed to retreive data for sym ", syms[i], ". Trying ", rep, " more times.\n"))     
        }
      }
    }
    # save downloaded ts with metadata in li in RData file
    if (li$success == TRUE) {
      save(ts, file=paste(downloaddir, sprintf("%d_%s.Rdata", i, syms[i]), sep="/"))
      cat(paste0("last : ", index(li$data)[nrow(li$data)], "\n"))
    } else {
      cat (paste0("Downloading sym ", syms[i], " was not successfull.\n"))
    }
  }
}
# ------------------------------------------------------------

# set Quandl api key
Quandl.api_key(quandlAPIkey)

# create download base directory if not exists
if (!dir.exists(downloadbasedir)) {
  dir.create(downloadbasedir)
}

# create download dir for this download
this.downloaddir <- paste(downloadbasedir, format(start.time, "%Y%m%d_%H%M%S"), sep="/")
dir.create(this.downloaddir)

syms <- c("OM", "RRS", "N", "ROE", "AAP")
from <- as.character(as.Date(start.time) - 7) # seven days in past
to <- as.character(as.Date(start.time))       # today


robustGetMultipleSymbols(syms, downloaddir=this.downloaddir, from, to, retry=3)
 

echoStopMark()
