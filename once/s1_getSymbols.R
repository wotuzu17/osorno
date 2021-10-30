#!/usr/bin/Rscript --vanilla
# Script downloads data from Quandl and stores it on download directory
# this script is for downloading the full dataset
# credentials in file ~/.osornodb.R
Sys.setenv(TZ="UTC")
scriptname <- "s1_getSymbols.R" # for logging

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/osorno_lib.R")
source("/home/voellenk/osorno_workdir/osorno/lib/raw_data_clean.R")
downloadbasedir <- "/home/voellenk/osorno_workdir/download"

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(Quandl))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(TTR))
#suppressPackageStartupMessages(library(DBI))
#suppressPackageStartupMessages(library(RMySQL))

option_list <- list(
  make_option(c("--numberofsyms"), type="integer", default=0, 
              help="For testing. Only download numberofsyms symbols [default %default]",
              metavar="number"),
  make_option(c("--exchange"), action="store", default="XTSX",
              help="download XTSX (ventures) or XTSE or XLON data [default %default]"),
  make_option(c("--specificsym"), action="store", default="",
              help="download only specific symbol [default %default]"),
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,
              help="Print extra output [default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

# determine osornodb and tickers file from exchange parameter
source("/home/voellenk/osorno_workdir/osorno/lib/select_exchange.R")

# tries to download one symbol, returns success and data
robustDownload <- function(sym, from=NULL, to=NULL) {
  success <- TRUE   # download successful?
  suitable <- FALSE # data suitable for further processing?
  rows <- 0
  dd <- data.frame()
  desc <-""
  cat(paste0(sym,"\n"))
  result <- tryCatch({
    if (is.null(from) & is.null(to)) {
      dd <- Quandl(paste(opt$exchange, sym, sep="/"), type="xts", order="asc")
    } else {
      dd <- Quandl(paste(opt$exchange, sym, sep="/"), type="xts", order="asc", start_date=from, end_date=to)
    }
    }, warning = function(w) {
      desc <- paste0("WARNING @ download of sym ", sym,": ", w$message, "\n")
      success <- FALSE
    }, error = function(e) {
      desc <- paste0("ERROR @ download of sym ", sym,": ", e$message, "\n")
      success <- FALSE
    }, finally = {
      if(nrow(dd) > 1) {
        dd <- eliminateZeroRows(dd)
        dd <- eliminateNegRows(dd)
        if(nrow(dd) > 1) { # some time series contain only zeros
          dd <- smoothPrice(dd)
          suitl <- checkTSSanity(dd)
          if (suitl$suitable == TRUE) {
            desc <- paste0("downloaded sym ", sym, ": ", 
                           nrow(dd), " rows from ",
                           min(index(dd)), " to ",
                           max(index(dd)))
            rows <- nrow(dd)
            suitable <- TRUE
          } else {
            desc <- paste0("Sym ", sym, ": ", suitl$desc)
          }
        } else {
          desc <- paste0("NO DATA AFTER FILTER @ download of sym ", sym)
        }
      } else {
        desc <- paste0("NO DATA @ download of sym ", sym)
      }
    })
  return(list("sym"= sym,
              "success" = success,
              "suitable" = suitable,
              "rows" = rows,
              "desc" = desc,
              "data" = dd))
}

robustGetMultipleSymbols <- function(syms, downloaddir, retry=3, verbose=FALSE) {
  ts <- data.frame()
  for(i in 1:length(syms)) {
    rep <- retry
    while(rep > 0) {
      li <- robustDownload(syms[i])
      if (verbose == TRUE) {
        cat (paste0(li$desc, "\n"))
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
    if (li$success == TRUE && li$suitable == TRUE) {
      save(ts, file=paste(downloaddir, sprintf("%d_%s.Rdata", i, syms[i]), sep="/"))
    } else {
      cat (paste0("Not saving sym ", syms[i], ".\n"))
    }
  }
}
# ------------------------------------------------------------

if (opt$verbose == TRUE) {
  str(opt)
}

# set Quandl api key
Quandl.api_key(quandlAPIkey)

# examine tickersfile
result <- tryCatch({ # on success, result contains the xtsxsyms data.frame
  xtsxsyms <- read.csv(tickersfile, stringsAsFactors = FALSE)
}, warning = function(w) {
  cat(paste0("WARNING: ", w$message, "\n"))
}, error = function(e) {
  cat(paste0("ERROR: ", e$message, "\n"))
}, finally = {
  if(!exists("xtsxsyms") || !is.data.frame(xtsxsyms)) {
    cat("cannot parse XTSX symbols file\n")
    xtsxsyms <- FALSE
  } else {
    cat(paste("XTSX symbols file from", 
               file.info(tickersfile)[,"mtime"], 
               "contains", nrow(xtsxsyms), "Tickers\n"))
  }
})

# create download base directory if not exists
if (!dir.exists(downloadbasedir)) {
  dir.create(downloadbasedir)
}

# create download dir for this download
this.downloaddir <- paste(downloadbasedir, paste0(opt$exchange, "_", format(start.time, "%Y%m%d_%H%M%S")), sep="/")
dir.create(this.downloaddir)

if (opt$numberofsyms > 0) {
  # download full set of numberofsyms random tickers
  syms <- sample(xtsxsyms[,1], floor(opt$numberofsyms))
  robustGetMultipleSymbols(syms, downloaddir=this.downloaddir, retry=3, verbose=opt$verbose)
} else if (opt$specificsym != "") {
  robustGetMultipleSymbols(opt$specificsym, downloaddir=this.downloaddir, retry=3, verbose=opt$verbose)
} else {
  # donwload entire list of symbols (~8622) in xtsxsyms
  # create dir for this download
  robustGetMultipleSymbols(xtsxsyms[,1], downloaddir=this.downloaddir, retry=3, verbose=opt$verbose)
}

echoStopMark()
