#!/usr/bin/Rscript --vanilla
# Script downloads data from Quandl and stores it on download directory
# credentials in file ~/.osornodb.R
Sys.setenv(TZ="UTC")
scriptname <- "nightlyRun_getSymbols.R" # for logging

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
XTSXtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSX_tickers.csv.gz"
downloadbasedir <- "/home/voellenk/osorno_workdir/download"

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(Quandl))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(TTR))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))

option_list <- list(
  make_option(c("--incremental"), action="store_true", default=FALSE,
              help="only download missing days [default %default]"),
  make_option(c("--fulldownload"), action="store_true", default=FALSE,
              help="download entire dataset [default %default]"),
  make_option(c("--numberofsyms"), type="integer", default=0, 
              help="For testing. Only download numberofsyms symbols [default %default]",
              metavar="number"),
  make_option(c("-v", "--verbose"), action="store_true", default=FALSE,
              help="Print extra output [default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

# ------------- some functions -------------------------------
echoStopMark <- function() {
  stop.time <- Sys.time()
  cat (paste(stop.time, scriptname, "stopped, duration:", round(as.numeric(difftime(stop.time, start.time, units="mins")),1), "mins\n"))
  cat ("----------------------------------------------------------------------\n")
}

# if any of OHLC prices contain zero, disregard line
eliminateZeroRows <- function(ts) {
  if (nrow(ts) < 1) {
    return(ts)
  } else {
    ts1 <- cbind(ts, ts[,1] * ts[,2] * ts[,3] * ts[,4])
    return(ts1[ts1[,ncol(ts1)] >0, c(1:(ncol(ts1)-1))]) 
  }
}

# if any of OHLCV prices contain values < 0, disregard line
eliminateNegRows <- function(ts) {
  if (nrow(ts) < 1) {
    return(ts)
  } else {
    ts1 <- cbind(ts, ts[,1] < 0 | ts[,2] < 0 | ts[,3] < 0 | ts[,4] < 0 | ts[,5] < 0)
    return(ts1[ts1[,ncol(ts1)] == 0, c(1:(ncol(ts1)-1))])
  }
}

# eliminate crazy price spikes
smoothPrice <- function(ts) {
  smoothCol <- function(col) {
    col1 <- cbind(col, ROC(col[,1]))
    col2 <- cbind(col1, lag(ROC(col[,1])*(-1), k=-1), lag(col1[,1]))
    col2[is.na(col2[,3]), 3] <- 2
    col3 <- cbind(col2, col2[,2] > 1 & col2[,3] > 1)
    col4 <- cbind(col3, col3[,4] * col3[,5])
    return(col4[,6] + col4[,1]*(col4[,5] == 0))
  }
  ts1<-smoothCol(ts[,1])
  ts1<-cbind(ts1, smoothCol(ts[,2]), smoothCol(ts[,3]), smoothCol(ts[,4]))
  colnames(ts1) <- c("Open", "High", "Low", "Close")
  if (ncol(ts) > 4) {
    return(cbind(ts1, ts[,c(5:ncol(ts))])[c(2:nrow(ts1)),])
  } else {
    return(ts1[c(2:nrow(ts1)),])
  }
}

# checkTSsanity. Return false for obviously wrong prices
checkTSSanity <- function(ts, minrows=200){
  if (nrow(ts) < minrows) {
    suitable <- FALSE
    desc <- paste0("Time series contains only ",nrow(ts)," (less than ", minrows,") rows.")
  } else if(max(ts[,c(1:4)] > 10000)) {
    suitable <- FALSE
    desc <- paste0("Time series max price ", max(ts[,c(1:4)]), " is too high.")
  } else if (max(ts[,5]) > 1E15) {
    suitable <- FALSE
    desc <- paste0("Time series max volume ", max(ts[,5]), " is too high.")
  } else {
    suitable <- TRUE
    desc <- ""
  }
  return(list("suitable" = suitable, "desc" = desc))
}

# tries to download one symbol, returns success and data
robustDownload <- function(sym, from=NULL, to=NULL) {
  success <- TRUE   # download successful?
  suitable <- FALSE # data suitable for further processing?
  rows <- 0
  dd <- data.frame()
  desc <-""
  cat(paste0(sym,"\t"))
  result <- tryCatch({
    if (is.null(from) & is.null(to)) {
      dd <- Quandl(paste("XTSX", sym, sep="/"), type="xts", order="asc")
    } else {
      dd <- Quandl(paste("XTSX", sym, sep="/"), type="xts", order="asc", start_date=from, end_date=to)
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

# examine XTSXtickersfile
result <- tryCatch({ # on success, result contains the xtsxsyms data.frame
  xtsxsyms <- read.csv(XTSXtickersfile, stringsAsFactors = FALSE)
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
               file.info(XTSXtickersfile)[,"mtime"], 
               "contains", nrow(xtsxsyms), "Tickers\n"))
  }
})

# create download base directory if not exists
if (!dir.exists(downloadbasedir)) {
  dir.create(downloadbasedir)
}

# create download dir for this download
this.downloaddir <- paste(downloadbasedir, format(start.time, "%Y%m%d_%H%M%S"), sep="/")
dir.create(this.downloaddir)

# determine which symbols to feed
if (opt$incremental == TRUE) {
  # query db for distinct symbols
  # for each symbol, get last day in db
  # get remaining data from quandl
  # feed to db
} else if (opt$fulldownload == TRUE) {
  if (opt$numberofsyms > 0) {
    # download full set of numberofsyms random tickers
    syms <- sample(xtsxsyms[,1], floor(opt$numberofsyms))
    robustGetMultipleSymbols(syms, downloaddir=this.downloaddir, retry=3, verbose=opt$verbose)
  } else {
    # donwload entire list of symbols (~8622) in xtsxsyms
    # create dir for this download
    robustGetMultipleSymbols(xtsxsyms[,1], downloaddir=this.downloaddir, retry=3, verbose=opt$verbose)
  }
} else {
  cat("ERROR: Select either parameter --incremental or --fulldownload\n")
  print_help(OptionParser(option_list=option_list))
}

echoStopMark()
