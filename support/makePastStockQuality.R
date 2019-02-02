#!/usr/bin/Rscript --vanilla
# script to create and fill stockquality table 
foo <- Sys.setlocale("LC_ALL", "en_US.UTF-8")
Sys.setenv(TZ="UTC")
scriptname <- "makePastStockQuality.R" # for logging
osornobasedir <- "/home/voellenk/osorno_workdir"
stockqualityscript <- "/home/voellenk/osorno_workdir/osorno/support/makeStockQuality.R"


start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))

option_list <- list(
  make_option(c("--exchange"), action="store", default="",
              help="download XTSX (ventures) or XTSE or XLON [default %default]"),
  make_option(c("--from"), action="store", default="",
              help="make analyze from day YYYY-MM-DD [default %default]"),
  make_option(c("--to"), action="store", default="",
              help="make analyze until day YYYY-MM-DD [default %default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

if(opt$exchange == "XTSX") {
  osornodb <- osornodb_xtsx
  cat("processing data from Toronto Ventures Exchange (XTSX).\n")
} else if (opt$exchange == "XTSE") {
  osornodb <- osornodb_xtse
  cat("processing data from Toronto Stock Exchange (XTSE).\n")
} else if (opt$exchange == "XLON"){
  osornodb <- osornodb_xlon
  cat("processing data from London Stock Exchange (XLON).\n")
} else {
  stop("exchange is not defined. Choose either --exchange=XTSX or XTSE or XLON.\n")
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

from <- as.Date(opt$from)
to <- as.Date(opt$to)
holidays <- as.Date(as.vector(getLowVolumeDays(con)[,1]))

dayvec <- format(seq(from, to, by="days"), "%Y-%m-%d")

for (day in dayvec) {
  if(format(as.Date(day), "%w") %in% c("0", "6")) {
    cat(paste(as.Date(day), "is weekend.\n"))
  } else if(as.Date(day) %in% holidays) {
    cat(paste(as.Date(day), "is holiday.\n"))
  } else {
    args <- sprintf("--day=%s", as.character(as.Date(day)))
    system2(stockqualityscript, args)
  }
}

# diconnect from DB
cat ("Disconnect from DB.\n")
databasedisconnect(con)

# finish
echoStopMark()