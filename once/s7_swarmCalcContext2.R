#!/usr/bin/Rscript --vanilla
# script grabs one date from the db and calculates metric

Sys.setenv(TZ="UTC")
scriptname <- "s7_swarmCalcContext2.R" # for logging
hname <- system2("hostname", stdout=TRUE)

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/osorno_lib.R")
source("/home/voellenk/osorno_workdir/osorno/lib/acceptable_syms.R")
source("/home/voellenk/osorno_workdir/osorno/lib/indicatorCalc.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_01_connect_disconnect.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_02_quotes_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_03_volumesum_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_06_context_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_08_contextCalc_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_09_context2_table.R")

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(TTR))

option_list <- list(
  make_option(c("-x", "--exchange"), action="store", default="",
              help="process XTSX (ventures) or XTSE or XLON data [default %default]"),
  make_option(c("-i", "--inDB"), action="store", default="localhost",
              help="database to read quote data from [default %default]"),
  make_option(c("-o", "--outDB"), action="store", default="localhost",
              help="database to write context data to [default %default]"),
  make_option(c("--TTL"), type="integer", default=3, 
              help="time to live. How many day jobs until process dies [default %default]",
              metavar="number"),
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,
              help="Print extra output [default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

# determine osornodb and tickers file from exchange parameter
source("/home/voellenk/osorno_workdir/osorno/lib/select_exchange.R")

# define which databases to use
osornodb.in <- osornodb
osornodb.in$host <- opt$inDB
osornodb.out <- osornodb
osornodb.out$host <- opt$outDB

# connect to in and out database (keys are in secret file)
cat (paste(Sys.time(), "Connect to inDB", osornodb.in$db, "on host", osornodb.in$host, "\n"))
coni <- connectToOsornoDb(osornodb.in)
cat (paste(Sys.time(), "Connect to outDB", osornodb.out$db, "on host", osornodb.out$host, "\n"))
cono <- connectToOsornoDb(osornodb.out)

# create context2 table if not exists
if (opt$verbose) {
  cat("creating context2 table if not exists.\n")
}
createContext2Table(cono, context2)

# get complete dates from quotes table of db.in
dates <- getDistinctDates(coni)
dt <- as.Date(dates[,1])
holidays <- getLowVolumeDays(coni)

TTL <- opt$TTL
while(TTL > 0) {
  # retrieve date to process
  this.line <- getOldestOpenDate(cono, "2")
  if(nrow(this.line) == 1) {
    this.date <- this.line[1, "date"] # character
    this.dt <- as.Date(this.date)     # Date
    cat(paste0("...identified ", this.date , " as oldest unprocessed date.\n"))
    # mark date as being processed now
    setProcessStartMark(cono, this.date, hname, "2")
    # verify if startmark is set
    vf <- verifyProcessMark(cono, this.date, hname, "2")
    if(nrow(vf) == 1) {
      starttime <- as.POSIXct(vf[1, "start"])
      cat(paste0("...start processing ", this.date, " at ", vf[1, "start"], "\n"))
      
      # now process that date
      # code is outsourced to another file for better reability
      source("/home/voellenk/osorno_workdir/osorno/lib/calc_Context2.R")
      
      # set finishmark
      setProcessStopMark(cono, this.date, nsyms, "2")
      # verify if finishmark was set
      vf <- verifyProcessMark(cono, this.date, hname, "2")
      if (nrow(vf) == 1) {
        endtime <- as.POSIXct(vf[1, "end"])
        cat(paste0("...completed processing ", this.date, " at ", vf[1, "end"], 
                   ". It took ", round(difftime(endtime, starttime, units="secs")), " seconds.\n"))
      } else {
        cat("...ERROR: could not verify process end mark.\n")
      }
    } else {
      cat("...ERROR: could not set process start mark.\n")
      break
    }
    # process date
  } else {
    cat("...FINISHED: no unprocessed date left.\n")
    break
  }
  TTL <- TTL - 1
}

# diconnect from DB
cat (paste(Sys.time(), "Disconnect from inDB", osornodb.in$db, "on host", osornodb.in$host, "\n"))
dbres <- dbDisconnect(coni)
cat (paste(Sys.time(), "Disconnect from outDB", osornodb.out$db, "on host", osornodb.out$host, "\n"))
dbres <- dbDisconnect(cono)

echoStopMark()
