#!/usr/bin/Rscript --vanilla
# prefill contextCalc table for swarmCalcContext

Sys.setenv(TZ="UTC")
scriptname <- "s5_prefill_contextCalc.R" # for logging

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/osorno_lib.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_01_connect_disconnect.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_02_quotes_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_08_contextCalc_table.R")
first.day <- as.Date("2005-01-01") # start analysis from here

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(TTR))

option_list <- list(
  make_option(c("--truncatecontextCalc"), action="store_true", default=FALSE,
              help="empty contextCalc table before new calculation [default %default]"),
  make_option(c("-x", "--exchange"), action="store", default="",
              help="download XTSX (ventures) or XTSE or XLON data [default %default]"),
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,
              help="Print extra output [default]")
)

opt <- parse_args(OptionParser(option_list=option_list))
# determine osornodb and tickers file from exchange parameter
source("/home/voellenk/osorno_workdir/osorno/lib/select_exchange.R")

# connect to database (keys are in secret file)
con <- connectToOsornoDb(osornodb)

# create context table if not exists
if (opt$verbose) {
  cat("creating contextCalc table if not exists.\n")
}
createContextCalcTable(con)

if (opt$truncatecontext == TRUE) {
  cat("truncating context table.\n")
  truncateContextTable(con)
}

dates <- getDistinctDates(con)
dt1 <- as.Date(dates[,1])
dt <- dt1[dt1 >= first.day]

cat("Inserting empty rows of all days in quote table:\n")
cat(paste0("beginning with ", first(dt), "\n"))
for(i in 1:length(dt)) {
  sql <- insertEmptyDateLine(con, dt[i])
  dbSendQuery(con, sql)
}
cat(paste0("ending with ", last(dt), "\n"))

# diconnect from DB
if (opt$verbose) {
  cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
}
dbres <- dbDisconnect(con)

echoStopMark()
