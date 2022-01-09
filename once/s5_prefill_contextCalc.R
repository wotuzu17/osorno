#!/usr/bin/Rscript --vanilla
# prefill contextCalc and contextCalc2 table for swarmCalcContext

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
  make_option(c("-V", "--version"), action="store", default="",
              help="select 2 for contextCalc2 table [default %default]"),
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,
              help="Print extra output [default]")
)

opt <- parse_args(OptionParser(option_list=option_list))

if(opt$version == "") {
  version <- ""
} else if (opt$version == "2") {
  version <- "2"
} else {
  stop("No valid version given.\n")
}

# determine osornodb and tickers file from exchange parameter
source("/home/voellenk/osorno_workdir/osorno/lib/select_exchange.R")

# connect to database (keys are in secret file)
con <- connectToOsornoDb(osornodb)

# create context table if not exists
if (opt$verbose) {
  cat(sprintf("creating contextCalc%s table if not exists.\n", opt$version))
}
createContextCalcTable(con, version)

if (opt$truncatecontext == TRUE) {
  cat("truncating context table.\n")
  truncateContextTable(con, version)
}

dates <- getDistinctDates(con)
dt1 <- as.Date(dates[,1])
dt <- dt1[dt1 >= first.day]

cat(sprintf("Inserting empty rows of all days in contextCalc%s table:\n", version))
cat(paste0("beginning with ", first(dt), "\n"))
for(i in 1:length(dt)) {
  sql <- insertEmptyDateLine(con, dt[i], version)
  dbSendQuery(con, sql)
}
cat(paste0("ending with ", last(dt), "\n"))

# diconnect from DB
if (opt$verbose) {
  cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
}
dbres <- dbDisconnect(con)

echoStopMark()
