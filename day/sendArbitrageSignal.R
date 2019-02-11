#!/usr/bin/Rscript --vanilla
# script creates rmarkdown report of market situation
foo <- Sys.setlocale("LC_ALL", "en_US.UTF-8")
Sys.setenv(TZ="UTC")
scriptname <- "sendArbitrageSignal.R" # for logging
osornobasedir <- "/home/voellenk/osorno_workdir"

start.time <- Sys.time()
cat (paste(start.time, scriptname, "started-------------------\n"))

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")
source("/home/voellenk/osorno_workdir/osorno/lib/chart_functions.R")
recipfile <- "/home/voellenk/osorno_workdir/phonebook/recipients.txt"

suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(TTR))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(quantmod))

option_list <- list(
  make_option(c("-a", "--all"), action="store_true", default=FALSE,
              help="create report for all recipients [default %default]"),
  make_option(c("--recipientnum"), type="integer", default=1, 
              help="Send signal to only specified recipient [default %default]",
              metavar="number")
)

opt <- parse_args(OptionParser(option_list=option_list))

# connect to database (keys are in secret file)
cat("Connect to Databases... ")
conlib <- connectToOsornoDb(osornodb_lib)
conv <- connectToOsornoDb(osornodb_xtsx)
cont <- connectToOsornoDb(osornodb_xtse)
conl <- connectToOsornoDb(osornodb_xlon)
cat("done!\n")

# get all pairs xtsx -> xlon
symlib <- getSymlib(conlib)
symlib <- symlib[with(symlib, order(name)),] # order alphabetically by name

# get exchange rate
CADGBP <- getFxExchangePair(conlib, "CADGBP")




cat("Disconnect from databases... ")
disconnectFromOsornoDb(conv, osornodb_xtsx)
disconnectFromOsornoDb(cont, osornodb_xtse)
disconnectFromOsornoDb(conl, osornodb_xlon)
disconnectFromOsornoDb(conlib, osornodb_lib)
cat("done!\n")

echoStopMark()