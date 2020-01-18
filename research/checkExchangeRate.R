# compare TSX and London prices

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")
XLONtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XLON_tickers.csv.gz"
XTSXtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSX_tickers.csv.gz"
XTSEtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSE_tickers.csv.gz"

library(stringdist)
suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(Quandl))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(TTR))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))

# ---------------some functions-------------------------------------

# connect to database (keys are in secret file)
conlib <- connectToOsornoDb(osornodb_lib)
conv <- connectToOsornoDb(osornodb_xtsx)
conl <- connectToOsornoDb(osornodb_xlon)

# get all pairs xtsx -> xlon
symlib <- getSymlib(conlib)
vlsyms <- symlib[!is.na(symlib$XTSX) & !is.na(symlib$XLON),]

for (i in 1:nrow(vlsyms)) {
  tsv <- getTickerDF(conv, vlsyms[i,"XTSX"])
  tsl <- getTickerDF(conl, vlsyms[i,"XLON"])
  tscom <- merge(tsv, tsl, by="date")
  tscom$ex <- tscom$close.y  / tscom$close.x / 100
  CADGBP <- getFX("CAD/GBP", auto.assign = FALSE)
  exdf <- data.frame("date" = as.character(index(CADGBP)), "CADGBP" = CADGBP[,1], row.names = NULL, stringsAsFactors = FALSE)
  tscom <- merge(tscom, exdf, by="date")
  
}

