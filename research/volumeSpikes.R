# investigate volume spike events

exchange <- "XTSX"

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")
XLONtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XLON_tickers.csv.gz"
XTSXtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSX_tickers.csv.gz"
XTSEtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSE_tickers.csv.gz"

suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(TTR))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(quantmod))

if (exchange == "XTSX") {
  con <- connectToOsornoDb(osornodb_xtsx)
} else if (exchange == "XTSE") {
  con <- connectToOsornoDb(osornodb_xtse)
} else if (exchange == "XLON") {
  con <- connectToOsornoDb(osornodb_xlon)
}

# get stockquality table to determine symbols to retrieve
# read stock quality table (must be current)
sqt <- getStockQualityTable(con) # sqt: 6765 tickers
sqt <- sqt[sqt$tooshort == 0, ]
sqt <- sqt[as.Date(sqt$lastdata) > as.Date("2015-01-01"), ]
sqt <- sqt[sqt$entriesdata/(sqt$entriesdata+sqt$zerodata )>.55, ]

holidays <- getLowVolumeDays(con, 5)[,1]

# get time series of all suitable tickers
tsli <- list()
for (i in 1:nrow(sqt)) {
  this.sym <- sqt[i,"ticker"]
  ts <- getTicker(con, this.sym, holidays)
  ts$ROC <- ROC(ts$close)
  ts$vol30d <- round(lag(runSum(ts$volume, n=30), k=1) / 30)
  ts$logvol <- log(ts$vol+1)
  ts$logvol30d <- log(ts$vol30d+1)
  ts$logclose <- log(ts$close)
  ts$dvol <- ts$close * ts$volume  # dollar volume
  ts$logdvol <- log(ts$dvol+1)
  tsli[[this.sym]] <- ts
}

# find high volume events in all suitable symbols
statdf <- data.frame()
for (i in 1:length(tsli)) {
  ts <- tsli[[i]]
  cts <- ts[complete.cases(ts), ]
  nrow <- nrow(cts)
  match <- nrow(cts[cts$logvol > cts$logvol30d+2,])
  statdf <- rbind(statdf, data.frame("ticker"=names(tsli)[i], "nrow" = nrow, "match" = match))
}

statdf$ratio <- statdf$match / (statdf$nrow)
