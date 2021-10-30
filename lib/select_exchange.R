# used by the once scripts

XLONtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XLON_tickers.csv.gz"
XTSXtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSX_tickers.csv.gz"
XTSEtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSE_tickers.csv.gz"
OTCBtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/OTCB_tickers.csv.gz"

if(opt$exchange == "XTSX") {
  cat("processing data from Toronto Ventures exchange (XTSX).\n")
  osornodb <- osornodb_xtsx
  tickersfile <- XTSXtickersfile 
} else if (opt$exchange == "XTSE") {
  cat("processing data from Toronto stock exchange (XTSE).\n")
  osornodb <- osornodb_xtse
  tickersfile <- XTSEtickersfile 
} else if (opt$exchange == "XLON") {
  cat("processing data from London stock exchange (XLON).\n")
  osornodb <- osornodb_xlon
  tickersfile <- XLONtickersfile 
} else if (opt$exchange == "OTCB") {
  cat("processing data from OTCB stock exchange (OTCB).\n")
  osornodb <- osornodb_otcb
  tickersfile <- OTCBtickersfile 
} else {
  stop("exchange is not defined. Choose either --exchange=XTSX, XTSE, XLON or OTCB.\n")
}