# chunk is used in s6_swarmCalcContext2.R

suitabletk <- getAcceptableBacktestSyms(coni, this.dt) # based on entries in context table
nsyms <- nrow(suitabletk)
cat(paste0(this.dt, ": ", nsyms, " tickers.\n"))
dminus52w <- this.dt - 365
dminus30d <- this.dt - 30

for(j in 1:nrow(suitabletk)) {
  tk <- suitabletk[j, "ticker"]
  this <- sapply(context2, names) # list of all elements in context2 list
  this$ticker <- tk
  this$date <- format(this.dt, "%Y-%m-%d")
  if ((j-1) %% floor(nrow(suitabletk)/10) == 0) { # print every 400th item to reach 10 printouts 
    cat(paste0(j, ":", tk, " "))
  }
  TSu <- getTicker1(coni, tk, adjust=FALSE, holidays=holidays$date, from=NULL, to=this.dt)
  TSa <- getTicker1(coni, tk, adjust=TRUE, holidays=holidays$date, from=NULL, to=this.dt)
  # log price (thats what we need the unadjusted quote series for)
  this$logprice <- log(as.vector(last(TSu$close)) + .00001)
  # log volumes
  TSa$vsum <- TSa$close * TSa$volume
  this$logvol_1 <- log(as.vector(TSa$vsum[nrow(TSa)]) +1)
  this$logvol_2_7 <- log(averageVsum(TSa, 2, 7) +1)
  this$logvol_8_63 <- log(averageVsum(TSa, 8, 63) +1)
  this$logvol_1_144 <- log(averageVsum(TSa, 1, 144) +1)
  # normalized ATR
  this$normATR7 <- normATR(TSa, 7)
  this$normATR63 <- normATR(TSa, 63)
  # donchian Channel
  this$donCh21 <- ClinDonchianChannel(TSa, 21)
  this$donCh144 <- ClinDonchianChannel(TSa, 144)
  # SMA
  SMA3 <- C2SMA(TSa, 3)
  this$C2SMA3 <- SMA3[1]
  this$SMA3d <- SMA3[2]
  SMA21 <- C2SMA(TSa, 21)
  this$C2SMA21 <- SMA21[1]
  this$SMA21d <- SMA21[2]
  SMA144 <- C2SMA(TSa, 144)
  this$C2SMA144 <- SMA144[1]
  this$SMA144d <- SMA144[2]
  this$SMA3_21 <- log(SMA3[3]) - log(SMA21[3])
  this$SMA21_144 <- log(SMA21[3]) - log(SMA144[3]) 
  # write line to db
  replaceContext2Line(cono, this)
}
cat("\n")