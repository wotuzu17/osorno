# chunk is used in s6_swarmCalcContext.R

this.tk <- getTickersOnDate(coni, this.dt)
nsyms <- length(this.tk)
cat(paste0(this.dt, ": ", nsyms, " tickers.\n"))
dminus52w <- this.dt - 365
dminus30d <- this.dt - 30
for(j in 1:length(this.tk)) {
  tk <- this.tk[j]
  this <- sapply(context, names) # list of all elements in context list
  this$ticker <- tk
  this$date <- format(this.dt, "%Y-%m-%d")
  if ((j-1) %% floor(length(this.tk)/10) == 0) { # print every 400th item to reach 10 printouts 
    cat(paste0(j, ":", tk, " "))
  }
  usable <- TRUE # assume the symbol is usable, may be overridden later on
  ts <- getTickerDF1(coni, tk, adjust=TRUE, NULL, this.dt)
  if(nrow(ts) > 0) {
    ts.xts <- xts(ts[,c("close", "volume")], order.by=as.Date(ts[,"date"])) 
  } else {
    usable <- FALSE
  }
  tsu <- getTickerDF1(coni, tk, adjust=FALSE, NULL, this.dt)
  if (nrow(tsu) > 0) {
    tsu.xts <- xts(tsu[,c("close", "volume")], order.by=as.Date(tsu[,"date"]))
  } else {
    usable <- FALSE
  }
  if(abs(nrow(ts) - nrow(tsu)) > 2) {
    usable <- FALSE
  }
  if (usable) {
    this$usable <- 1
    this$first <- ts[1, "date"]
    this$last <- ts[nrow(ts), "date"]
    this$entries <- nrow(ts)
    this$adjustments <- nrow(ts[!is.na(ts[,"adj_factor"]), ])
    ts$ROC <- ROC(ts[,"close"])
    ts$zero <- ts$volume == 0 & (ts$ROC == 0 | is.na(ts$ROC))
    this$zerodata <- sum(ts$zero)
    if (sum(ts$zero) == nrow(ts)) { # only rows with no action
      this$usable <- 0
      this$firstdata <- "1900-01-01"
      this$lastdata <- "1900-01-01"
    } else {
      nr <- ts$ROC[!is.na(ts$ROC) & ts$ROC!=0]
      this$lpnr <- sum(nr>0)     # number of positive days
      this$lnnr <- sum(nr<0)     # number of negative days
      this$entries52w <- nrow(ts.xts[paste0(dminus52w, "/")])
      this$entries30d <- nrow(ts.xts[paste0(dminus30d, "/")])
      tsnozero <- ts[ts$zero == FALSE, ]
      this$firstdata <- tsnozero[1, "date"]
      this$lastdata <- tsnozero[nrow(tsnozero), "date"]
      this$entriesdata <- nrow(ts[ts$date >= this$firstdata & ts$date <= this$lastdata, ])
    }
  } else {
    this$usable <- 0
    this$first <- "1900-01-01"
    this$firstdata <- "1900-01-01"
    this$last <- "1900-01-01"
    this$lastdata <- "1900-01-01"
  }
  replaceContextLine(cono, this)
}
cat("\n")