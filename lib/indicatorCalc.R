# Diverse Funktionen zur Berechnung von TS Parameter

# for example: n1=2, n2=6: second-last day to 6th last day
averageVsum <- function(TS, n1, n2) {
  nr <- nrow(TS)
  TTS <- TS[c((nr-n2+1):(nr-n1+1)), "vsum"]
  return(sum(TTS$vsum)/nrow(TTS))
}

# returns c(close_to_SMA, D_SMA, last SMA)
C2SMA <- function(TS, n) {
  tTS <- tail(TS[,"close"], n+1)
  SM <- SMA(tTS[,1], n)
  tSM <- tail(SM, 2)
  C2SM <- as.vector(log(last(TS$close)) - log(last(SM[,1])))
  dSM <- as.vector(log(tSM[2,1])) - as.vector(log(tSM[1,1]))
  return(c(C2SM, dSM, as.vector(tSM[2,1])))
}

# ATR normalized to last close
normATR <- function(TS, n) {
  tTS <- tail(TS[,c("high", "low", "close")], n+1)
  AT <- ATR(tTS, n, "SMA")
  SM <- SMA(tTS[,"close"], n)
  return(as.vector(last(AT$atr))/as.vector(last(SM$SMA)))
}

# returns +1 when on upper end of donchian channel, -1 when on lower end
ClinDonchianChannel <- function(TS, n) {
  tTS <- tail(TS[,"close"], n+1)
  htTS <- head(tTS, n)
  high <- max(htTS)
  low <- min(htTS)
  if(high == low) {
    return(0)
  }
  k <- 2/(high - low)
  d <- 1-k*high
  return(k*as.vector(last(tTS[,1]))+d)
}

