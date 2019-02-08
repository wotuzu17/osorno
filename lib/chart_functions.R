# some functions to create time series charts
tsdfplot <- function(df, name, sym, exchange, start=NULL, end=NULL) {
  df <- df[,c("date", "high", "low", "close", "volume")]
  df$date <- as.Date(df$date)
  df$logvol <- log(df[,"volume"]+10)
  p <- ggplot(df, aes(date, close)) + 
    geom_line(colour="lightgray") +
    geom_linerange(aes(ymin=low, ymax=high), colour="darkgray") +
    geom_point(aes(colour=logvol), show.legend=TRUE) + 
    scale_y_log10(breaks=pretty_breaks()) +
    scale_color_distiller(palette="Spectral") +
    theme(axis.title.x = element_blank(), axis.title.y = element_blank()) + 
    ggtitle(paste0(name, " (",sym, ") @ ", exchange)) 
  if(!is.null(start)) {
    p <- p + scale_x_date(date_breaks= "2 month", labels=date_format("%b-%y"), limits=c(start, end))
  }
  return(p)
}

# human readable big numbers
formatNumber <- function(n) {
  if (!is.numeric(n)) {
    return(n)
  }
  if(n<1000) {
    return(as.character(round(n)))
  } else if (n<1000000) {
    return(paste0(round(n/1000, 1), "k"))
  } else {
    return(paste0(round(n/1000000, 1), "m"))
  }
}

# last month daily average volume
avdailyvolume <- function(df, pastdate) {
  retvar <- list()
  vols <- df[df$date > pastdate, "volume"]
  avvol <- sum(vols)/length(vols)
  retvar[["avvol"]] <- avvol
  retvar[["avvol_short"]] <- formatNumber(avvol)
  return(retvar)
}

# explains where the turnover was higher
explainvolume <- function(volli) {
  if (volli[[1]]$avvol == 0) {
    stri <- ("During the last month there was no trading in London.\n")
  } else if (volli[[2]]$avvol == 0) {
    stri <- ("During the last month there was no trading in Toronto.\n")
  } else if (volli[[1]]$avvol >= volli[[2]]$avvol) {
    stri <- paste0("Last month turnover was ", round(volli[[1]]$avvol / volli[[2]]$avvol, 1),
                   " times higher in London than in Toronto.\n")
  } else {
    stri <- paste0("Last month turnover was ", round(volli[[2]]$avvol / volli[[1]]$avvol, 1),
                   " times higher in Toronto than in London.\n")
  }
  return(stri)
}

# makes dataframe of 2 time series with exchange rate
exchangeRateDf <- function(tsl, tsc, rate) {
  preparedf <- function(ts, exchange) {
    subts <- ts[,c("date", "open", "high", "low", "close", "volume")]
    colnames(subts) <- c("date", paste0(exchange, ".", colnames(subts)[2:ncol(subts)]))
    return(subts)
  }
  tslp <- preparedf(tsl, "L")
  tscp <- preparedf(tsc, "T")
  tsm <- merge(tslp, tscp, by="date")
  tsm$ratio.H <- tsm$L.high / tsm$T.low / 100
  tsm$ratio.L <- tsm$L.low / tsm$T.high / 100
  tsm$ratio.C <- tsm$L.close / tsm$T.close / 100
  tsm <- merge(tsm, rate)
  tsm$date <- as.Date(tsm$date)
  tsm$volnum <- (tsm$L.volume > 0) * 2 + (tsm$T.volume > 0)
  tsm$volcode <- ""
  codetable <- c("no_vol" = 0, "Tor" = 1, "Lon" = 2, "both" = 3)
  for (i in 0:length(codetable)-1) {
    tsm[tsm$volnum == i, "volcode"] <- names(codetable)[i+1]
  }
  tsm$volcode <- factor(tsm$volcode, levels=names(codetable))
  return(tsm)
}

exchangeRatePlot <- function(exdf, name) {
  centerdate <- first(exdf)$date + (last(exdf)$date - first(exdf)$date)/2
  p <- ggplot(exdf, aes(date, ratio.C)) + 
    geom_point(aes(colour=volcode)) +
    scale_colour_brewer(palette="Set1") +
    geom_linerange(aes(ymin=ratio.L, ymax=ratio.H), colour="darkgray") +
    annotate("text", x=centerdate, y=Inf, label="overvalued @ London", vjust=1, hjust=.5, colour="blue") + 
    annotate("text", x=centerdate, y=-Inf, label="overvalued @ Toronto", vjust=-1, hjust=.5, colour="red") +
    theme(axis.title.x = element_blank(), axis.title.y = element_blank()) + 
    geom_line(aes(date, quote), colour="brown") + 
    ggtitle(paste0(name, ", CADGBP"))
  return(p)
}