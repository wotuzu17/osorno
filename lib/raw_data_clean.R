# functions to get and clean quandl raw data

# get Quandl raw data. Needs to be cleaned 
robustGetQuandlData <- function(sym, from=NULL, to=NULL) {
  maxtry <- 3
  dd <- data.frame()
  while(maxtry > 0) {
    result <- tryCatch({
      desc <- ""
      success <- FALSE
      if (is.null(from) & is.null(to)) {
        dd <- Quandl(paste("XTSX", sym, sep="/"), type="xts", order="asc")
      } else {
        dd <- Quandl(paste("XTSX", sym, sep="/"), type="xts", order="asc", start_date=from, end_date=to)
      }
    }, warning = function(w) {
      desc <- paste0("WARNING @ download of sym ", sym,": ", w$message, "\n")
    }, error = function(e) {
      desc <- paste0("ERROR @ download of sym ", sym,": ", e$message, "\n")
    }, finally = {
      cat(desc)
      if (nrow(dd) > 1) {
        maxtry <- 0
        success <- TRUE
      } else {
        maxtry <- maxtry-1 
      }
    })
  }
  return(list("success" = success, "data" = dd))
}

# if any of OHLC prices contain zero, disregard line
eliminateZeroRows <- function(ts) {
  if (nrow(ts) < 1) {
    return(ts)
  } else {
    ts1 <- cbind(ts, ts[,1] * ts[,2] * ts[,3] * ts[,4])
    return(ts1[ts1[,ncol(ts1)] >0, c(1:(ncol(ts1)-1))]) 
  }
}

# if any of OHLCV prices contain values < 0, disregard line
eliminateNegRows <- function(ts) {
  if (nrow(ts) < 1) {
    return(ts)
  } else {
    ts1 <- cbind(ts, ts[,1] < 0 | ts[,2] < 0 | ts[,3] < 0 | ts[,4] < 0 | ts[,5] < 0)
    return(ts1[ts1[,ncol(ts1)] == 0, c(1:(ncol(ts1)-1))])
  }
}

# eliminate crazy price spikes
smoothPrice <- function(ts) {
  smoothCol <- function(col) {
    col1 <- cbind(col, ROC(col[,1]))
    col2 <- cbind(col1, lag(ROC(col[,1])*(-1), k=-1), lag(col1[,1]))
    col2[is.na(col2[,3]), 3] <- 2
    col3 <- cbind(col2, col2[,2] > 1 & col2[,3] > 1)
    col4 <- cbind(col3, col3[,4] * col3[,5])
    return(col4[,6] + col4[,1]*(col4[,5] == 0))
  }
  ts1<-smoothCol(ts[,1])
  ts1<-cbind(ts1, smoothCol(ts[,2]), smoothCol(ts[,3]), smoothCol(ts[,4]))
  colnames(ts1) <- c("Open", "High", "Low", "Close")
  if (ncol(ts) > 4) {
    return(cbind(ts1, ts[,c(5:ncol(ts))])[c(2:nrow(ts1)),])
  } else {
    return(ts1[c(2:nrow(ts1)),])
  }
}

# checkTSsanity. Return false for obviously wrong prices
checkTSSanity <- function(ts, minrows=200){
  if (nrow(ts) < minrows) {
    suitable <- FALSE
    desc <- paste0("Time series contains only ",nrow(ts)," (less than ", minrows,") rows.")
  } else if(max(ts[,c(1:4)] > 10000)) {
    suitable <- FALSE
    desc <- paste0("Time series max price ", max(ts[,c(1:4)]), " is too high.")
  } else if (max(ts[,5]) > 1E15) {
    suitable <- FALSE
    desc <- paste0("Time series max volume ", max(ts[,5]), " is too high.")
  } else {
    suitable <- TRUE
    desc <- ""
  }
  return(list("suitable" = suitable, "desc" = desc))
}

# for most recent rows of ticker
getPartCleanedQuandlData <- function(sym, from, to) {
  success <- FALSE
  dd <- data.frame()
  li <- robustGetQuandlData(sym, from, to)
  if (li$success == TRUE) {
    dd <- li$data
    dd <- eliminateZeroRows(dd)
    dd <- eliminateNegRows(dd)
    if (nrow(dd) > 0) {
      suitl <- checkTSSanity(dd, 1)
      if (suitl$suitable == TRUE) {
        desc <- ""
        success <- TRUE
      } else {
        desc <- paste0("Not suitable: ", suitl$desc)
      }
    } else {
      desc <- "No data after FILTER"
    }
  } else {
    desc <- "NO DATA @ download"
  }
  return(list("success" = success, "desc" = desc, "data" =dd))
}

# workflow to receive full data for given sym
getFullCleanedQuandlData <- function(sym) {
  success <- FALSE
  dd <- data.frame()
  li <- robustGetQuandlData(sym)
  if (li$success == TRUE) {
    dd <- li$data
    dd <- eliminateZeroRows(dd)
    dd <- eliminateNegRows(dd)
    if (nrow(dd) > 1) {
      dd <- smoothPrice(dd)
      suitl <- checkTSSanity(dd)
      if (suitl$suitable == TRUE) {
        desc <- ""
        success <- TRUE
      } else {
        desc <- paste0("Not suitable: ", suitl$desc)
      }
    } else {
      desc <- "No data after FILTER"
    }
  } else {
    desc <- "NO DATA @ download"
  }
  return(list("success" = success, "desc" = desc, "data" =dd))
}