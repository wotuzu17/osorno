# compare TSX and London prices

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/db_basic_functions.R")
XLONtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XLON_tickers.csv.gz"
XTSXtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSX_tickers.csv.gz"
XTSEtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/XTSE_tickers.csv.gz"
OTCBtickersfile <- "/home/voellenk/osorno_workdir/data/symbols/OTCB_tickers.csv.gz"

library(stringdist)
suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(Quandl))
suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(TTR))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))

conlib <- connectToOsornoDb(osornodb_lib)
conv <- connectToOsornoDb(osornodb_xtsx)
cont <- connectToOsornoDb(osornodb_xtse)
conl <- connectToOsornoDb(osornodb_xlon)
cono <- connectToOsornoDb(osornodb_otcb)

# -------------- some function ----------------------
cleanMatchDf <- function(df) {
  ret <- data.frame()
  for (i in 1:nrow(df)) {
    if (i==1) {
      ret <- rbind(ret, df[i,])
    } else {
      if (!(df[i, "matchsym"] %in% ret[, "matchsym"])) {
        if (grepl("_", df[i, "ticker"]) == FALSE & grepl("_", df[i, "matchsym"]) == FALSE ) {
          ret <- rbind(ret, df[i,])
        }
      }
    }
  }
  return(ret)
}

feedMatchesToDB <- function(dd, ex1, ex2) {
  for(i in 1:nrow(dd)) {
    res <- checkPairExistence(conlib, ex1, ex2, dd[i,"ticker"], dd[i,"matchsym"])
    if (nrow(res) >  0) {
      cat(paste0(dd[i, "ticker"], " ", dd[i, "matchsym"], " ", dd[i, "match"], " is already in DB.\n"))
    } else {
      cat(paste0("Insert ", dd[i, "ticker"], " ", dd[i, "matchsym"], " ", dd[i, "match"], " to DB.\n"))
      sql <- insertSymlibLine(dd[i,], ex1, ex2)
      try(dbSendQuery(conlib, sql))
    }
  }
}
# ---------------------------------------------------

# parse tickersfile
parseTickersfile <- function(tickersfile) {
  result <- tryCatch({ # on success, result contains the xtsxsyms data.frame
    xtsxsyms <- read.csv(tickersfile, stringsAsFactors = FALSE)
  }, warning = function(w) {
    cat(paste0("WARNING: ", w$message, "\n"))
  }, error = function(e) {
    cat(paste0("ERROR: ", e$message, "\n"))
  }, finally = {
    if(!exists("xtsxsyms") || !is.data.frame(xtsxsyms)) {
      cat("cannot parse XTSX symbols file\n")
      xtsxsyms <- FALSE
    } else {
      cat(paste("XTSX symbols file from", 
                file.info(tickersfile)[,"mtime"], 
                "contains", nrow(xtsxsyms), "Tickers\n"))
    }
  })
  return(xtsxsyms)
}

XTSX <- parseTickersfile(XTSXtickersfile)
XTSE <- parseTickersfile(XTSEtickersfile)
XLON <- parseTickersfile(XLONtickersfile)
OTCB <- parseTickersfile(OTCBtickersfile)
XTSX$name <- trimws(sub("\\(.*\\)", "", XTSX[,2]))
XTSE$name <- trimws(sub("\\(.*\\)", "", XTSE[,2]))
XLON$name <- trimws(sub("\\(.*\\)", "", XLON[,2]))
OTCB$name <- trimws(sub("\\(.*\\)", "", OTCB[,2]))

atXTSX <- getActiveTickers(conv, Sys.Date()-5)
atXTSE <- getActiveTickers(cont, Sys.Date()-5)
atXLON <- getActiveTickers(conl, Sys.Date()-5)
atOTCB <- getActiveTickers(cono, Sys.Date()-5)

# XTSX match with XLON
XTSX$match <- ""
XTSX$matchsym <- ""
for (i in 1:nrow(XTSX)) {
  ix <- amatch(XTSX[i,"name"], XLON[,"name"], maxDist=2)
  if(!is.na(ix)) {
    XTSX[i,"match"] <- XLON[ix,"name"]
    XTSX[i,"matchsym"] <- XLON[ix,"ticker"]   
  }
}
XTSX[XTSX$match != "" & !grepl("^0", XTSX$matchsym),c(1,3,4,5)]

# XTSE match with XLON
XTSE$match <- ""
XTSE$matchsym <- ""
for (i in 1:nrow(XTSE)) {
  ix <- amatch(XTSE[i,"name"], XLON[,"name"], maxDist=2)
  if(!is.na(ix)) {
    XTSE[i,"match"] <- XLON[ix,"name"]
    XTSE[i,"matchsym"] <- XLON[ix,"ticker"]   
  }
}
XTSE[XTSE$match != "" & !grepl("^0", XTSE$matchsym),c(1,3,4,5)]

# XTSX match with OTCB
XTSX1 <- XTSX[XTSX$ticker %in% atXTSX, ]
XTSX1$match <- ""
XTSX1$matchsym <- ""
for (i in 1:nrow(XTSX1)) {
  ix <- amatch(XTSX1[i,"name"], OTCB[,"name"], maxDist=2)
  if(!is.na(ix)) {
    XTSX1[i,"match"] <- OTCB[ix,"name"]
    XTSX1[i,"matchsym"] <- OTCB[ix,"ticker"]   
  }
}
fXTSX <- cleanMatchDf(XTSX1[XTSX1$matchsym != "",])
feedMatchesToDB(fXTSX, "XTSX", "OTCB")

# XTSE match with OTCB
XTSE1 <- XTSE[XTSE$ticker %in% atXTSE, ]
XTSE1$match <- ""
XTSE1$matchsym <- ""
for (i in 1:nrow(XTSE1)) {
  ix <- amatch(XTSE1[i,"name"], OTCB[,"name"], maxDist=2)
  if(!is.na(ix)) {
    XTSE1[i,"match"] <- OTCB[ix,"name"]
    XTSE1[i,"matchsym"] <- OTCB[ix,"ticker"]   
  }
}
fXTSE <- cleanMatchDf(XTSE1[XTSE1$matchsym != "",])
feedMatchesToDB(fXTSE, "XTSE", "OTCB")

# XLON match with OTCB
XLON1 <- XLON[XLON$ticker %in% atXLON, ]
XLON1$match <- ""
XLON1$matchsym <- ""
for (i in 1:nrow(XLON1)) {
  ix <- amatch(XLON1[i,"name"], OTCB[,"name"], maxDist=2)
  if(!is.na(ix)) {
    XLON1[i,"match"] <- OTCB[ix,"name"]
    XLON1[i,"matchsym"] <- OTCB[ix,"ticker"]   
  }
}
XLON2 <- XLON1[XLON1$match != "" & !grepl("^0", XLON1$ticker), ]
fXLON <- cleanMatchDf(XLON2[XLON2$matchsym != "",])
feedMatchesToDB(fXLON, "XLON", "OTCB")
