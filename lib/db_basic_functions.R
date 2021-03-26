# robust connect
connectToOsornoDb <- function(osornodb) {
  result <- tryCatch({ # on success, result contains the connection var con
    con <- dbConnect(MySQL(), 
                     user=osornodb$user, 
                     password=osornodb$password, 
                     dbname=osornodb$db, 
                     host=osornodb$host)
  }, warning = function(w) {
    cat(paste0("WARNING: ", w$message, "\n"))
  }, error = function(e) {
    cat(paste0("ERROR: ", e$message, "\n"))
  }, finally = {
    if(exists("con")) {
      cat (paste(Sys.time(), "Connect to db", osornodb$db), "on host", osornodb$host, "\n")
    } else {
      cat ("unable to connect to database, stopping now.\n")
      echoStopMark()
      stop()
    }
  })
  return(con)
}

disconnectFromOsornoDb<- function(con, osornodb) {
  cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
  dbres <- dbDisconnect(con)
}

truncateQuotesTable <- function(con, sfx="") {
  if (sfx!=""){
    sfx <- paste0("_",sfx)
  }
  sql <- sprintf("TRUNCATE quotes%s", sfx)
  try(dbClearResult(dbSendQuery(con, sql)))
}

# create quotes table if not exists
createQuotesTable <- function(con, sfx="") {
  if (sfx!=""){
    sfx <- paste0("_",sfx)
  }
  sql <- sprintf("CREATE TABLE IF NOT EXISTS `quotes%s` (
  `ticker` varchar(20) NOT NULL,
  `date` date NOT NULL,
  `open` decimal(10,4),
  `high` decimal(10,4),
  `low` decimal(10,4),
  `close` decimal(10,4),
  `volume` float DEFAULT NULL,
  `adj_factor` float DEFAULT NULL,
  `adj_type` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`ticker`, `date`)
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;", sfx)
  try(dbClearResult(dbSendQuery(con, sql)))
}

# create volumesum table if not exists
createVolumeSumTable <- function(con) {
  sql <- c("CREATE TABLE IF NOT EXISTS `volumesum` (
  `date` date NOT NULL,
  `volume` int(11) NULL,
  PRIMARY KEY (`date`)
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;")
  try(dbClearResult(dbSendQuery(con, sql)))
}

truncateVolumeSumTable <- function(con) {
  try(dbClearResult(dbSendQuery(con, "TRUNCATE `volumesum`")))
}

# create stockquality table if not exists
createStockQualityTable <- function(con) {
  sql <- c("CREATE TABLE IF NOT EXISTS `stockquality` (
  `ticker` varchar(20) NOT NULL,
  `first` date NOT NULL,
  `firstdata` date NULL,
  `last` date NOT NULL,
  `lastdata` date NULL,
  `entries` int(11) NULL,
  `entriesdata` int(11) NULL,
  `entries.52w` int(11) NULL,
  `entries.30d` int(11) NULL,
  `adjustments` int(11) NULL,
  `zerodata` int(11) NULL,
  `lpnr` int(11) NULL,
  `lnnr` int(11) NULL,
  `spnr` float DEFAULT NULL,
  `snnr` float DEFAULT NULL,
  `tooshort` tinyint(1) NULL,
  PRIMARY KEY (`ticker`) 
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;")
  try(dbClearResult(dbSendQuery(con, sql)))
}

# create stockquality table if not exists
createStockQualityDayTable <- function(con, day) {
  sql <- sprintf("CREATE TABLE IF NOT EXISTS `stockquality_%s` (
  `ticker` varchar(20) NOT NULL,
  `first` date NOT NULL,
  `firstdata` date NULL,
  `last` date NOT NULL,
  `lastdata` date NULL,
  `entries` int(11) NULL,
  `entriesdata` int(11) NULL,
  `entries.52w` int(11) NULL,
  `entries.30d` int(11) NULL,
  `adjustments` int(11) NULL,
  `zerodata` int(11) NULL,
  `lpnr` int(11) NULL,
  `lnnr` int(11) NULL,
  `spnr` float DEFAULT NULL,
  `snnr` float DEFAULT NULL,
  `tooshort` tinyint(1) NULL,
  PRIMARY KEY (`ticker`) 
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;", day)
  try(dbClearResult(dbSendQuery(con, sql)))
}

dropStockQualityTable <- function(con) {
  sql <- c("DROP TABLE `stockquality`")
  try(dbClearResult(dbSendQuery(con, sql)))
}

dropStockQualityDayTable <- function(con, day) {
  sql <- sprintf("DROP TABLE `stockquality_%s`", day)
  try(dbClearResult(dbSendQuery(con, sql)))
}

insertDayVolumeSumLine <- function(con, date, volume) {
  sql <- sprintf("INSERT INTO `volumesum` (`date`, `volume`) VALUES ('%s', '%d');",
                 as.character(date), volume)
  return(sql)
}

# on holidays, the sum of volume is 0
getLowVolumeDays <- function(con, maxvol=0) {
  sql <- sprintf("SELECT * FROM `volumesum` WHERE `volume` <= %d ", maxvol)
  try(lowvoldays <- dbGetQuery(con, sql))
  return(lowvoldays)
}

getDistinctSymbolsinQuotes <- function(con) {
  try(tickers <- dbGetQuery(con, "SELECT DISTINCT `ticker` FROM `quotes`"))
  return(tickers)
}

getDistinctDates <- function(con) {
  try(dates <- dbGetQuery(con, "SELECT DISTINCT `date` FROM `quotes` ORDER BY `date`"))
  return(dates)
}

getDistinctVolsumDates <- function(con) {
  try(dates <- dbGetQuery(con, "SELECT DISTINCT `date` FROM `volumesum` ORDER BY `date`"))
  return(dates)
}

getVolumeSum <- function(con, date) {
  try(vsum <- dbGetQuery(con, sprintf("SELECT SUM(`volume`) FROM `quotes` WHERE `date` = '%s'", date)))
  return(vsum)
}

getSymlib <- function(con) {
  try(symlib <- dbGetQuery(con, "SELECT * FROM `symlib`"))
  return(symlib)
}

insertStockQualityLine <- function(sym, day, first, firstd, last, lastd, entries, entriesd, 
                                   entries.52w, entries.30d, adjustments, zerod, 
                                   lpnr, lnnr, spnr, snnr, tooshort) {
  dd <- ifelse(is.null(day), "", sprintf("_%s", day))
  sql <- sprintf("INSERT INTO `stockquality%s` 
                 (`ticker`, `first`, `firstdata`, `last`, `lastdata`, 
                  `entries`, `entriesdata`, `entries.52w`, `entries.30d`, 
                  `adjustments`, `zerodata`, 
                  `lpnr`, `lnnr`, `spnr`, `snnr`, `tooshort`) VALUES 
                 ('%s', '%s', '%s', '%s', '%s', 
                  '%d', '%d', '%d', '%d', 
                  '%d', '%d', 
                  '%d', '%d', '%f', '%f', '%d');",
                 dd, 
                 sym, first, firstd, last, lastd, 
                 entries, entriesd, entries.52w, entries.30d,
                 adjustments, zerod, 
                 lpnr, lnnr, spnr, snnr, tooshort)
  return(sql)
}

# sample tsline:
#Date Open High  Low Close Volume Adjustment Factor Adjustment Type
#2005-08-04 0.19 0.19 0.16  0.19  21000                NA              NA
insertQuoteLine <- function(sym, tsline, adjust=TRUE) {
  sfx <- ifelse(adjust, "", "_UADJ")
  if (is.na(tsline[1,7])) {
    sql <- sprintf("INSERT INTO `quotes%s` (`ticker`, `date`, `open`, `high`, `low`, `close`, `volume`) 
          VALUES ('%s', '%s', '%f', '%f', '%f', '%f', '%f');",
                   sfx, sym, as.character(index(tsline[1,])),
                   tsline[1, 1], tsline[1, 2], tsline[1, 3], tsline[1, 4], tsline[1, 5])
  } else {
    sql <- sprintf("INSERT INTO `quotes%s` (`ticker`, `date`, `open`, `high`, `low`, `close`, `volume`, `adj_factor`, `adj_type`) 
          VALUES ('%s', '%s', '%f', '%f', '%f', '%f', '%f', '%f', '%s');",
                   sfx, sym, as.character(index(tsline[1,])),
                   tsline[1, 1], tsline[1, 2], tsline[1, 3], tsline[1, 4], tsline[1, 5], 
                   tsline[1, 6], tsline[1, 7])
  }
  return(sql)
}

# update line
updateQuoteLine <- function(sym, tsline, adjust=TRUE) {
  sfx <- ifelse(adjust, "", "_UADJ")
  if (is.na(tsline[1,7])) {
    sql <- sprintf("UPDATE `quotes%s` SET `open` = '%f', `high` = '%f', `low` = '%f', `close` = '%f', `volume` = '%f' 
                   WHERE `quotes`.`ticker` = '%s' AND `quotes`.`date` = '%s';",
                   sfx, tsline[1, 1], tsline[1, 2], tsline[1, 3], tsline[1, 4], tsline[1, 5], sym, as.character(index(tsline[1,])))
  } else {
    sql <- sprintf("UPDATE `quotes%s` SET `open` = '%f', `high` = '%f', `low` = '%f', `close` = '%f', `volume` = '%f', `adj_factor` = '%f', `adj_type` = '%s'
                   WHERE `quotes`.`ticker` = '%s' AND `quotes`.`date` = '%s';",
                   sfx, tsline[1, 1], tsline[1, 2], tsline[1, 3], tsline[1, 4], tsline[1, 5], tsline[1, 6], tsline[1, 7],
                   sym, as.character(index(tsline[1,])))
  }
  return(sql)
}

insertFullSymTS <- function(con, sym, ts, adjust=TRUE) {
  for(i in 1:nrow(ts)) {
    dbSendQuery(con, insertQuoteLine(sym, ts[i,], adjust))
  }
}

removeSymFromQuoteTblLine <- function(sym) {
  return(sprintf("DELETE FROM `quotes` WHERE `ticker` = '%s'", sym))
}

removeSymFromUADJQuoteTblLine <- function(sym) {
  return(sprintf("DELETE FROM `quotes_UADJ` WHERE `ticker` = '%s'", sym))
}

removeSymFromQuoteTbl <- function(con, sym) {
  try(dbres <- dbSendQuery(con, removeSymFromQuoteTblLine(sym)))
}

removeSymFromUADJQuoteTbl <- function(con, sym) {
  try(dbres <- dbSendQuery(con, removeSymFromUADJQuoteTblLine(sym)))
}

getMostRecentDateInQuotes <- function(con) {
  mostRecentDate <- data.frame()
  sql <- "SELECT `date` FROM `quotes` ORDER BY `date` DESC LIMIT 1"
  try(mostRecentDate <- dbGetQuery(con, sql))
  if (nrow(mostRecentDate) == 1) {
    return(mostRecentDate[1,1]) # date as character
  } else {
    return(NULL)
  }
}

getMostRecentTickerDate <- function(con, ticker) {
  mostRecentDate <- data.frame()
  sql <- sprintf("SELECT `date` FROM `quotes` WHERE `ticker` = '%s' ORDER BY `date` DESC LIMIT 1", ticker)
  try(mostRecentDate <- dbGetQuery(con, sql))
  if (nrow(mostRecentDate) == 1) {
    return(mostRecentDate[1,1]) # date as character
  } else {
    return(NULL)
  }
}

getActiveTickers <- function(con, date) {
  activeTickers <- data.frame()
  sql <- sprintf("SELECT DISTINCT `ticker` FROM `quotes` WHERE `date` >= '%s'", date)
  try(activeTickers <- dbGetQuery(con, sql))
  if (nrow(activeTickers) > 0) {
    return(activeTickers[,1]) # ticker names as vector
  } else {
    return(NULL)
  }
}

# legacy function (quotes table with adjusted quotes only)
getTicker <- function(con, ticker, holidays=NULL, from=NULL, to=NULL) {
  fromclause <- ""
  toclause <- ""
  if (!is.null(from)) {
    fromclause <- sprintf("AND `date` >= '%s'", from)
  }
  if (!is.null(to)) {
    toclause <- sprintf("AND `date` <= '%s'", to)
  }
  sql <- sprintf("SELECT `date`,`open`,`high`,`low`,`close`,`volume` 
                 FROM `quotes` 
                 WHERE `ticker` = '%s' %s %s
                 ORDER BY `date`", ticker, fromclause, toclause)
  try(ts <- suppressWarnings(dbGetQuery(con, sql)))
  if (!is.null(holidays)) {
    ts <- ts[!ts$date %in% holidays,]
  }
  return(xts(ts[,-1], order.by=as.Date(ts[,1])))
}

# new function. choose from adjusted or unadjusted quotes
getTicker1 <- function(con, ticker, adjust=TRUE, holidays=NULL, from=NULL, to=NULL) {
  sfx <- ifelse(adjust, "", "_UADJ")
  fromclause <- ""
  toclause <- ""
  if (!is.null(from)) {
    fromclause <- sprintf("AND `date` >= '%s'", from)
  }
  if (!is.null(to)) {
    toclause <- sprintf("AND `date` <= '%s'", to)
  }
  sql <- sprintf("SELECT `date`,`open`,`high`,`low`,`close`,`volume` 
                 FROM `quotes%s` 
                 WHERE `ticker` = '%s' %s %s
                 ORDER BY `date`", sfx, ticker, fromclause, toclause)
  try(ts <- suppressWarnings(dbGetQuery(con, sql)))
  if (!is.null(holidays)) {
    ts <- ts[!ts$date %in% holidays,]
  }
  return(xts(ts[,-1], order.by=as.Date(ts[,1])))
}

# gets full data set with all columns
# returns data.frame, for stockquality table
# legacy function (quotes table with adjusted quotes only)
getTickerDF <- function(con, ticker, from=NULL, to=NULL) {
  fromclause <- ""
  toclause <- ""
  if (!is.null(from)) {
    fromclause <- sprintf("AND `date` >= '%s'", from)
  }
  if (!is.null(to)) {
    toclause <- sprintf("AND `date` <= '%s'", to)
  }
  sql <- sprintf("SELECT * FROM `quotes` WHERE `ticker` = '%s' %s %s
                 ORDER BY `date`", ticker, fromclause, toclause)
  return(suppressWarnings(dbGetQuery(con, sql)))
}

# new function. choose from adjusted or unadjusted quotes
getTickerDF1 <- function(con, ticker, adjust=TRUE, from=NULL, to=NULL) {
  sfx <- ifelse(adjust, "", "_UADJ")
  fromclause <- ""
  toclause <- ""
  if (!is.null(from)) {
    fromclause <- sprintf("AND `date` >= '%s'", from)
  }
  if (!is.null(to)) {
    toclause <- sprintf("AND `date` <= '%s'", to)
  }
  sql <- sprintf("SELECT * FROM `quotes%s` WHERE `ticker` = '%s' %s %s
                 ORDER BY `date`", sfx, ticker, fromclause, toclause)
  return(suppressWarnings(dbGetQuery(con, sql)))
}

getTickerUntilDate <- function(con, ticker, to, holidays=NULL) {
  sql <- sprintf("SELECT `date`,`open`,`high`,`low`,`close`,`volume` FROM `quotes` 
                 WHERE `ticker` = '%s' AND `date` <= '%s' ORDER BY `date`", ticker, to)
  try(ts <- suppressWarnings(dbGetQuery(con, sql)))
  if (!is.null(holidays)) {
    ts <- ts[!ts$date %in% holidays,]
  }
  return(xts(ts[,-1], order.by=as.Date(ts[,1])))
}

# returns whole stock quality table
getStockQualityTable <- function(con) {
  sql <- ("SELECT * from `stockquality` ORDER BY `ticker`")
  return(suppressWarnings(dbGetQuery(con, sql)))
}

# ------- fxexchange functions -------------------------------
createFxExchangeTable <- function(con) {
  sql <- c("CREATE TABLE IF NOT EXISTS `fxexchange` (
  `pair` varchar(6) NOT NULL,
  `date` date NOT NULL,
  `quote` double,
  PRIMARY KEY (`pair`, `date`)
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;")
  try(dbClearResult(dbSendQuery(con, sql)))
}

insertFxExchangeLine <- function(pair, date, quote) {
  sql <- sprintf("INSERT INTO `fxexchange` (`pair`, `date`, `quote`) VALUES ('%s', '%s', '%f');",
                 pair, as.character(date), quote)
  return(sql)
}

# the info pairs is given in colname of second column
insertFxExchangeChunk <- function(con, df) {
  pair <- colnames(df)[2]
  for (i in 1:nrow(df)) {
    dbSendQuery(con, insertFxExchangeLine(pair, df[i,1], df[i,2]))
  }
  return(FALSE)
}

# returns whole stock quality table
getFxExchangePair <- function(con, pair) {
  sql <- (sprintf("SELECT `date`, `quote` from `fxexchange` WHERE `pair` = '%s' ORDER BY `date`", pair))
  return(suppressWarnings(dbGetQuery(con, sql)))
}

truncateFxExchangeTable <- function(con) {
  try(dbClearResult(dbSendQuery(con, "TRUNCATE fxexchange")))
}

#----------------- symlib functions -------------
insertSymlibLine <- function(line, ex1, ex2) {
  exes <- c("XTSX", "XTSE", "XLON", "OTCB")
  reexes <- exes[which(!exes %in% c(ex1, ex2))]
  sql <- sprintf("INSERT INTO `symlib` (`ix`, `%s`, `%s`, `%s`, `%s`, `name`) VALUES 
                 (NULL, '%s', '%s', NULL, NULL, '%s')", 
                 ex1, ex2, reexes[1], reexes[2],
                 line[1,"ticker"], line[1,"matchsym"], line[1,"match"])
  return(sql)
}

checkPairExistence <- function(con,ex1, ex2, sym1, sym2) {
  sql <- sprintf("SELECT * FROM `symlib` WHERE `%s` = '%s' AND `%s` = '%s' ORDER BY `ix`",
                 ex1, sym1, ex2, sym2)
  try(dbGetQuery(con, sql))
}


