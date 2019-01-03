truncateQuotesTable <- function(con) {
  try(dbClearResult(dbSendQuery(con, "TRUNCATE quotes")))
}

# create quotes table if not exists
createQuotesTable <- function(con) {
  sql <- c("CREATE TABLE IF NOT EXISTS `quotes` (
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
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;")
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
  `adjustments` int(11) NULL,
  `zerodata` int(11) NULL,
  `tooshort` tinyint(1) NULL,
  PRIMARY KEY (`ticker`) 
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;")
  try(dbClearResult(dbSendQuery(con, sql)))
}

dropStockQualityTable <- function(con) {
  sql <- c("DROP TABLE `stockquality`")
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

insertStockQualityLine <- function(sym, first, firstd, last, lastd, entries, entriesd, adjustments, zerod, tooshort) {
  sql <- sprintf("INSERT INTO `stockquality` 
                 (`ticker`, `first`, `firstdata`, `last`, `lastdata`, 
                  `entries`, `entriesdata`, `adjustments`, `zerodata`, `tooshort`) VALUES 
                 ('%s', '%s','%s', '%s', '%s', '%d', '%d', '%d', '%d', '%d');", 
                 sym, first, firstd, last, lastd, entries, entriesd, adjustments, zerod, tooshort)
  return(sql)
}

# sample tsline:
#Date Open High  Low Close Volume Adjustment Factor Adjustment Type
#2005-08-04 0.19 0.19 0.16  0.19  21000                NA              NA
insertQuoteLine <- function(sym, tsline) {
  if (is.na(tsline[1,7])) {
    sql <- sprintf("INSERT INTO `quotes` (`ticker`, `date`, `open`, `high`, `low`, `close`, `volume`) 
          VALUES ('%s', '%s', '%f', '%f', '%f', '%f', '%f');",
                   sym, as.character(index(tsline[1,])),
                   tsline[1, 1], tsline[1, 2], tsline[1, 3], tsline[1, 4], tsline[1, 5])
  } else {
    sql <- sprintf("INSERT INTO `quotes` (`ticker`, `date`, `open`, `high`, `low`, `close`, `volume`, `adj_factor`, `adj_type`) 
          VALUES ('%s', '%s', '%f', '%f', '%f', '%f', '%f', '%f', '%s');",
                   sym, as.character(index(tsline[1,])),
                   tsline[1, 1], tsline[1, 2], tsline[1, 3], tsline[1, 4], tsline[1, 5], 
                   tsline[1, 6], tsline[1, 7])
  }
  return(sql)
}

# update line
updateQuoteLine <- function(sym, tsline) {
  if (is.na(tsline[1,7])) {
    sql <- sprintf("UPDATE `quotes` SET `open` = '%f', `high` = '%f', `low` = '%f', `close` = '%f', `volume` = '%f' 
                   WHERE `quotes`.`ticker` = '%s' AND `quotes`.`date` = '%s';",
                   tsline[1, 1], tsline[1, 2], tsline[1, 3], tsline[1, 4], tsline[1, 5], sym, as.character(index(tsline[1,])))
  } else {
    sql <- sprintf("UPDATE `quotes` SET `open` = '%f', `high` = '%f', `low` = '%f', `close` = '%f', `volume` = '%f', `adj_factor` = '%f', `adj_type` = '%s'
                   WHERE `quotes`.`ticker` = '%s' AND `quotes`.`date` = '%s';",
                   tsline[1, 1], tsline[1, 2], tsline[1, 3], tsline[1, 4], tsline[1, 5], tsline[1, 6], tsline[1, 7],
                   sym, as.character(index(tsline[1,])))
  }
  return(sql)
}

insertFullSymTS <- function(con, sym, ts) {
  for(i in 1:nrow(ts)) {
    dbSendQuery(con, insertQuoteLine(sym, ts[i,]))
  }
}

removeSymFromQuoteTblLine <- function(sym) {
  return(sprintf("DELETE FROM `quotes` WHERE `ticker` = '%s'", sym))
}

removeSymFromQuoteTbl <- function(con, sym) {
  try(dbres <- dbSendQuery(con, removeSymFromQuoteTblLine(sym)))
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

# gets full data set with all columns
# returns data.frame, for stockquality table
getTickerDF <- function(con, ticker) {
  sql <- sprintf("SELECT * FROM `quotes` WHERE `ticker` = '%s' ORDER BY `date`", ticker)
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


