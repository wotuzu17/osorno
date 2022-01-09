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

truncateQuotesTable <- function(con, sfx="") {
  if (sfx!=""){
    sfx <- paste0("_",sfx)
  }
  sql <- sprintf("TRUNCATE quotes%s", sfx)
  try(dbClearResult(dbSendQuery(con, sql)))
}

getDistinctSymbolsinQuotes <- function(con) {
  try(tickers <- dbGetQuery(con, "SELECT DISTINCT `ticker` FROM `quotes` ORDER BY `ticker`"))
  return(tickers)
}

getDistinctDates <- function(con) {
  try(dates <- dbGetQuery(con, "SELECT DISTINCT `date` FROM `quotes` ORDER BY `date`"))
  return(dates)
}

getVolumeSum <- function(con, date) {
  try(vsum <- dbGetQuery(con, sprintf("SELECT SUM(`volume`) FROM `quotes` WHERE `date` = '%s'", date)))
  return(vsum)
}

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

# loop through each lines of ts and fill to db
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

# on a given date, what ticker has a history?
getTickersOnDate <- function(con, date) {
  activeTickers <- data.frame()
  sql <- sprintf("SELECT DISTINCT `ticker` FROM `quotes` WHERE `date` <= '%s'", date)
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
getTickerDF1 <- function(con, ticker, adjust=TRUE, holidays=NULL, from=NULL, to=NULL) {
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
  TS <- suppressWarnings(dbGetQuery(con, sql))
  if (is.null(holidays)) {
    return(TS)
  } else {
    return(TS[!(TS$date %in% holidays), ])
  }
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
