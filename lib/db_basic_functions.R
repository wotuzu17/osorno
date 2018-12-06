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

getVolumeSum <- function(con, date) {
  try(vsum <- dbGetQuery(con, sprintf("SELECT SUM(`volume`) FROM `quotes` WHERE `date` = '%s'", date)))
  return(vsum)
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

removeSymFromQuoteTblLine <- function(sym) {
  return(sprintf("DELETE FROM `quotes` WHERE `ticker` = '%s'", sym))
}

