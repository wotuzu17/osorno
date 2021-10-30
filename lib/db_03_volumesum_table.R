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

getDistinctVolsumDates <- function(con) {
  try(dates <- dbGetQuery(con, "SELECT DISTINCT `date` FROM `volumesum` ORDER BY `date`"))
  return(dates)
}

