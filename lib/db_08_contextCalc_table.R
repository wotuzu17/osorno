# create contextCalc table if not exists
# can also be used for context2
createContextCalcTable <- function(con, version="") {
  sql <- sprintf("CREATE TABLE IF NOT EXISTS `contextCalc%s` (
  `date` date NOT NULL,
  `host` varchar(50) NULL,
  `start` datetime NULL,
  `end` datetime NULL,
  `numberofsyms` int(11) NULL,
  PRIMARY KEY (`date`) 
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;", version)
  try(dbClearResult(dbSendQuery(con, sql)))
}

dropContextCalcTable <- function(con, version="") {
  sql <- sprintf("DROP TABLE `contextCalc%s`", version)
  try(dbClearResult(dbSendQuery(con, sql)))
}

truncateContextCalcTable <- function(con, version="") {
  sql <- sprintf("TRUNCATE TABLE `contextCalc%s`", version)
  try(dbClearResult(dbSendQuery(con, sql)))
}

insertEmptyDateLine <- function(con, date, version="") {
  sql <- sprintf("INSERT INTO `contextCalc%s` (`date`) VALUES ('%s');",
                 version, as.character(date))
  return(sql)
}

getOldestOpenDate <- function(con, version="") {
  sql <- sprintf("SELECT * FROM `contextCalc%s` WHERE `host` IS NULL ORDER BY `date` LIMIT 1",
                 version)
  return(suppressWarnings(dbGetQuery(con, sql)))
}

setProcessStartMark <- function(con, date, host, version="") {
  sql <- sprintf("UPDATE `contextCalc%s` 
                 SET `host` = '%s', `start` = NOW() 
                 WHERE `contextCalc%s`.`date` = '%s'", version, host, version, date)
  try(dbClearResult(dbSendQuery(con, sql)))
}

setProcessStopMark <- function(con, date, nsyms, version="") {
  sql <- sprintf("UPDATE `contextCalc%s` 
                 SET `end` = NOW(), `numberofsyms` = '%d' 
                 WHERE `contextCalc%s`.`date` = '%s'", version, nsyms, version, date)
  try(dbClearResult(dbSendQuery(con, sql)))
}

verifyProcessMark <- function(con, date, host, version="") {
  sql <- sprintf("SELECT * FROM `contextCalc%s` 
                 WHERE `date` = '%s' AND `host` = '%s'", version, date, host)
  return(suppressWarnings(dbGetQuery(con, sql)))
}
