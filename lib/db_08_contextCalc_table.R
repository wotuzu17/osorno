# create contextCalc table if not exists
createContextCalcTable <- function(con) {
  sql <- c("CREATE TABLE IF NOT EXISTS `contextCalc` (
  `date` date NOT NULL,
  `host` varchar(50) NULL,
  `start` datetime NULL,
  `end` datetime NULL,
  `numberofsyms` int(11) NULL,
  PRIMARY KEY (`date`) 
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;")
  try(dbClearResult(dbSendQuery(con, sql)))
}

dropContextCalcTable <- function(con) {
  sql <- c("DROP TABLE `contextCalc`")
  try(dbClearResult(dbSendQuery(con, sql)))
}

truncateContextCalcTable <- function(con) {
  sql <- c("TRUNCATE TABLE `contextCalc`")
  try(dbClearResult(dbSendQuery(con, sql)))
}

insertEmptyDateLine <- function(con, date) {
  sql <- sprintf("INSERT INTO `contextCalc` (`date`) VALUES ('%s');",
                 as.character(date))
  return(sql)
}

getOldestOpenDate <- function(con) {
  sql <- "SELECT * FROM `contextCalc` WHERE `host` IS NULL ORDER BY `date` LIMIT 1"
  return(suppressWarnings(dbGetQuery(con, sql)))
}

setProcessStartMark <- function(con, date, host) {
  sql <- sprintf("UPDATE `contextCalc` 
                 SET `host` = '%s', `start` = NOW() 
                 WHERE `contextCalc`.`date` = '%s'", host, date)
  try(dbClearResult(dbSendQuery(con, sql)))
}

setProcessStopMark <- function(con, date, nsyms) {
  sql <- sprintf("UPDATE `contextCalc` 
                 SET `end` = NOW(), `numberofsyms` = '%d' 
                 WHERE `contextCalc`.`date` = '%s'", nsyms, date)
  try(dbClearResult(dbSendQuery(con, sql)))
}

verifyProcessMark <- function(con, date, host) {
  sql <- sprintf("SELECT * FROM `contextCalc` 
                 WHERE `date` = '%s' AND `host` = '%s'", date, host)
  return(suppressWarnings(dbGetQuery(con, sql)))
}
