# create contextCalc table if not exists
createContextCalcTable <- function(con) {
  sql <- c("CREATE TABLE IF NOT EXISTS `contextCalc` (
  `date` date NOT NULL,
  `host` varchar(50) NULL,
  `task` varchar(50) NULL,
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