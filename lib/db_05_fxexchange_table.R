createFxExchangeTable <- function(con) {
  sql <- c("CREATE TABLE IF NOT EXISTS `fxexchange` (
  `pair` varchar(6) NOT NULL,
  `date` date NOT NULL,
  `quote` double,
  PRIMARY KEY (`pair`, `date`)
  ) ENGINE=MyISAM DEFAULT CHARSET=ascii;")
  try(dbClearResult(dbSendQuery(con, sql)))
}

truncateFxExchangeTable <- function(con) {
  try(dbClearResult(dbSendQuery(con, "TRUNCATE fxexchange")))
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

# returns whole currency pair
getFxExchangePair <- function(con, pair) {
  sql <- (sprintf("SELECT `date`, `quote` from `fxexchange` WHERE `pair` = '%s' ORDER BY `date`", pair))
  return(suppressWarnings(dbGetQuery(con, sql)))
}

