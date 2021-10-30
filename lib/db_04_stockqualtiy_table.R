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

dropStockQualityTable <- function(con) {
  sql <- c("DROP TABLE `stockquality`")
  try(dbClearResult(dbSendQuery(con, sql)))
}

# returns whole stock quality table
getStockQualityTable <- function(con) {
  sql <- ("SELECT * from `stockquality` ORDER BY `ticker`")
  return(suppressWarnings(dbGetQuery(con, sql)))
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
