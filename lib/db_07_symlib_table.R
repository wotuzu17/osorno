getSymlib <- function(con) {
  try(symlib <- dbGetQuery(con, "SELECT * FROM `symlib`"))
  return(symlib)
}

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