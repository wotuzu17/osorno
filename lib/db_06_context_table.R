# define structure of context table
context <- list( # sprintf char, database type, db NULL, primary key
  "ticker" =  c("s", "varchar(20)", "NOT NULL", TRUE),
  "date" = c("s", "date", "NOT NULL", TRUE),
  "usable" = c("d", "tinyint(1)", "NOT NULL", FALSE),
  "first" = c("s", "date", "NOT NULL", FALSE),
  "firstdata" = c("s", "date", "NOT NULL", FALSE),
  "last" = c("s", "date", "NOT NULL", FALSE),
  "lastdata" = c("s", "date", "NOT NULL", FALSE),
  "entries" = c("d", "int(11)", "NULL", FALSE),
  "entriesdata" = c("d", "int(11)", "NULL", FALSE),
  "entries52w" = c("d", "int(11)", "NULL", FALSE),
  "entries30d" = c("d", "int(11)", "NULL", FALSE),
  "adjustments" = c("d", "int(11)", "NULL", FALSE),
  "zerodata" = c("d", "int(11)", "NULL", FALSE),
  "lpnr" = c("d", "int(11)", "NULL", FALSE),
  "lnnr" = c("d", "int(11)", "NULL", FALSE)
)

createContextTableLine <- function(con, ctx) {
  sql <- ("CREATE TABLE IF NOT EXISTS `context` (")
  for (i in 1:length(ctx)) {
    sql <- paste0(sql, "`", names(ctx)[i], "` ", 
                  ctx[[i]][2], " ",
                  ctx[[i]][3], ", ")
  }
  sql <- paste0(sql, " PRIMARY KEY (")
  firstrun <- TRUE
  for (i in 1:length(ctx)) {
    if (ctx[[i]][4] == TRUE) {
      if (firstrun == FALSE) {
        sql <- paste0(sql, ", ")
      }
      sql <- paste0(sql, "`", names(ctx)[i], "`")
      firstrun <- FALSE
    }
  }
  sql <- paste0(sql, ")) ENGINE=MyISAM DEFAULT CHARSET=ascii;")
  return(sql)
}

createContextTable <- function(con, ctx) {
  sql <- createContextTableLine(con, ctx)
  try(dbClearResult(dbSendQuery(con, sql)))
}

truncateContextTable <- function(con) {
  try(dbClearResult(dbSendQuery(con, "TRUNCATE `context`")))
}

insertContextLine <- function(con, row) {
  nnull <- !sapply(row, is.null)
  sql <- paste0("INSERT INTO `context`(`", 
                paste0(names(row)[nnull], collapse = "`,`"), 
                "`) VALUES ('",
                paste0(unlist(row[nnull]), collapse="','"),
                "');")
  try(dbClearResult(dbSendQuery(con, sql)))
}

# same as insert, but replaces row if already exists
replaceContextLine <- function(con, row) {
  nnull <- !sapply(row, is.null)
  sql <- paste0("REPLACE INTO `context`(`", 
                paste0(names(row)[nnull], collapse = "`,`"), 
                "`) VALUES ('",
                paste0(unlist(row[nnull]), collapse="','"),
                "');")
  try(dbClearResult(dbSendQuery(con, sql)))
}

getContextLine <- function(ticker, date) {
  sql <- sprintf("SELECT * FROM `context` WHERE `ticker`='%s' AND `date`='%s'", ticker, date)
  return(suppressWarnings(dbGetQuery(con, sql)))
}

getContextTblOnDate <- function(con, date) {
  sql <- sprintf("SELECT * FROM `context` WHERE `date`='%s'", date)
  return(suppressWarnings(dbGetQuery(con, sql)))
}
