# define structure of context2 table
context2 <- list( # sprintf char, database type, db NULL, primary key
  "ticker" =  c("s", "varchar(20)", "NOT NULL", TRUE),
  "date" = c("s", "date", "NOT NULL", TRUE),
  "logprice" = c("d", "float", "NOT NULL", FALSE),
  "logvol_1" = c("d", "float", "NOT NULL", FALSE),
  "logvol_2_7" = c("d", "float", "NOT NULL", FALSE),
  "logvol_8_63" = c("d", "float", "NOT NULL", FALSE),
  "logvol_1_144" = c("d", "float", "NOT NULL", FALSE),
  "normATR7" = c("d", "float", "NOT NULL", FALSE),
  "normATR63" = c("d", "float", "NOT NULL", FALSE), 
  "donCh21" = c("d", "float", "NOT NULL", FALSE),
  "donCh144" = c("d", "float", "NOT NULL", FALSE),
  "C2SMA3" = c("d", "float", "NOT NULL", FALSE),
  "SMA3d" = c("d", "float", "NOT NULL", FALSE),
  "C2SMA21" = c("d", "float", "NOT NULL", FALSE),
  "SMA21d" = c("d", "float", "NOT NULL", FALSE),
  "C2SMA144" = c("d", "float", "NOT NULL", FALSE),
  "SMA144d" = c("d", "float", "NOT NULL", FALSE),
  "SMA3_21" = c("d", "float", "NOT NULL", FALSE),
  "SMA21_144" = c("d", "float", "NOT NULL", FALSE)
)

createContext2TableLine <- function(ctx) {
  sql <- ("CREATE TABLE IF NOT EXISTS `context2` (")
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

createContext2Table <- function(con, ctx) {
  sql <- createContext2TableLine(ctx)
  try(dbClearResult(dbSendQuery(con, sql)))
}

truncateContext2Table <- function(con) {
  try(dbClearResult(dbSendQuery(con, "TRUNCATE `context2`")))
}

insertContext2Line <- function(con, row) {
  nnull <- !sapply(row, is.null)
  sql <- paste0("INSERT INTO `context2`(`", 
                paste0(names(row)[nnull], collapse = "`,`"), 
                "`) VALUES ('",
                paste0(unlist(row[nnull]), collapse="','"),
                "');")
  try(dbClearResult(dbSendQuery(con, sql)))
}

# same as insert, but replaces row if already exists
replaceContext2Line <- function(con, row) {
  nnull <- !sapply(row, is.null)
  sql <- paste0("REPLACE INTO `context2`(`", 
                paste0(names(row)[nnull], collapse = "`,`"), 
                "`) VALUES ('",
                paste0(unlist(row[nnull]), collapse="','"),
                "');")
  try(dbClearResult(dbSendQuery(con, sql)))
}

getContext2Line <- function(ticker, date) {
  sql <- sprintf("SELECT * FROM `context2` WHERE `ticker`='%s' AND `date`='%s'", ticker, date)
  return(suppressWarnings(dbGetQuery(con, sql)))
}

getContext2TblOnDate <- function(con, date) {
  sql <- sprintf("SELECT * FROM `context2` WHERE `date`='%s'", date)
  return(suppressWarnings(dbGetQuery(con, sql)))
}
