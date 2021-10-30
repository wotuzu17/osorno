# robust connect
connectToOsornoDb <- function(osornodb) {
  result <- tryCatch({ # on success, result contains the connection var con
    con <- dbConnect(MySQL(), 
                     user=osornodb$user, 
                     password=osornodb$password, 
                     dbname=osornodb$db, 
                     host=osornodb$host)
  }, warning = function(w) {
    cat(paste0("WARNING: ", w$message, "\n"))
  }, error = function(e) {
    cat(paste0("ERROR: ", e$message, "\n"))
  }, finally = {
    if(exists("con")) {
      cat (paste(Sys.time(), "Connect to db", osornodb$db), "on host", osornodb$host, "\n")
    } else {
      cat ("unable to connect to database, stopping now.\n")
      echoStopMark()
      stop()
    }
  })
  return(con)
}

disconnectFromOsornoDb<- function(con, osornodb) {
  cat (paste(Sys.time(), "Disconnect from db", osornodb$db, "on host", osornodb$host, "\n"))
  dbres <- dbDisconnect(con)
}