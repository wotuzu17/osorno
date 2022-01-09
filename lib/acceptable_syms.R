# function to determine acceptable syms for backtesting
# for a given day
# requires context table

getAcceptableBacktestSyms <- function(con, date) {
  ddate <- as.Date(date)
  ddate_2w <- seq(ddate, length=2, by="-14 days")[2] # vor zwei Wochen
  tc <- getContextTblOnDate(con, ddate)
  tc$lastdata <- as.Date(tc$lastdata) # convert string to date
  tc$ratio <- (tc[,"lpnr"] + tc[,"lnnr"]) * 100 / 
    (tc[,"lpnr"] + tc[,"lnnr"] + tc[,"zerodata"])
  tc1 <- tc[tc$usable == 1, ]
  tc2 <- tc1[tc1$entries30d == max(tc1$entries30d), ]
  tc3 <- tc2[tc2$entries52w == max(tc2$entries52w), ]
  tc4 <- tc3[tc3$lastdata >= ddate_2w, ]
  tc5 <- tc4[tc4$ratio >= 15, ]
  return(tc5)
}