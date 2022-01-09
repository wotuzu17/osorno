# investigate volume spike events

opt <- list()
opt$exchange <- "XTSX"
opt$inDB <- "127.0.0.1"

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/osorno_lib.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_01_connect_disconnect.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_02_quotes_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_03_volumesum_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_06_context_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_09_context2_table.R")
source("/home/voellenk/osorno_workdir/osorno/lib/chart_functions.R")

suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(TTR))
suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(scales))
suppressPackageStartupMessages(library(patchwork)) # to combine ggplots in one file

# determine osornodb and tickers file from exchange parameter
source("/home/voellenk/osorno_workdir/osorno/lib/select_exchange.R")

# define which databases to use
osornodb.in <- osornodb
osornodb.in$host <- opt$inDB

cat (paste(Sys.time(), "Connect to inDB", osornodb.in$db, "on host", osornodb.in$host, "\n"))
coni <- connectToOsornoDb(osornodb.in)

# get holidays
holidays <- getLowVolumeDays(coni)[,1]

# read syms from tickersfile
syms <- read.csv(tickersfile, stringsAsFactors = FALSE, na.strings="NOSUCHSTRING")

date <- "2005-01-03"
ddate <- as.Date(date)
ddate_1y <- seq(ddate, length=2, by="-1 year")[2] # Vorjahresdatum
ddate_2w <- seq(ddate, length=2, by="-14 days")[2] # vor zwei Wochen
tc <- getContextTblOnDate(coni, date)
tc$lastdata <- as.Date(tc$lastdata)
tc$ratio <- (tc[,"lpnr"] + tc[,"lnnr"]) * 100 / 
  (tc[,"lpnr"] + tc[,"lnnr"] + tc[,"zerodata"])

dim(tc)
tc1 <- tc[tc$usable == 1, ]
dim(tc1)
tc2 <- tc1[tc1$entries30d == max(tc1$entries30d), ]
dim(tc2)
tc3 <- tc2[tc2$entries52w >= max(tc2$entries52w), ]
dim(tc3)
tc4 <- tc3[tc3$lastdata >= ddate_2w, ]
dim(tc4)
tc5 <- tc4[tc4$ratio >= 15, ]
dim(tc5)

tc6 <- getAcceptableBacktestSyms(coni, date)

for (i in 1:nrow(tc6)) {
  this.sym <- tc6[i, "ticker"]
  this.name <- syms[syms$ticker == this.sym, 2]
  TS <- getTickerDF1(coni, this.sym, holidays=holidays, to=date)
  pl_all <- tsdfplot(TS, this.name, this.sym, "XTSX", start=NULL, end=NULL) 
  pl_1y <- tsdfplot(TS, this.name, this.sym, "XTSX", start=ddate_1y, end=ddate)
  pl_all + pl_1y + plot_layout(nrow=2)
  ggsave(filename=paste0(this.sym, ".png"), 
         device="png",
         width = 5,
         height = 5,
         path="/home/voellenk/osorno_workdir/experimentalplots")
}

i <- 100
this.sym <- tc6[i, "ticker"]
TS <- getTicker1(coni, this.sym, holidays=holidays, to=date)
TS$dvol <- TS$close * TS$volume

# create context2 table if not exists
createContext2TableLine(context2)
createContext2Table(coni, context2)

# diconnect from DB
cat (paste(Sys.time(), "Disconnect from inDB", osornodb.in$db, "on host", osornodb.in$host, "\n"))
dbres <- dbDisconnect(coni)
