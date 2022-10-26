opt <- list()
opt$exchange <- "XTSX"
opt$inDB <- "127.0.0.1"

# global definitions
source("/home/voellenk/.osornodb.R")   # secret key file
source("/home/voellenk/osorno_workdir/osorno/lib/osorno_lib.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_01_connect_disconnect.R")
source("/home/voellenk/osorno_workdir/osorno/lib/db_02_quotes_table.R")

suppressPackageStartupMessages(library(xts))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(TTR))

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

# get distinct dates from quotes
dta <- getDistinctDates(coni, TRUE)
dtu <- getDistinctDates(coni, FALSE)


ta <- getTickersOnDate1(coni, date, TRUE)
tu <- getTickersOnDate1(coni, date, FALSE)

# diconnect from DB
cat (paste(Sys.time(), "Disconnect from inDB", osornodb.in$db, "on host", osornodb.in$host, "\n"))
dbres <- dbDisconnect(coni)
