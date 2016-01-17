
require(data.table)
require(ggplot2)
source("rdb/mysql.R")

query <- "select co_emd from tdatasets;"
data <- execute.select(query)


p <- ggplot(data, aes(x=co_emd)) + 
        geom_histogram(binwidth=1) +
        scale_y_sqrt()

print(p)