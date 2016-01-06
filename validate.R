require(data.table)
require(ggplot2)
source("rdb/mysql.R")

# What can we say about the validity of found coordinates?
# Compare coordinates found in pdf's with coordinates already known in EMD.

# We will make comparisons for each method by which coordinates were found.

# What is the percentage of 'correct' points? Correct in the sense that the found
# point is within a reasonable distance from the known points.
# What is reasonable distance? 

# Step 1
# - compute medium x and y for each dataset with coordinates
# - compute the standard deviation per x and y if there is more than 1 point per dataset
# - set the field for standard deviation to 0 for datasets with only one coordinate
# - save this as data/mean_emd.csv
compute.mean.emd <- function() {
        query <- paste("SELECT tds_id, datasetId, coor_x, coor_y",
                       "FROM tdatasets",
                       "JOIN tspatials ON parent_datasetId = datasetId",
                       "WHERE source = 'emd';",
                       sep = "\n")
        dt <- as.data.table(execute.select(query))
        message("got ", nrow(dt), " rows for emd.")
        
        # aggregate on mean of x and y per dataset
        dtm <- dt[, j = list(
                n_emd = .N, # number of coordinates
                mean_x = as.integer(mean(coor_x)), 
                mean_y = as.integer(mean(coor_y)),
                sd_x = as.integer(ifelse(is.na(sd(coor_x)), 0, (sd(coor_x)))),
                sd_y = as.integer(ifelse(is.na(sd(coor_y)), 0, (sd(coor_y))))),
                by = list(tds_id, datasetId)]
        message("aggregated ", nrow(dtm), " rows for emd")
        
        write.csv(dtm, file="data/mean_emd.csv", row.names=FALSE)
        
        dtm
}

read.mean.emd <- function() {
        if (!file.exists("data/mean_emd.csv")) {
                compute.mean.emd()
        }
        dtm <- data.table(read.csv(file="data/mean_emd.csv", stringsAsFactors=FALSE))
        dtm
}

# Step 2
# get the found spatial data and save it
# and...
get.spatials <- function() {
        query <- paste("SELECT parent_datasetId as datasetId, source, coor_x, coor_y,",
                       "method, xy_exchanged",
                       "FROM coar_n.tspatials",
                       "WHERE method > 0;",
                       sep="\n")
        dt <- as.data.table(execute.select(query))
        message("got ", nrow(dt), " rows for spatials.")
        write.csv(dt, file="data/spatials.csv", row.names=FALSE)
        dt
}

read.spatials <- function() {
        if (!file.exists("data/spatials.csv")) {
                get.spatials()
        }
        dts <- data.table(read.csv(file="data/spatials.csv", stringsAsFactors=FALSE))
        dts
}

# Step 2 (continued)
# and...
# compute the distance of x and y from found points to x and y of known central point from metadata
# also compute the real distance between these points
compute.distance <- function() {
        dtm <- read.mean.emd()
        setkey(dtm, datasetId)
        dts <- read.spatials()
        # merge tabellen
        dt <- merge(dtm, dts)
        # bereken de afstand van x en y met de mean x en y, houd rekening met sd
        # formule:      dx =  abs(x - mx) - sdx
        #               als dx < 0, 0, anders dx
        dt[, d_x:=ifelse(abs(coor_x - mean_x) - sd_x < 0, 0, abs(coor_x - mean_x) - sd_x)]
        dt[, d_y:=ifelse(abs(coor_y - mean_y) - sd_y < 0, 0, abs(coor_y - mean_y) - sd_y)]
        # breng de twee afstanden terug tot een afstand tot het mean punt
        # de afstand van twee punten in een x/y coordinatenstelsel is gelijk aan
        # de vierkantswortel van de som van het kwadraat van het verschil tussen de twee x/y dinges
        dt[, distance:=as.integer(sqrt((d_x)^2 + (d_y)^2))]
        
        dt
}

puntenwolk <- function() {
        # 
        dt <- compute.distance()
        rows1 <- nrow(dt)
        # beperk de maximale afstand
        afs <- 10000
        dt <- dt[d_x <= afs, ]
        dt <- dt[d_y <= afs, ]
        rows2 <- nrow(dt)
        perc <- as.integer((rows2/rows1) * 100)
        
        p <- ggplot(dt, aes(d_x, d_y))
        p + geom_point(aes(colour = factor(method))) +
                ggtitle(
                        paste("afstand tussen gevonden punt en centraal punt uit metadata voor x en y",
                              paste("voor", rows2, "van de", rows1, "gevonden punten (", perc, "%)"),
                              sep="\n")) +
                scale_x_continuous("afstand x in meters") +
                scale_y_continuous("afstand y in meters") +
                theme_bw(base_size = 20)
        
}

plok.puntenwolk <- function() {
        cairo_pdf("images/puntenwolk.pdf", bg="transparent", width = 17, height = 14)
        op <- par(bg="transparent")
        puntenwolk()
        dev.off()
        par(op)
}

