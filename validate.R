require(data.table)
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



