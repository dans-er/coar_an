
require(data.table)
source("rdb/mysql.R")

RDtoWGS84 <- function(x, y) {
        # converts points x, y to WGS84 lat, lon
        if (is.null(x) || is.na(x) || length(x) < 1) {
                return(NA)
        }
        p <- (x - 155000.00)/100000.00
        q <- (y - 463000.00)/100000.00
        df <- ((q*3235.65389)+(p^2*-32.58297)+(q^2*-0.24750)+(p^2*q*-0.84978)+(q^3*-0.06550)+(p^2*q^2*-0.01709)
               +(p*-0.00738)+(p^4*0.00530)+(p^2*q^3*-0.00039)+(p^4*q*0.00033)+(p*q*-0.00012))/3600.00
        dl <- ((p*5260.52916)+(p*q*105.94684)+(p*q^2*2.45656)+(p^3*-0.81885)+(p*q^3*0.05594)+(p^3*q*-0.05607)
               +(q*0.01199)+(p^3*q^2*-0.00256)+(p*q^4*0.00128)+(q^2*0.00022)+(p^2*-0.00022)+(p^5*0.00026))/3600.00
        lat <- round(52.15517440+df, 6)
        lon <- round(5.387206210+dl, 6)
        
        list("lat"=lat, "lon"=lon)
}

computeMedianDistance <-function(id, x, y) {
        p <- unlist(dtmco[rds_id==id, c(4,5), with=F])
        as.integer(sqrt((x - p[1])^2 + (y - p[2])^2))
}

emdValues <- function() {
        # compute the median RD point for each dataset;
        # compute the mean RD point for each dataset;
        # add lat/lon coordinates
        query <- paste("SELECT tds_id, datasetId, coor_x, coor_y",
                       "FROM tdatasets",
                       "JOIN tspatials ON parent_datasetId = datasetId",
                       "WHERE source = 'emd';",
                       sep = "\n")
        dt <- as.data.table(execute.select(query))
        message("got ", nrow(dt), " rows for emd.")
        
        # aggregate on median of x and y per datasetemd
        dtm <- dt[, j = list(
                n_emd = .N, 
                median_emd_x = as.integer(median(coor_x)), 
                median_emd_y = as.integer(median(coor_y))), 
                by = list(tds_id, datasetId)]
        message("aggregated ", nrow(dtm), " rows for emd")
        
        # compute latitude, longitude for median points
        dtm[, c("median_emd_lat", "median_emd_lon") := RDtoWGS84(median_emd_x, median_emd_y), with = FALSE]
        dtm[, median_emd_lat:=sprintf("%.6f", median_emd_lat)]
        dtm[, median_emd_lon:=sprintf("%.6f", median_emd_lon)]
        
        setnames(dtm, old="tds_id", new="rds_id")
        setkey(dtm, rds_id, datasetId)
        
        setnames(dt, old="tds_id", new="rds_id")
        setkey(dt, rds_id, datasetId)
        
        # compute the distance to emd median center point per emd point in dt
        dtmco <<- dtm
        dt[,median_dist_emd:=mapply(computeMedianDistance, id=rds_id, x=coor_x, y=coor_y, USE.NAMES=FALSE)]
        
        # compute the median and 3th quantile of the median distances
        dtq2 <- dt[, j = list(median_dist_emd_q2=as.integer(quantile(median_dist_emd)[[3]])), by = list(rds_id, datasetId)]
        dtq3 <- dt[, j = list(median_dist_emd_q3=as.integer(quantile(median_dist_emd)[[4]])), by = list(rds_id, datasetId)]
        
        dtm <- merge(dtm, dtq2, all=TRUE)
        dtm <- merge(dtm, dtq3, all=TRUE)
        message("computed median emd distance for ", nrow(dtm), " rows")
        
        dtm
}

pdfValues <- function() {
        # compute the median RD point for each dataset; add lat/lon coordinates
        query <- paste("SELECT tds_id, datasetId, coor_x, coor_y",
                       "FROM tdatasets",
                       "JOIN tspatials ON parent_datasetId = datasetId",
                       "WHERE source != 'emd';",
                       sep = "\n")
        dt <- as.data.table(execute.select(query))
        message("got ", nrow(dt), " rows for pdf.")
        # aggregate on median of x and y per dataset
        dtm <- dt[, j = list(
                n_pdf = .N, 
                median_pdf_x = as.integer(median(coor_x)), 
                median_pdf_y = as.integer(median(coor_y))), 
                by = list(tds_id, datasetId)]
        message("aggregated ", nrow(dtm), " rows for pdf")
        
        # compute latitude, longitude for median points
        dtm[, c("median_pdf_lat", "median_pdf_lon") := RDtoWGS84(median_pdf_x, median_pdf_y), with = FALSE]
        dtm[, median_pdf_lat:=sprintf("%.6f", median_pdf_lat)]
        dtm[, median_pdf_lon:=sprintf("%.6f", median_pdf_lon)]
        
        setnames(dtm, old="tds_id", new="rds_id")
        setkey(dtm, rds_id, datasetId)
        
        setnames(dt, old="tds_id", new="rds_id")
        setkey(dt, rds_id, datasetId)
        
        # compute the distance to pdf center point per pdf point in dt
        dtmco <<- dtm
        dt[,median_dist_pdf:=mapply(computeMedianDistance, id=rds_id, x=coor_x, y=coor_y, USE.NAMES=FALSE)]
        
        
        # compute the median and 3th quantile of the distances
        dtq2 <- dt[, j = list(median_dist_pdf_q2=as.integer(quantile(median_dist_pdf)[[3]])), by = list(rds_id, datasetId)]
        dtq3 <- dt[, j = list(median_dist_pdf_q3=as.integer(quantile(median_dist_pdf)[[4]])), by = list(rds_id, datasetId)]
        
        dtm <- merge(dtm, dtq2, all=TRUE)
        dtm <- merge(dtm, dtq3, all=TRUE)
        message("computed median pdf distance for ", nrow(dtm), " rows")
        
        dtm
}

getNullEmdPdf <- function() {
        query <- paste("SELECT tds_id, datasetId",
                       "FROM coar_n.tdatasets",
                       "where pdf_files = 0 AND co_emd = 0;",
                       sep = "\n")
        dt <- as.data.table(execute.select(query))
        message("got ", nrow(dt), " rows for neither emd nor pdf.")
        setnames(dt, old="tds_id", new="rds_id")
        setkey(dt, rds_id, datasetId)
        dt
}

computeAll <- function() {
        dt.emd <- emdValues()
        dt.pdf <- pdfValues()
        dt.none <- getNullEmdPdf()
        
        dt <- merge(dt.emd, dt.pdf, all=TRUE)
        dt <- merge(dt, dt.none, all=TRUE)
        message("merged ", nrow(dt), " rows for emd and pdf")        
        message(nrow(dt), " should be equal to the number of records in tdatasets.")
        
        dt[, n_emd:=ifelse(is.na(n_emd), 0, n_emd)]
        dt[, n_pdf:=ifelse(is.na(n_pdf), 0, n_pdf)]
        
        # compute the distance between median emd en median pdf
        dt[, median_distance:=as.integer(sqrt((median_emd_x - median_pdf_x)^2 + (median_emd_y - median_pdf_y)^2))]
        
        message("Sending to database (rows, columns): ", nrow(dt), ", ", ncol(dt))
        tableTodb(dt, "rdatasets", row.names=FALSE, append=TRUE)
}
