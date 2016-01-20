require(data.table)
source("rdb/mysql.R")
source("geo.R")
source("compute.R")

path.spread.1 <- "data/spread_1.csv" # ruw.
path.spread.2 <- "data/spread_2.csv" # spreiding berekend, gefilterd op methode.
path.spread.3 <- "data/spread_3.csv" # geo names voor kritieke records.
path.spread.4 <- "data/spread_4.csv" # bereken waarde gewicht records.
path.aggregate <- "data/aggregate.csv" # aggregatie van verschillende waarden op niveau dataset

select.spread.raw <- function() {
        query <- paste("SELECT datasetId, emd_title, lat, lon, coor_x, coor_y, method",
                       "FROM tdatasets",
                       "JOIN tspatials ON parent_datasetId = datasetId",
                       "WHERE source != 'emd' AND co_emd = 0;",
                       sep = "\n")
        dt <- as.data.table(execute.select(query))
        message("got ", nrow(dt), " rows for pdf-points.")
        write.csv(dt, path.spread.1, row.names=FALSE)
        dt
}

get.spread.data <- function() {
        if (file.exists(path.spread.1)) {
                dt <- as.data.table(read.csv(path.spread.1, stringsAsFactor=FALSE))
        } else {
                dt <- select.spread.raw()
        }
        dt
}

compute.spread <- function() {
        dt <- get.spread.data()
        message("initial amount of records: ", nrow(dt))
        # aggregate on standard deviation per dataset        
        dtm <- dt[, j = list(
                n = .N,
                mc1 = length(unique(method)),
                sdx1 = as.integer(ifelse(is.na(sd(coor_x)), 0, (sd(coor_x)))),
                sdy1 = as.integer(ifelse(is.na(sd(coor_y)), 0, (sd(coor_y))))
        ),
        by = list(datasetId)]
        message("aggregated ", nrow(dtm), " rows for standard deviation")
        dtm[, dist1:=as.integer(sqrt((sdx1)^2 + (sdy1)^2))]
        # merge the aggregated sd's with original table
        dts <- merge(dt, dtm, by=c("datasetId"), all.x=TRUE)
        message("    median: ", median(dts$dist1), " mean: ", mean(dts$dist1))
        
        ## eerste ronde - method 3
        dts[, method2:=ifelse(dist1 > 10000 & mc1 > 1 & method == 3, NA, method)]
        dts[, x2:=ifelse(dist1 > 10000 & mc1 > 1 & method == 3, NA, coor_x)]
        dts[, y2:=ifelse(dist1 > 10000 & mc1 > 1 & method == 3, NA, coor_y)]        
        # aggregate on standard deviation per dataset  
        dtm <- dts[, j = list(
                mc2 = length(unique(method2[!is.na(method2)])),
                sdx2 = as.integer(ifelse(is.na(sd(x2, na.rm=TRUE)), 0, (sd(x2, na.rm=TRUE)))),
                sdy2 = as.integer(ifelse(is.na(sd(y2, na.rm=TRUE)), 0, (sd(y2, na.rm=TRUE))))
        ),
        by = list(datasetId)]
        dtm[, dist2:=as.integer(sqrt((sdx2)^2 + (sdy2)^2))]
        dts <- merge(dts, dtm, by=c("datasetId"), all.x=TRUE)
        message("-m3 median: ", median(dts$dist2), " mean: ", mean(dts$dist2))
        
        ## tweede ronde - method 2
        dts[, method3:=ifelse(dist2 > 10000 & mc2 > 1 & method == 2, NA, method)]
        dts[, x3:=ifelse(dist2 > 10000 & mc2 > 1 & method == 2, NA, x2)]
        dts[, y3:=ifelse(dist2 > 10000 & mc2 > 1 & method == 2, NA, y2)]        
        # aggregate on standard deviation per dataset  
        dtm <- dts[, j = list(
                mc3 = length(unique(method3[!is.na(method3)])),
                sdx3 = as.integer(ifelse(is.na(sd(x3, na.rm=TRUE)), 0, (sd(x3, na.rm=TRUE)))),
                sdy3 = as.integer(ifelse(is.na(sd(y3, na.rm=TRUE)), 0, (sd(y3, na.rm=TRUE))))
        ),
        by = list(datasetId)]
        dtm[, dist3:=as.integer(sqrt((sdx3)^2 + (sdy3)^2))]
        dts <- merge(dts, dtm, by=c("datasetId"), all.x=TRUE)
        message("-m2 median: ", median(dts$dist3), " mean: ", mean(dts$dist3))
        
        write.csv(dts, path.spread.2, row.names=FALSE)
        dts
}

# 
geo.evidence <- function(username, start=1) {
        dt <- as.data.table(read.csv(path.spread.3, stringsAsFactor=FALSE))
     
        #dt[, name:=""]
        #dt[, adminName2:=""]
        tel <- start
        req <- 1
        for (x in start:nrow(dt)) { 
              
              done = TRUE
              if (is.null(dt$name[x]) | is.null(dt$admineName2[x])) {
                      done = FALSE
              } else if (dt$name[x]=="" & dt$admineName2[x]=="") {
                      done = FALSE
              }
              
                      
              if (!done) { # condition interposed after first run with only
                                                        # check on dist[3]
                      
                 if (dt$n[x] < 9 | dt$dist1[x] > 10000 | dt$dist2[x] > 10000 | dt$dist3[x] > 10000) {
                       try({  
                              geo <- get.geonames(username, dt$emd_title[x], dt$lat[x], dt$lon[x]) 
                              dt$name[x] <- geo[1]
                              dt$adminName2[x] <- geo[2]
                       })
                      #Sys.sleep(1)
                      req <- req + 1
                 }
              }
              
              
              tel <- tel + 1
              if (tel %% 100 == 0) {
                     write.csv(dt, path.spread.3, row.names=FALSE)  
                     print(tel)
                     message("requests: ", req)
                     Sys.sleep(10)
              }
              if (req > 1500) {
                      message("requests: ", req)
                      Sys.sleep(65 * 60)
                      req <- 1
              }
        }
        write.csv(dt, path.spread.3, row.names=FALSE) 
        dt
}

get.weight <- function(coor_x, coor_y, method, n, dist1, dist2, dist3, mc1, mc2, name, adminName2, c_names, c_admin) {
        w <- 1
        if (c_names + c_admin == 0) {   # no geo names for records from this dataset
               if (method == 3 & mc1 > 1 & dist2 < dist1) {
                       w <- 0
               } else if (method == 2 & mc2 > 1 & dist3 < dist2) {
                       w <- 0
               }
        } else { # we have geo names for the dataset
               if (name == "" & adminName2 == "") { # we have no evidence 
                       if (method == 3 & mc1 > 1 & dist2 < dist1) {
                               w <- 0
                       } else if (method == 2 & mc2 > 1 & dist3 < dist2) {
                               w <- 0
                       }
                       
               }
               
               if (name != "") { # name evidence
                       w <- w + 5
               } 
               if (adminName2 != "") { # adminName evidence
                       w <- w + 4
               }
        }
        w
}

compute.weight <- function() {
        dt <- as.data.table(read.csv(path.spread.3, stringsAsFactor=FALSE))
        # count records with name/adminName2 found with geo.evidence
        dtm <- dt[, j = list(
                c_names = sum(name != ""),
                c_admin = sum(adminName2 != "")
        ),
        by = list(datasetId)]
        # merge the count columns
        dt <- merge(dt, dtm, by=c("datasetId"), all.x=TRUE)
        
                #dt <- dt[578:581, ]
        # compute the weight of each record
        #dt[, weigth:=get.weight(coor_x, coor_y, method, n, dist1, dist2, dist3, mc1, mc2, name, adminName2, c_names, c_admin)]
        dt[, weight:=mapply(get.weight, coor_x, coor_y, method, n, dist1, dist2, dist3, mc1, mc2, name, adminName2, c_names, c_admin)]
        dt$mc1 <- NULL
        dt$sdx1 <- NULL
        dt$sdy1 <- NULL
        
        dt$method2 <- NULL
        dt$x2 <- NULL
        dt$y2 <- NULL
        dt$mc2 <- NULL
        dt$sdx2 <- NULL
        dt$sdy2 <- NULL
        
        dt$method3 <- NULL
        dt$x3 <- NULL
        dt$y3 <- NULL
        dt$mc3 <- NULL
        dt$sdx3 <- NULL
        dt$sdy3 <- NULL
        
        dt <- dt[!weight==0, ]
        
        write.csv(dt, path.spread.4, row.names=FALSE) 
        dt
}



aggregate.on.datasets <- function() {
        dt <- as.data.table(read.csv(path.spread.4, stringsAsFactor=FALSE))
        
        # get the standard deviation for series and weighted series per dataset
        # 
        dts <- dt[, j = list(
                sdx = as.integer(ifelse(is.na(sd(coor_x)), 0, (sd(coor_x)))),
                sdy = as.integer(ifelse(is.na(sd(coor_y)), 0, (sd(coor_y)))),
                sdwx = as.integer(ifelse(is.na(sd(rep(coor_x, times=weight))), 0, (sd(rep(coor_x, times=weight))))),
                sdwy = as.integer(ifelse(is.na(sd(rep(coor_y, times=weight))), 0, (sd(rep(coor_y, times=weight)))))
        ),
        by = list(datasetId)]
        # and the distance for these standard deviations
        dts[, dist:=as.integer(sqrt((sdx)^2 + (sdy)^2))]
        dts[, distw:=as.integer(sqrt((sdwx)^2 + (sdwy)^2))]
        dt <- merge(dt, dts, by=c("datasetId"), all.x=TRUE)
        
        # aggregate
        dta <- dt[, j = list(
                emd_title = emd_title[1],
                n_pdf = n[1],
                n_used = .N,
                sdx = sdx[1],
                sdy = sdy[1],
                sdwx = sdwx[1],
                sdwy = sdwy[1],
                dist1 = dist1[1],
                dist = dist[1],
                distw = distw[1],
                minx = min(coor_x),
                maxx = max(coor_x),
                miny = min(coor_y),
                maxy = max(coor_y),
                opp = ((max(coor_x) - min(coor_x))/1000) * ((max(coor_y) - min(coor_y))/1000),
                waarde = sum(weight),
                medianx = as.integer(median(rep(coor_x, times=weight))),
                mediany = as.integer(median(rep(coor_y, times=weight))),
                plaats = paste0(unique(unlist(strsplit(name, ","))), collapse=","),
                gemeente = paste0(unique(unlist(strsplit(adminName2, ","))), collapse=",")
        ),
        by = list(datasetId)]
        dta[, lat:=RDtoWGS84(medianx, mediany)$lat]
        dta[, lon:=RDtoWGS84(medianx, mediany)$lon]
        write.csv(dta, path.aggregate, row.names=FALSE) 
        dta
}

