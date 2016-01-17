s
require(XML)
require(data.table)

# http://api.geonames.org/findNearbyPostalCodes?lat=51.901835&lng=5.970233&username=username
        

get.geonames <- function(username, s, lat, lon) {
        url <- paste0("http://api.geonames.org/findNearbyPostalCodes?lat=",
                      lat,
                      "&lng=",
                      lon,
                      "&username=",
                      username)
        doc <- xmlInternalTreeParse(url)
        geo <- as.data.table(xmlToDataFrame(doc))
        geo[, mname:=mapply(grepl, pattern=name, x=s, ignore.case=TRUE)]
        names <- paste(as.character(unique(geo[mname==TRUE,]$name)), collapse=",")
        
        # <adminName2>Gemeente Gilze en Rijen</adminName2>
        geo[, gemeente:=ifelse(adminName2=="", NA, gsub("Gemeente ", "", adminName2))]
        geo[, gname:=mapply(grepl, pattern=gemeente, x=s, ignore.case=TRUE)]
        gemeentes <- paste(as.character(unique(geo[gname==TRUE,]$adminName2)), collapse=",")
        
        c(name=names, adminName2=gemeentes)
}