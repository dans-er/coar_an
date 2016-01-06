## Stuff to interact with a database.

require(RMySQL)

## some system acra dabra needed for correct query results.
Sys.setlocale("LC_ALL","C") 

## set properties according to situation at hand
.db.host <- "127.0.0.1"
.db.port <- 1505
.db.name <- "coar_n"
.db.user <- "root"
.db.pass <- ""

## create a connection. it is responsibility of calling method to close the connection.
create.connection <- function() {
        dbConnect(MySQL(), user=.db.user, host=.db.host, port = .db.port, dbname = .db.name)
}

## execute the given query and return the results.
execute.select <- function(query) {
        con <- create.connection()
        rs <- dbSendQuery(con, query)
        dt <- dbFetch(rs, n = -1)
        dbClearResult(rs)
        dbDisconnect(con)
        return(dt)
}

fileToDB <- function(tablename, filename) {
        con <- create.connection()
        dbWriteTable(con, name=tablename, value=filename)
        dbDisconnect(con)
}

## write a data.frame to the database.
tableTodb <- function(df, tablename, field.types=NULL, row.names=FALSE, append=FALSE) {
        con <- create.connection() 
        dbWriteTable(con, name=tablename, value=df, 
                     field.types=field.types,
                     row.names=row.names,
                     append=append)
        dbDisconnect(con)
}