# Phea ------------------------------------------------------------------------------------------------------------
# "Phenotyping Algebra"
# By Fabrício Kury, 2022
# fab at kury.dev.
#


# Data ------------------------------------------------------------------------------------------------------------
if(!exists('.pheaglobalenv'))
  .pheaglobalenv <- new.env(parent=emptyenv())


# Setup Phea ------------------------------------------------------------------------------------------------------
#' Setup Phea
#'
#' Configures Phea, in particular the SQL shorthands `sqlt()`, `sql0()` and `sqla()`.
#'
#' @export
#' @param connection DBI-compatible SQL connection (e.g. produced by DBI::dbConnect).
#' @param schema Schema to be used by default in `sqlt()`. If no schema, use `NA`.
#' @param verbose Logical. Optional. If TRUE (default), functions will print to console at times.
#' @param .fix_dbplyr_spark Logical. Optional. Very niche functionality. Set to `TRUE` to attempt to fix the use of
#' `IGNORE NULLS` by the OBDC driver connected to a Spark SQL server/cluster.
setup_phea <- function(connection, schema, verbose = TRUE, .fix_dbplyr_spark = FALSE) {
  assign('con', connection, envir = .pheaglobalenv)
  assign('schema', schema, envir = .pheaglobalenv)
  assign('verbose', verbose, envir = .pheaglobalenv)
  
  if(.fix_dbplyr_spark) {
    if(connection@info$dbms.name == "Spark SQL") {
      # Fix dbplyr's last_value() implementation.
      `last_value_sql.Spark SQL` <<- function(con, x) {
        dbplyr:::build_sql("LAST_VALUE(", ident(as.character(x)), ", true)", con = con)
      }
    }
  }
}

