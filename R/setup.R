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
#' `IGNORE NULLS` by the OBDC driver connected to a Spark SQL server/cluster. This is the only situation where this
#' argument should be used.
setup_phea <- function(connection, schema, verbose = TRUE, .fix_dbplyr_spark = FALSE) {
  assign('con', connection, envir = .pheaglobalenv)
  assign('schema', schema, envir = .pheaglobalenv)
  assign('verbose', verbose, envir = .pheaglobalenv)
  
  postgres_exists <- function() {
    db_engine <- dbplyr::db_connection_describe(.pheaglobalenv$con)
    return(grepl('postgres', db_engine, ignore.case = TRUE))
  }
  
  sql_function_exists <- function(name) {
    function_check <- DBI::dbGetQuery(.pheaglobalenv$con,
      paste0('select * from
        pg_proc p
        join pg_namespace n
        on p.pronamespace = n.oid
        where proname =\'', name, '\';')) |>
      nrow()
    return(function_check == 1)
  }
  
  if(postgres_exists()) {
    
    if(!sql_function_exists('phea_coalesce_r_sfunc')) {
      if(verbose)
        message('PostgreSQL detected in "', dbplyr::db_connection_describe(.pheaglobalenv$con), '".')
      
      if(verbose)
        message('Installing phea_coalesce_r_sfunc.')
      
      DBI::dbExecute(.pheaglobalenv$con,
        "create function phea_coalesce_r_sfunc(state anyelement, value anyelement)
        returns anyelement
        immutable parallel safe
        as
        $$
          select coalesce(value, state);
        $$ language sql;")
    }
    
    if(!sql_function_exists('phea_last_value_ignore_nulls')) {
      if(verbose)
        message('Installing phea_last_value_ignore_nulls.')
      
      DBI::dbExecute(.pheaglobalenv$con,
        "create aggregate phea_last_value_ignore_nulls(anyelement) (
          sfunc = phea_coalesce_r_sfunc,
          stype = anyelement
        );")
    }
    
    assign('custom_aggregate', 'phea_last_value_ignore_nulls', envir = .pheaglobalenv)
  }
  
  if(.fix_dbplyr_spark) {
    if(connection@info$dbms.name == "Spark SQL") {
      # Fix dbplyr's last_value() implementation.
      `last_value_sql.Spark SQL` <<- function(con, x) {
        dbplyr:::build_sql("LAST_VALUE(", ident(as.character(x)), ", true)", con = con)
      }
    }
  }
}

