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
#' Configures Phea, %in% particular the SQL shorthands `sqlt()`, `sql0()` and `sqla()`.
#'
#' @export
#' @param connection DBI-compatible SQL connection (e.g. produced by DBI::dbConnect).
#' @param schema Schema to be used by default %in% `sqlt()`. If no schema, use `NA`.
#' @param verbose Logical. If TRUE (default), functions will print to console at times.
#' @param engine Character. What is the flavor of your SQL server. If not provided, `setup_phea()` will try to detect it
#' from `dbplyr::db_connection_describe()`. Options are: `postgres`, `mysql`, `redshift`, `spark`, `oracle`,
#' `bigquery`, `sqlserver`. Names are case-insensitive but must otherwise match exactly. If `engine` is not provided and
#' it can't be detected, an error is raised.
#' @param compatibility_mode Logical. If `TRUE` (default is `FALSE`), all component features besides `window` are
#' deactivated, and all components become strictly _"most recently available record_" on all columns. Turning this
#' feature on may help make Phea work on SQL flavors it where it wasn't tested.
setup_phea <- function(connection, schema, verbose = TRUE, engine = NULL, compatibility_mode = FALSE,
  custom_aggregate = NULL) {
  assign('con', connection, envir = .pheaglobalenv)
  assign('schema', schema, envir = .pheaglobalenv)
  assign('verbose', verbose, envir = .pheaglobalenv)
  
  if(is.null(engine)) {
    db_desc <- dbplyr::db_connection_describe(.pheaglobalenv$con)
    
    if(is.null(engine) && grepl('postgres', db_desc, ignore.case = TRUE))
      engine <- 'postgres'
    
    if(is.null(engine) && grepl('mysql', db_desc, ignore.case = TRUE))
      engine <- 'mysql'
    
    if(is.null(engine) && grepl('redshift', db_desc, ignore.case = TRUE))
      engine <- 'redshift'
    
    if(is.null(engine) && (grepl('spark', db_desc, ignore.case = TRUE)
      || isTRUE(try(connection@info$dbms.name == "Spark SQL"))))
      engine <- 'spark'
    
    if(is.null(engine) && grepl('bigquery', db_desc, ignore.case = TRUE))
      engine <- 'bigquery'
    
    if(is.null(engine))
      stop(paste0("Unable to detect SQL engine. Please provide `engine` argument. Options are: postgres, mysql, ",
        "redshift, spark,  oracle, bigquery, sqlserver. If your engine is not on the list, you can try another one ", 
        "with similar SQL syntax."))
  } else {
    # Normalize to lower case
    engine <- tolower(engine)
  }
  
  assign('engine', engine, envir = .pheaglobalenv)
  
  engine_code <- NULL
  
  if(is.null(engine_code) && engine %in% c('mysql')) {
    engine_code <- 0
    compatibility_mode <- TRUE
  }
  
  if(is.null(engine_code) && engine %in% c('postgres'))
    engine_code <- 1
  
  if(is.null(engine_code) && engine %in% c('redshift', 'oracle', 'bigquery'))
    engine_code <- 2
  
  if(is.null(engine_code) && engine %in% c('spark'))
    engine_code <- 3
  
  if(is.null(engine_code) && engine %in% c('sqlserver'))
    engine_code <- 4
  
  assign('engine_code', engine_code, envir = .pheaglobalenv)
  
  if(engine == 'postgres' && !compatibility_mode) {
    sql_function_exists <- function(name) {
      function_check <- DBI::dbGetQuery(.pheaglobalenv$con,
        paste0('select * from
          pg_proc p
          inner join pg_namespace n
          on p.pronamespace = n.oid
          where proname =\'', name, '\';')) |>
        nrow()
      return(function_check == 1)
    }
    
    install_sql_function <- function(name, code) {
      if(verbose)
        message('Installing ', name, '.')
      DBI::dbExecute(.pheaglobalenv$con, code)
    }
    
    already_installed <- c('phea_coalesce_r_sfunc', 'phea_coalesce_nr_sfunc',
      'phea_last_value_ignore_nulls', 'phea_first_value_ignore_nulls') |>
      sapply(sql_function_exists, USE.NAMES = TRUE)
    
    need_to_install <- !already_installed
    
    if(any(need_to_install) && verbose)
      message('Engine configured to PostgreSQL.')
    
    tryCatch({
      if(need_to_install[['phea_coalesce_r_sfunc']]) {
        install_sql_function('phea_coalesce_r_sfunc',
          "create or replace function phea_coalesce_r_sfunc(state anyelement, value anyelement)
          returns anyelement
          immutable parallel safe
          as
          $$
            select coalesce(value, state);
          $$ language sql;")
      }
      
      if(need_to_install[['phea_coalesce_nr_sfunc']]) {
        install_sql_function('phea_coalesce_nr_sfunc',
          "create or replace function phea_coalesce_nr_sfunc(state anyelement, value anyelement)
          returns anyelement
          immutable parallel safe
          as
          $$
            select coalesce(state, value);
          $$ language sql;")
      }
      
      if(need_to_install[['phea_last_value_ignore_nulls']]) {
        install_sql_function('phea_last_value_ignore_nulls',
          "create or replace aggregate phea_last_value_ignore_nulls(anyelement) (
            sfunc = phea_coalesce_r_sfunc,
            stype = anyelement
          );")
      }
      
      if(need_to_install[['phea_first_value_ignore_nulls']]) {
        install_sql_function('phea_first_value_ignore_nulls',
          "create or replace aggregate phea_first_value_ignore_nulls(anyelement) (
            sfunc = phea_coalesce_nr_sfunc,
            stype = anyelement
          );")
      }
      
      custom_aggregate <- list(
        last_value = 'phea_last_value_ignore_nulls',
        first_value = 'phea_first_value_ignore_nulls')
    },
      error = \(e) {
        warning('Unable to install custom aggregates. `last_value` and `first_value` will require compatibility mode.')
        compatibility_mode <<- TRUE
    })
  }
    
  if(engine == 'spark') {
    # Insert _nulls treatment_ into dbplyr's last_value() and first_value() implementation.
    `last_value_sql.Spark SQL` <<- function(con, x) {
      dbplyr:::build_sql("last_value(", ident(as.character(x)), ", true)", con = con)
    }
    
    `first_value_sql.Spark SQL` <<- function(con, x) {
      dbplyr:::build_sql("first_value(", ident(as.character(x)), ", true)", con = con)
    }
  }
  
  assign('compatibility_mode', compatibility_mode, envir = .pheaglobalenv)
  
  if(!is.null(custom_aggregate))
    assign('custom_aggregate', custom_aggregate, envir = .pheaglobalenv)
  
  assign('warns', list(), envir = .pheaglobalenv)
}

