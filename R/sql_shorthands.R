# Phea ------------------------------------------------------------------------------------------------------------
# "Phenotyping Algebra"
# By Fabrício Kury, 2022
# fab at kury.dev.
#

# Head shot & code shot -------------------------------------------------------------------------------------------
#' Head shot
#'
#' Peek at the first rows of `lazy_tbl`.
#'
#' Collects the first `nrows` of `lazy_tbl` into a tibble, calls `View()`, then returns the tibble.
#'
#' @export
#' @param lazy_tbl Lazy table to look at.
#' @param nrow Number of rows to collect.
#' @param blind If true, will not call `View()`, but just return the result.
#' @param .title If provided, this is the title of `View()`. If not, `deparse(substitute(lazy_tbl))` will be used.
#' @return Collected tibble containing the first `nrows` of `lazy_tbl`.
head_shot <- function(lazy_tbl, nrows = 10, blind = FALSE, .title = NA) {
  if(!blind && is.na(.title))
    varname <- deparse(substitute(lazy_tbl))

  res <- lazy_tbl |>
    head(n = nrows) |>
    collect()

  if(!blind)
    View(res, title = ifelse(is.na(.title), paste0(varname, ' ', nrows), .title))

  res
}

#' Code shot
#'
#' See the SQL code behind a `lazy_tbl`.
#'
#' Obtain the SQL code rendered from `lazy_tbl`, write to the clipboard, and return it. Optionally, don't write to
#' clipboard.
#'
#' @export
#' @param lazy_tbl Lazy table to look at.
#' @param clip If `TRUE` (default), will write to clipboard.
#' @return SQL object containing the query. Can be coerced to character by `as.character()`.
code_shot <- function(lazy_tbl, clip = TRUE) {
  # 2022-11-20 12:28
  res <- lazy_tbl |>
    dbplyr::sql_render()
  
  if(clip)
    writeClipboard(res)
  
  return(res)
}


# sql0, sqlt & sqla -----------------------------------------------------------------------------------------------
#' SQL table
#'
#' Produces a lazy table object of `table` in the preconfigured default schema, or in the specified `schema`.
#'
#' This function is a shorthand for `select * from def_schema.table;`. `def_schema` is the schema passed to
#' `setup_phea()` previously.
#'
#' @export
#' @param table *Unquoted* name of the table to be accessed within `def_schema`.
#' @param .table Character. Name of the table to be accessed within `def_schema`. If `.table` is provided, `table` is
#' ignored.
#' @param schema Optional. Character. Name of schema to use. If provided, overrides `def_schema`.  
#' @return Lazy table equal to `select * from def_schema.table;`.
#' @seealso [sql0()] to run arbitrary SQL queries on the server. [sqla()] to run arbitrary SQL queries using lazy
#' tables.
#' @examples
#' `sqlt(person)`
#' `sqlt(.table = 'person')`
#' 
#' `sqlt(condition_occurrence)`
#' `sqlt(.table = 'condition_occurrence')`
sqlt <- function(table, schema = NULL, .table = NULL) {
  if(!exists('con', envir = .pheaglobalenv))
    stop('SQL connection not found. Please call setup_phea() before sqlt().')
  
  if(is.null(schema))
    schema <- .pheaglobalenv$schema
  
  if(!is.null(.table))
    table_name <- .table
  else
    table_name <- deparse(substitute(table))
  
  if(is.na(schema))
    res <- dplyr::tbl(.pheaglobalenv$con, table_name)
  else
    res <- dplyr::tbl(.pheaglobalenv$con, dbplyr::in_schema(schema, table_name))
  
  res
}

#' SQL query
#'
#' Combines with `paste0` the strings in `...`, then runs it as a SQL query.
#'
#' @export
#' @param ... Character strings to be concatenated with `paste0`.
#' @seealso [sqlt()] to create lazy tables from SQL tables. [sqla()] to run arbitrary SQL with lazy tables.
#' @return Lazy table corresponding to the query.
sql0 <- function(...) {
  sql_txt <- paste0(...)
  
  # Remove the ending ';' if the user wrote it. tbl() can't take it.
  sql_txt <- sql_txt |>
    trimws() |>
    stringr::str_replace(';$', '')
  
  dplyr::tbl(.pheaglobalenv$con,
    dplyr::sql(sql_txt))
}

#' SQL query with arguments
#'
#' Combines with `paste0` the strings in `...`, then runs it as a SQL query using `args` as common table expressions.
#' The names of the `args` objects are the names made available in the query.
#'
#' @export
#' @param args 
#' @param ... Character strings to be concatenated with `paste0`.
#' @seealso [sqlt()] to create lazy tables from SQL tables. [sql0()] to run arbitrary SQL.
#' @return Lazy table corresponding to the query.
#' @examples
#' ```
#' list(a = sqlt(person), b = sqlt(procedure)) |>
#'   sqla('select person_id fr', 'om a inner jo', 'in b on a.person_id = b.person_id')
#' ```
sqla <- function(args, ...) {
  # Produces a dbplyr tbl object from arbitrary SQL.
  # Usage example:
  # sqla(list(a = sqlt(person)), 'select person_id from a')
  query <- paste0(...)
  
  if(stringr::str_count(query, ";") > 1)
    stop('Only a single SQL query is allowed. The ending ";" is optional.')
  
  # For some reason, we can't have the query end with ;, otherwise we get "Error: Failed to prepare query: ERROR: 
  # syntax error at or near ";"". So, remove ending ';', if it exists.
  query_str_len <- nchar(query)
  if(substr(query, query_str_len, query_str_len) == ';')
    query <- substr(query, 1, query_str_len-1)
  
  # Convert the arguments into common table expressions (WITH clauses).
  with_clauses <- purrr::map2(names(args), args, \(arg_name, arg) {
    paste0(arg_name, ' as (', dbplyr::sql_render(arg), ') ')
  }) |>
    paste0(collapse = ', ')
  
  query_with_args <- paste0('with ', with_clauses, query)
  
  return(
    dplyr::sql(query_with_args) |>
      dplyr::tbl(src = .pheaglobalenv$con))
}


