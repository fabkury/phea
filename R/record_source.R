# Phea ------------------------------------------------------------------------------------------------------------
# "Phenotyping Algebra"
# By Fabrício Kury, 2022
# fab at kury.dev.
#


# Random name -----------------------------------------------------------------------------------------------------
random_name <- function(len) {
  # Generate random rec_name, 6 characters long, case-insensitive, starting with a letter.
  sample(letters, 1) |>
    c(sample(c(letters, 0:9), len-1, replace = TRUE)) |>
    as.list() |>
    do.call(what = paste0)
}


# Make record source ----------------------------------------------------------------------------------------------
#' Make record source
#'
#' Create a Phea record source.
#'
#' Creates a record source from a lazy table.
#'
#' @export
#' 
#' @param records Lazy table with records to be used.
#' 
#' @param pid Character. Name of the colum in `records` that gives the person (patient) identifier.
#' @param ts Character. Name of the colum in `records` that gives the timestamp.
#' @param .pid Unquoted characters. Optional. Use this argument to pass unquoted characters to the `pid` argument. If
#' `pid` is provided, `.pid` is ignored. See examples.
#' @param .ts Unquoted characters. Optional. Use this argument to pass unquoted characters to the `ts` argument. If `ts`
#'  is provided, `.ts` is ignored. See examples.
#'  
#' @param rec_name Character. Optional. Record name.
#' @param vars Character vector. Optional. Name of the colums to make available from `records`. If not supplied, all
#' columns are used.
#' 
#' @seealso [make_component()] to create a component from a record source.
#' 
#' @return Phea record source object.
#' 
#' @examples
#' # This:
#' diabetes_mellitus <- sqlt(condition_occurrence) |>
#'   filter(condition_concept_id == 201820) |>
#'   make_component(
#'     .pid = person_id,
#'     .ts = condition_start_datetime,
#'     delay = "'6 months'::interval")
#'     
#' # Is the same as:
#' diabetes_mellitus <- sqlt(condition_occurrence) |>
#'   filter(condition_concept_id == 201820) |>
#'   make_component(
#'     pid = 'person_id',
#'     ts = 'condition_start_datetime',
#'     delay = "'6 months'::interval")
#'     
#' # Which is also the same as:
#' diabetes_mellitus <- sqlt(condition_occurrence) |>
#'   filter(condition_concept_id == 201820) |>
#'   make_record_source(
#'     pid = 'person_id',
#'     ts = 'condition_start_datetime') |>
#'   make_component(
#'     delay = "'6 months'::interval")
make_record_source <- function(records, pid = NULL, ts = NULL, vars = NULL, .pid = NULL, .ts = NULL, rec_name = NULL) {
  rec_source <- list()

  rec_source$records <- records
  
  if(is.null(rec_name))
    rec_name <- random_name(6)

  rec_source$rec_name <- rec_name

  if(is.null(pid) || is.na(pid))
    pid <- deparse(substitute(.pid))
  rec_source$pid <- pid

  if(is.null(ts) || is.na(ts))
    ts <- deparse(substitute(.ts))
  rec_source$ts <- ts
  
  if(is.null(vars))
    vars <- setdiff(colnames(records), pid) # Keep all columns but `pid`
  rec_source$vars <- vars
  
  attr(rec_source, 'phea') <- 'record_source'

  rec_source
}


