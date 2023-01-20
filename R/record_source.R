# Phea ------------------------------------------------------------------------------------------------------------
# "Phenotyping Algebra"
# By Fabrício Kury, 2022
# fab at kury.dev.
#


# Random name -----------------------------------------------------------------------------------------------------
# random_rec_name <- function(len) {
# }
random_rec_name <- function(len) {
  # random rec_name, integer, 0 to `len` digits long
  return(as.integer(runif(1) * 10^len))
  
  # random rec_name, `len` characters long, case-insensitive, starting with a letter
  # c(sample(letters, 1), sample(c(letters, 0:9), len-1, replace = TRUE)) |>
  #   as.list() |> do.call(what = paste0)
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
#' @param .pid,.ts Unquoted characters. Use these argument to pass unquoted characters to the `pid` or `ts` arguments.
#' If `pid`/`ts` is provided, `.pid`/`.ts` is ignored. See examples.
#'  
#' @param rec_name Integer. Optional. Number to use as record name. If not provided, a random one will be generated.
#' 
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

  # records
  rec_source$records <- records
  
  # pid
  if(is.null(pid) || is.na(pid))
    pid <- deparse(substitute(.pid))
  source_colums <- colnames(records)
  if(! pid %in% source_colums)
    stop('pid \'', pid, '\' not found in input data.')
  rec_source$pid <- pid

  # ts
  if(is.null(ts) || is.na(ts))
    ts <- deparse(substitute(.ts))
  if(! ts %in% source_colums)
    stop('ts \'', ts, '\' not found in input data.')
  rec_source$ts <- ts
  
  # rec_name
  if(is.null(rec_name)) {
    rec_name <- random_rec_name(6)
  } else {
    if(is.numeric(rec_name))
      rec_name <- as.integer(rec_name)
    
    if(!is.integer(rec_name))
      stop('rec_name must be integer or numeric, or NULL.')
  }
  
  rec_source$rec_name <- rec_name
  
  # vars
  if(is.null(vars))
    vars <- setdiff(colnames(records), pid) # Keep all columns but `pid`
  
  rec_source$vars <- vars
  
  # finalize
  attr(rec_source, 'phea') <- 'record_source'

  rec_source
}


