# Phea ------------------------------------------------------------------------------------------------------------
# "Phenotyping Algebra"
# By Fabrício Kury, 2022
# fab at kury.dev.
#

# Make component --------------------------------------------------------------------------------------------------
#' Make component
#'
#' Produce a Phea component.
#'
#' Creates a component from the given `input_source` record source and optional parameters.
#' 
#' If `input_source` is a record source, it is used.
#' 
#' If `input_source` is a component, it is copied, and any other paremeter if provided overwrites the original one.
#' 
#' If `input_source` is a lazy table, a record source is generated from it, and used. In this case, arguments `pid`
#' and `ts` (or `.pid`, `.ts`) must also be provided.
#'
#' @export
#' @param input_source A record source from `make_record_source()`, a component from `make_component()`, or a lazy
#'   table. If the latter case, arguments `ts` and `pid` must also be provided.
#'   
#' @param line Integer. Which line to pick. 0 = skip no lines, 1 = skip one line, 2 = skip two lines, etc.
#' @param delay Character. Minimum time difference between phenotype date and component date. Time interval in SQL
#'   language. Include any necessary type casting according to the SQL flavor of the server. Examples in PostgreSQL:
#'   `'3 months'::interval`, `'20 seconds'::interval`, `'1.5 hours'::interval`.
#'   
#' @param window Character. Maximum time difference between phenotype date and the date of any component. Time interval
#'  in SQL language (see argument `delay`). 
#' 
#' @param ts Character. If passing a lazy table to `input_source`, `ts` is passed to `make_record_source()` to 
#' buid a record source. See \code{\link{make_record_source}}.
#' @param pid Character. If passing a lazy table to `input_source`, `pid` is passed to `make_record_source()`
#' to buid a record source. See \code{\link{make_record_source}}.
#'     
#' @param .ts Unquoted characters. Optional. Use this argument to pass unquoted characters to the `ts` argument (see
#' examples). If `ts` is provided, `.ts` is ignored.
#' @param .pid Unquoted characters. Optional. Use this argument to pass unquoted characters to the `pid` argument (see
#' examples). If `pid` is provided, `.pid` is ignored.
#'  
#' @seealso [make_record_source()] to create a record source.
#' @return Phea component object.
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
make_component <- function(input_source,
  line = NA, bound = NA,
  delay = NA, window = NA,
  ahead = NA, up_to = NA,
  rows = NULL, range = NULL,
  pid = NULL, ts = NULL,
  .ts = NULL, .pid = NULL,
  .fn = NA, .ts_fn = NA,
  .passthrough = FALSE) {
# Prepare ---------------------------------------------------------------------------------------------------------
  if(is.null(pid))
    pid = deparse(substitute(.pid))
  if(is.null(ts))
    ts = deparse(substitute(.ts))
  if(!is.null(pid) && pid == 'NULL')
    pid <- NULL
  if(!is.null(ts) && ts == 'NULL')
    ts <- NULL
  
# Generate component according to the logic of parameter overload -------------------------------------------------
  component <- list()
  if(isTRUE(attr(input_source, 'phea') == 'component')) {
    # Input is actually a component. Just copy it.
    component <- input_source
  } else {
    # Input is not a component, so we will build one.
    if(isTRUE(attr(input_source, 'phea') == 'record_source')) {
      # Input is a record source. Use it.
      component$rec_source <- input_source
    } else {
      if(is.null(ts) || is.null(pid) || ts == 'NULL' || pid == 'NULL')
        stop('If providing a lazy table or phenotype to make_component(), must also provide pid/.pid and ts/.ts.')
      
      if(isTRUE(attr(input_source, 'phea') == 'phenotype')) {
        # Input is a phenotype. Make a record source from it.
        component$rec_source <- make_record_source(records = input_source, pid = pid, ts = ts)
      } else {
        if('tbl_lazy' %in% class(input_source)) {
          # Input is a lazy table. Make a record source from it.
          component$rec_source <- make_record_source(records = input_source, pid = pid, ts = ts)
        } else {
          stop('Unable to recognize input_source.')
        }
      }
    }
      
    if(is.na(line) && is.na(delay) && is.na(window) && is.na(ahead) && is.na(up_to))
      line <- 0
    
    # Guarantee existance of the objects, even if it's NA.
    component$line <- line
    component$bound <- bound
    component$delay <- delay
    component$comp_window <- window
    component$ahead <- ahead
    component$up_to <- up_to
    component$fn <- .fn
    component$ts_fn <- .ts_fn
    component$.passthrough <- .passthrough
  }
  
# Overwrite with input parameters if they were provided -----------------------------------------------------------
  # TODO: Not add these parameters to component since they're just needed to build window_sql.
  if(!is.na(line))
    component$line <- line
  
  if(!is.na(bound))
    component$bound <- bound
  
  if(!is.na(delay) || !is.na(window)) {
    component$delay <- delay
    # If the user tries to apply a delay or a window, erase the line if not provided.
    if(is.na(line))
      component$line <- NA
  }
  
  if(!is.na(window))
    component$comp_window <- window

  if(!is.na(.passthrough))
    component$.passthrough <- .passthrough
  
  if(!is.na(ahead))
    component$ahead <- ahead
  
  if(!is.na(up_to))
    component$up_to <- up_to

  if(!is.na(.fn))
    component$fn <- .fn
  
  if(!is.na(.ts_fn))
    component$ts_fn <- .ts_fn
  
  if(!is.null(rows) && !is.null(range))
    stop(paste0('rows and range cannot be used simultaneously.'))
  
  if((!is.na(component$delay) || !is.na(component$comp_window)) && !is.na(component$line))
    stop(paste0('line and delay/window cannot be used simultaneously.'))
  
# Build window function SQL ---------------------------------------------------------------------------------------
  component$columns <- component$rec_source$vars
  
  # Add timestamp column
  if(!component$.passthrough)
    component$columns <- c(component$columns, 'ts')
  
  columns_sql <- paste0('case when ', DBI::dbQuoteIdentifier(.pheaglobalenv$con, 'name'), ' = ',
    DBI::dbQuoteString(.pheaglobalenv$con, component$rec_source$rec_name),
    ' then ', DBI::dbQuoteIdentifier(.pheaglobalenv$con, component$columns), ' else null end')
  
  over_clause <- paste0('partition by ', DBI::dbQuoteIdentifier(.pheaglobalenv$con, 'pid'), ', ',
    DBI::dbQuoteIdentifier(.pheaglobalenv$con, 'name'), ' order by ', DBI::dbQuoteIdentifier(.pheaglobalenv$con, 'ts'))
  
  if(is.na(component$ts_fn))
    component$ts_fn <- 'last_value'
  
  # component_has_been_built is just to help us trim the code identation, as opposed to using nested if-else`s.
  component_has_been_built <- FALSE
  
  if(!is.null(rows)) {
    # TODO
    component$access <- 'rows'
    component_has_been_built <- TRUE
  }
  
  if(!component_has_been_built && !is.null(range)) {
    # TODO
    component$access <- 'range'
    component_has_been_built <- TRUE
  }
  
  if(!component_has_been_built &&
      (!is.na(component$line) || !is.na(component$bound))) {
    # Produce access via *line*.
    component$access <- 'line'
    
    if(is.na(component$fn))
      component$fn <- 'last_value' # Line access defaults to last_value
    
    use_fn <- rep(component$fn, length(component$columns))
    use_fn[component$columns == 'ts'] <- component$ts_fn
    
    
    component$window_sql <- lapply(seq(component$columns), \(i) {
      sql_txt <- paste0(use_fn[i], '(', columns_sql[i], ')')
      dbplyr::win_over(
        expr = sql(sql_txt),
        partition = c('pid', 'name'),
        order = 'ts',
        frame = c(
          ifelse(is.na(component$bound), -Inf, -component$bound),
          ifelse(is.na(component$line), 0, -component$line)),
        con = .pheaglobalenv$con)
    })
    
    # Unlist the SQL objects without accidentally converting to character.
    component$window_sql <- do.call(c, component$window_sql)
    component_has_been_built <- TRUE
  }
  
  if(!component_has_been_built) {
    if(!is.na(component$delay) || !is.na(component$comp_window)) {
      # Produce access via *delay/window*.
      component$access <- 'delay'
      
      if(is.na(component$fn))
        component$fn <- 'last_value' # *delay/window* defaults to last_value
      
      use_fn <- rep(component$fn, length(component$columns))
      use_fn[component$columns == 'ts'] <- component$ts_fn
      
      sql_start <- paste0(use_fn, '(', columns_sql, ') over (', over_clause, ' ')
      
      if(is.na(component$up_to)) {
        sql_txts <- paste0(sql_start, 'range between ',
          ifelse(is.na(component$comp_window) || component$comp_window == Inf, 'unbounded', component$comp_window),
          ' preceding and ',
          ifelse(is.na(component$delay) || component$delay == 0, 'current row', paste0(component$delay, ' preceding')),
          ')')
      } else {
        # In this branch, comp_window is allowed to be == 0 (current row), because we have up_to.
        sql_txts <- paste0(sql_start, 'range between ',
          ifelse(is.na(component$comp_window) || component$comp_window == Inf, 'unbounded preceding',
            ifelse(component$comp_window == 0, 'current row', paste0(component$comp_window, ' preceding'))),
              ' and ', ifelse(component$up_to == Inf, 'unbounded', paste0(component$up_to, ' following')), ')')
      }
    } else {
      # Produce access via *ahead/up_to*.
      component$access <- 'ahead'
      
      if(is.na(component$ahead) && is.na(component$up_to))
        stop('Unable to identify the component\'s type of access. All parameters are empty.')
      
      if(is.na(component$fn))
        component$fn <- 'first_value' # *ahead/up_to* default to first_value
      
      use_fn <- rep(component$fn, length(component$columns))
      use_fn[component$columns == 'ts'] <- component$ts_fn
      
      sql_start <- paste0(use_fn, '(', columns_sql, ') over (', over_clause, ' ')
      sql_txts <- paste0(sql_start, 'range between ',
        ifelse(is.na(component$ahead), 'current row', paste0(component$ahead, ' following')),
        ' and ',
        ifelse(is.na(component$up_to) || component$up_to == Inf, 'unbounded', component$up_to), ' following)')
    }
    
    # Produce SQL objects from character
    component$window_sql <- sql(sql_txts)
    component_has_been_built <- TRUE
  }
  
# Finalize and return ---------------------------------------------------------------------------------------------
  if(!component_has_been_built)
    warning('component_has_been_built is FALSE.')
  
  attr(component, 'phea') <- 'component'
  component
}


