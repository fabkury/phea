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
  line = NULL, bound = NULL,
  delay = NULL, window = NULL,
  ahead = NULL, up_to = NULL,
  # rows = NULL, range = NULL,
  pid = NULL, ts = NULL,
  .ts = NULL, .pid = NULL,
  .fn = NULL, .ts_fn = NULL,
  .passthrough = FALSE
  ) {
# Prepare ---------------------------------------------------------------------------------------------------------
  # This is just to make code easier to read.
  dbQuoteId <- function(x)
    DBI::dbQuoteIdentifier(.pheaglobalenv$con, x)
  
  if(is.null(pid) || is.na(pid)) {
    pid = deparse(substitute(.pid))
    if(pid == 'NULL')
      pid <- NULL
  }
  
  if(is.null(ts) || is.na(ts)) {
    ts = deparse(substitute(.ts))
    if(ts == 'NULL')
      ts <- NULL
  }
  
# Generate component, capture record source -----------------------------------------------------------------------
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
      if(is.null(pid) || is.null(ts))
        stop('If providing a lazy table or phenotype to make_component(), must also provide pid or .pid and ts or .ts.')
      
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

#     component$line <- line
#     component$bound <- bound
#     component$delay <- delay
#     component$window <- window
#     component$ahead <- ahead
#     component$up_to <- up_to
#     component$fn <- .fn
#     component$ts_fn <- .ts_fn
#     component$.passthrough <- .passthrough
  }
  
# Overwrite with input parameters if they were provided -----------------------------------------------------------
  # TODO: Not add these parameters to component since they're just needed to build access_sql.
  if(!is.null(line))
    component$line <- line
  
  if(!is.null(bound))
    component$bound <- bound
  
  if(!is.null(delay))
    component$delay <- delay
  
  if(!is.null(window))
    component$window <- window

  if(!is.null(.passthrough))
    component$.passthrough <- .passthrough

  if(!is.null(ahead))
    component$ahead <- ahead
  
  if(!is.null(up_to))
    component$up_to <- up_to

  if(!is.null(.fn))
    component$fn <- .fn
  
  if(!is.null(.ts_fn))
    component$ts_fn <- .ts_fn
  
  # if(!is.null(rows) && !is.null(range))
  #   stop(paste0('rows and range cannot be used simultaneously.'))
  
  # if((!is.null(component$delay) || !is.null(component$window)) && !is.null(component$line))
  #   stop(paste0('line and delay/window cannot be used simultaneously.'))
  
# Build window function SQL ---------------------------------------------------------------------------------------
  component$columns <- component$rec_source$vars
  
  # Add timestamp column
  if(!component$.passthrough)
    component$columns <- c(component$columns, 'ts')
  
  # ts_fn defaults to last_value
  if(is.null(component$ts_fn) || is.na(component$ts_fn))
    component$ts_fn <- 'last_value' 
  
  # .passthrough defaults to FALSE
  if(is.null(component$.passthrough) || is.na(component$.passthrough))
    component$.passthrough <- FALSE
  
  columns_sql <- paste0('case when ', dbQuoteId('name'), ' = ',
    DBI::dbQuoteString(.pheaglobalenv$con, component$rec_source$rec_name),
    ' then ', dbQuoteId(component$columns), ' else null end')
  
  over_clause <- paste0('partition by ', dbQuoteId('pid'), ', ', dbQuoteId('name'), ' order by ', dbQuoteId('ts'))
  
  # component_has_been_built is just to help us trim the code identation, as opposed to using nested if-else`s.
  component_has_been_built <- FALSE
  
  # TODO:
  # if(!is.null(rows)) {
  #   component$access <- 'rows'
  #   component_has_been_built <- TRUE
  # }
  
  # TODO:
  # if(!component_has_been_built && !is.null(range)) {
  #   component$access <- 'range'
  #   component_has_been_built <- TRUE
  # }
  
  # In the absence of any parameter line/bound/delay/window/ahead/up_to, we default to line = 0.
  if(( is.null(component$line)  ||  is.na(component$line)) &&
      (is.null(component$bound) ||  is.na(component$bound)) &&
      (is.null(component$delay) ||  is.na(component$delay)) && 
      (is.null(component$window) || is.na(component$window)) &&
      (is.null(component$ahead) ||  is.na(component$ahead)) &&
      (is.null(component$up_to) ||  is.na(component$up_to))) {
    component$line <- 0
  }
  
  # Let's prepare some variables to make code easier to read, and also to contemplate the functionality where the user
  # can erase a parameter by setting it to NA (while NULL means "don't change this").
  # We do need to copy the variables from inside `component` because `component` may have come from a phenotype or
  # another component, i.e. because of parameter overload.
  line <-   component$line
  bound <-  component$bound
  delay <-  component$delay
  window <- component$window
  ahead <-  component$ahead
  up_to <-  component$up_to
  if(isTRUE(is.na(line)))   line <- NULL
  if(isTRUE(is.na(bound)))  bound <- NULL
  if(isTRUE(is.na(delay)))  delay <- NULL
  if(isTRUE(is.na(window))) window <- NULL
  if(isTRUE(is.na(ahead)))  ahead <- NULL
  if(isTRUE(is.na(up_to)))  up_to <- NULL
  
  # At this point, if any of the parameters line/bound/delay/window/ahead/up_to were NA, they are now NULL.
  # Notice that we *don't* change the parameter itself to NULL inside `component`, but just the local variables here.
  
  if(!component_has_been_built && (!is.null(line) || !is.null(bound))) {
    # Produce access via *line*.
    component$access <- 'line'
    
    if(is.null(component$fn) || is.na(component$fn))
      component$fn <- 'last_value' # default to last_value
    
    use_fn <- rep(component$fn, length(component$columns))
    use_fn[component$columns == 'ts'] <- component$ts_fn
    
    if(is.null(bound) && !is.null(line) && line == 0) {
      # Here is one optimization.
      # This is the "most default" case: the user just wants the most recent record of the component, without line/
      # bound/delay/window/ahead/up_to. In this case, we don't need a window function. We can just copy the column
      # whenever the line comes from the correct record source. In other words, the SQL to access the value is merely
      # the CASE WHEN ... statement that otherwise goes inside the window function call.
      component$access_sql <- sql(columns_sql)
    } else {
      component$access_sql <- lapply(seq(component$columns), \(i) {
        sql_txt <- paste0(use_fn[i], '(', columns_sql[i], ')')
        dbplyr::win_over(
          expr = sql(sql_txt),
          partition = c('pid', 'name'),
          order = 'ts',
          frame = c(
            ifelse(is.null(bound), -Inf, -bound),
            ifelse(is.null(line), 0, -line)),
          con = .pheaglobalenv$con)
      })
      # Unlist the SQL objects without accidentally converting to character.
      component$access_sql <- do.call(c, component$access_sql)
    }
    
    component_has_been_built <- TRUE
  }
  
  if(!component_has_been_built) {
    if(!is.null(delay) || !is.null(window)) {
      # Produce access via *delay/window*.
      component$access <- 'delay'
      
      if(is.null(component$fn) || is.na(component$fn))
        component$fn <- 'last_value' # default to last_value
      
      use_fn <- rep(component$fn, length(component$columns))
      use_fn[component$columns == 'ts'] <- component$ts_fn
      
      sql_start <- paste0(use_fn, '(', columns_sql, ') over (', over_clause, ' ')
      
      if(is.null(up_to)) {
        sql_txts <- paste0(sql_start, 'range between ',
          ifelse(is.null(window) || window == Inf, 'unbounded',   window), ' preceding',
          ' and ',
          ifelse(is.null(delay)  || delay == 0   , 'current row', paste0(delay, ' preceding')),
          ')')
      } else {
        # In this branch, window is allowed to be == 0 (current row), because we have up_to.
        sql_txts <- paste0(sql_start, 'range between ',
          ifelse(is.null(window) || window == Inf, 'unbounded preceding',
            ifelse(window == 0, 'current row', paste0(window, ' preceding'))),
            ' and ',
          ifelse(up_to == Inf                    , 'unbounded',
            paste0(up_to, ' following')),
          ')')
      }
    } else {
      # Produce access via *ahead/up_to*.
      component$access <- 'ahead'
      
      if(is.null(ahead) && is.null(up_to))
        stop('Unable to identify the component\'s type of access. All parameters are empty.')
      
      if(is.null(component$fn) || is.na(component$fn))
        component$fn <- 'first_value' # default to first_value
      
      use_fn <- rep(component$fn, length(component$columns))
      use_fn[component$columns == 'ts'] <- 'last_value' # default to last_value
      
      sql_start <- paste0(use_fn, '(', columns_sql, ') over (', over_clause, ' ')
      sql_txts <- paste0(sql_start, 'range between ',
        ifelse(is.na(ahead),                 'current row', paste0(ahead, ' following')),
        ' and ',
        ifelse(is.na(up_to) || up_to == Inf, 'unbounded',   up_to), ' following)')
    }
    
    # Produce SQL objects from character
    component$access_sql <- sql(sql_txts)
    component_has_been_built <- TRUE
  }
  
# Finalize and return ---------------------------------------------------------------------------------------------
  if(!component_has_been_built)
    warning('component_has_been_built is FALSE.')
  
  attr(component, 'phea') <- 'component'
  component
}


