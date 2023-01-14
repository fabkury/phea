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
  line = NULL, bound = NULL, delay = NULL, window = NULL, ahead = NULL, up_to = NULL,
  pid = NULL, .pid = NULL ,ts = NULL, .ts = NULL,
  fn = NULL, ts_fn = NULL, arg = NULL, ts_arg = NULL, omit_value = NULL, ts_omit_value = NULL,
  # pick_by = NULL,
  passthrough = FALSE
  ) {
# Prepare ---------------------------------------------------------------------------------------------------------
  dbQuoteId <- function(x) # This is just to make code easier to read.
    DBI::dbQuoteIdentifier(.pheaglobalenv$con, x)
  
  # pid and ts must be processed before capturing input_source.
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
  
# Capture input_source and create `component` according to argument overload --------------------------------------
  component <- list()
  
  if(isTRUE(attr(input_source, 'phea') == 'component')) {
    # Input is a component. Just copy it.
    component <- input_source
  } else {
    # Input is not a component, so we will build one.
    if(isTRUE(attr(input_source, 'phea') == 'record_source')) {
      # Input is a record source. Use it.
      component$rec_source <- input_source
    } else {
      if(isTRUE(attr(input_source, 'phea') == 'phenotype')) {
        # Input is a phenotype. Make a record source from it.
        # If the user didn't provide pid and ts, default to 'pid' and 'ts'.
        # TODO: Move the recognizing of this default to make_record_source()?
        if(is.null(pid))
          pid <- 'pid'
        if(is.null(ts))
          ts <- 'ts'
      } else {
        if('tbl_lazy' %in% class(input_source)) {
          # Input is a lazy table. Make a record source from it.
          if(is.null(pid) || is.null(ts))
            stop('If providing a lazy table to make_component(), must also provide `pid` or `.pid` and `ts` or `.ts`.')
        } else {
          stop('Unable to recognize input_source.')
        }
      }
      
      component$rec_source <- make_record_source(records = input_source, pid = pid, ts = ts)
    }
  }

  # TODO: Do I really need to copy component$rec_source$vars into component$columns?
  component$columns <- component$rec_source$vars
  
  ## Overwrite with input parameters if they were provided
  if(!is.null(line)) component$line <- line
  if(!is.null(bound)) component$bound <- bound
  if(!is.null(delay)) component$delay <- delay
  if(!is.null(window)) component$window <- window
  if(!is.null(ahead)) component$ahead <- ahead
  if(!is.null(up_to)) component$up_to <- up_to
  if(!is.null(fn)) component$fn <- fn
  if(!is.null(arg)) component$arg <- arg
  if(!is.null(ts_fn)) component$ts_fn <- ts_fn
  if(!is.null(ts_arg)) component$ts_arg <- ts_arg
  if(!is.null(omit_value)) component$omit_value <- omit_value
  if(!is.null(ts_omit_value)) component$ts_omit_value <- ts_omit_value
  # if(!is.null(pick_by)) component$pick_by <- pick_by
  if(!is.null(passthrough)) component$passthrough <- passthrough
  
  # passthrough defaults to FALSE
  if(is.null(component$passthrough) || isTRUE(is.na(component$passthrough)))
    component$passthrough <- FALSE
  
  # Add timestamp column
  if(!component$passthrough)
    component$columns <- c(component$columns, 'ts')
  
# Prepare variables -----------------------------------------------------------------------------------------------
  # Let's prepare some variables to make code easier to read, and also to contemplate the functionality where the user
  # can erase a parameter by setting it to NA (while NULL means "don't change this").
  # We do need to copy the variables from inside `component` because `component` may have come from a phenotype or
  # another component, i.e. because of parameter overload.
  line   <- component$line
  bound  <- component$bound
  delay  <- component$delay
  window <- component$window
  ahead  <- component$ahead
  up_to  <- component$up_to
  
  if(isTRUE(is.na(line)))   line <- NULL
  if(isTRUE(is.na(bound)))  bound <- NULL
  if(isTRUE(is.na(delay)))  delay <- NULL
  if(isTRUE(is.na(window))) window <- NULL
  if(isTRUE(is.na(ahead)))  ahead <- NULL
  if(isTRUE(is.na(up_to)))  up_to <- NULL
  # `fn` and `ts_fn` are different just because we do want the defaults to be stored inside `component`.
  
  if(is.null(component$fn) || isTRUE(is.na(component$fn)) || isTRUE(component$fn == ''))
    component$fn <- 'last_value'
  
  if(is.null(component$ts_fn) || isTRUE(is.na(component$ts_fn)) || isTRUE(component$ts_fn == ''))
    component$ts_fn <- 'last_value'
  
  if(is.null(component$arg) || isTRUE(is.na(component$arg)))
    component$arg <- ''
  
  if(is.null(component$ts_arg) || isTRUE(is.na(component$ts_arg)))
    component$ts_arg <- ''
  
  if(is.null(component$omit_value) || isTRUE(is.na(component$omit_value)))
    component$omit_value <- FALSE
  
  if(is.null(component$ts_omit_value) || isTRUE(is.na(component$ts_omit_value)))
    component$ts_omit_value <- FALSE

# Capture named args fn/ts, arg/ts, omit_value/ts -----------------------------------------------------------------
  capture_named_args <- function(args, def_arg, ts_arg, columns, passthrough, preprocess_fn = NULL) {
    name_not_empty <- function(x) {
      !is.na(names(x)) & nchar(names(x)) > 0 # Is any name not empty?
    }
    # Check if an unnamed fn was passed.
    if(is.null(names(args))) {
      default_arg <- args
    } else {
      names_mask <- name_not_empty(args)
      
      if(any(!names_mask)) # Is any name empty?
        default_arg <- args[min(which(!names_mask))] # Use first empty name.
      else
        default_arg <- def_arg
    }
    
    if(!is.null(preprocess_fn))
      default_arg <- preprocess_fn(default_arg)
    
    # Repeat args for each item in columns, with names.
    use_arg <- sapply(columns, \(x) default_arg, simplify = FALSE, USE.NAMES = TRUE)
    
    # Apply declared functions for columns, in any.
    names_mask <- name_not_empty(args)
    if(any(names_mask))
      for(cmp_name in names(args[names_mask])) {
        use_arg[names(use_arg) == cmp_name] <- ifelse(!is.null(preprocess_fn),
          preprocess_fn(args[names(args) == cmp_name]), args[names(args) == cmp_name])
      }
    
    if(!passthrough)
      use_arg[columns == 'ts'] <- ifelse(!is.null(preprocess_fn), preprocess_fn(ts_arg), ts_arg) # component$ts_fn
      
    use_arg
  }
  
  # use_fn is a vector of window functions, one for each column.
  use_fn <- capture_named_args(component$fn, 'last_value',
    component$ts_fn, component$columns, component$passthrough)
  
  # Use custom aggregates if needed
  if(exists('custom_aggregate', envir = .pheaglobalenv)) {
    for(i in 1:length(.pheaglobalenv$custom_aggregate)) {
      fn_name_out <- names(.pheaglobalenv$custom_aggregate)[i]
      fn_name_in <- .pheaglobalenv$custom_aggregate[[i]]
      mask <- grepl(fn_name_out, use_fn, ignore.case = TRUE)
      if(any(mask))
        use_fn[mask] <- fn_name_in
    }
  }
  
  # use_arg is a vector of arguments to window functions, one for each column.
  process_arg <- function(x) {
    if(is.null(x) || length(x) == 0 || isTRUE(is.na(x)) || isTRUE(x == ''))
      return('')
    
    classes <- lapply(x, class) |> unlist()
    
    mask <- classes == 'character'
    if(any(mask))
      x[mask] <- DBI::dbQuoteString(.pheaglobalenv$con, unlist(x[mask]))
    
    mask <- classes == 'Date'
    if(any(mask)) # TODO: Test these date manipulations.
      x[mask] <- DBI::dbQuoteString(.pheaglobalenv$con, sapply(x[mask], strftime, '%Y-%m-%d'))
    
    mask <- classes == 'numeric' | classes == 'integer'
    if(any(mask))
      x[mask] <- unlist(x[mask])

    return(paste0(x, collapse = ', '))
  }
  
  use_arg <- capture_named_args(component$arg, '',
    component$ts_arg, component$columns, component$passthrough,
    preprocess_fn = process_arg)
  
  # use_omit_value is a vector of Boolean, one for each column.
  use_omit_value <- capture_named_args(component$omit_value, FALSE,
    component$ts_omit_value, component$columns, component$passthrough)

# Default nulls treatment -----------------------------------------------------------------------------------------
  if(.pheaglobalenv$engine_code == 3) {
    # TODO: Test this on DataBricks.
    nulls_treatment_mask <- sapply(use_fn, grepl, pattern = 'last_value|first_value', ignore.case = TRUE)
    if(any(nulls_treatment_mask)) {
      use_arg[nulls_treatment_mask] <- ', TRUE'
      use_omit_value[nulls_treatment_mask] <- FALSE
    }
  }
  
# Default to line = 0 if needed -----------------------------------------------------------------------------------
  # At this point, if any of the parameters line/bound/delay/window/ahead/up_to were NA, they are now NULL (or their
  # appropriate default value). Notice that we didn't change the parameters themselves to NULL inside `component`,
  # but just the local variables here -- except for `fn` and `ts_fn`.
  
  # In the absence of any parameter line/bound/delay/window/ahead/up_to, we default to line = 0. Notice we also store
  # it in the `component`.
  if(is.null(line) && is.null(bound) && is.null(delay) && is.null(window) && is.null(ahead) && is.null(up_to)) {
    line <- 0
    component$line <- 0
  }
  
# Build window function SQL ---------------------------------------------------------------------------------------
  # columns_sql is vectorized by the presence of `component$columns` 
  columns_sql <- paste0('case when ', dbQuoteId('name'), ' = ', component$rec_source$rec_name,
    ' then ', dbQuoteId(component$columns), ' else null end')
  
  component$placement_sql <- columns_sql
  
  over_clause <- paste0(
    'partition by ', dbQuoteId('pid'),
    # ', ', dbQuoteId('name'),
    ' order by ', 
    dbQuoteId('ts'))
  
  # component_has_been_built is just to help us trim the code identation, as opposed to using nested if-else`s.
  component_has_been_built <- FALSE
  make_params_sql <- function() {
    purrr::pmap(list(columns_sql, use_arg, use_omit_value), function(x, y, z) {
      if(z) return(y)
      if(y == '') return(x)
      else return(paste0(x, ', ', y))
    })
  }
  
  # Now we figure out the acess mode, and produce the window function calls.
  if(isTRUE(is.na(line)))
    line <- NULL
  
  if(.pheaglobalenv$compatibility_mode) {
    # This is the "most default" case: the user just wants the most recent record of the component, without line/
    # bound/delay/window/ahead/up_to. In this case, we don't need a window function. We can just copy the column
    # whenever the line comes from the correct record source, then use tidyr::fill() to fill NULLs downward.
    # In other words, the SQL to access the value is merely the CASE WHEN ... statement that otherwise goes inside the
    # window function call.
    component$access <- 'line'
    
    component$access_sql <- dplyr::sql(columns_sql)
    component_has_been_built <- TRUE
  }
  
  if(!component_has_been_built && (!is.null(line) || !is.null(bound))) {
    # Produce access via *line*.
    # Line access is built differently, because we can use dbplyr::win_over(). dbplyr::win_over() does not support
    # window functions' RANGE mode, forcing us to not use it when mode is not ROWS.
    component$access <- 'line'
    
    params_sql <- make_params_sql()
    
    component$access_sql <- lapply(seq(component$columns), \(i) {
      sql_txt <- paste0(use_fn[i], '(', params_sql[i], ')')
      dbplyr::win_over(con = .pheaglobalenv$con,
        expr = dplyr::sql(sql_txt),
        partition = 'pid',
        order = 'ts',
        frame = c(
          ifelse(is.null(bound), -Inf, -bound),
          ifelse( is.null(line),    0, -line)))
    })
    
    # Unlist the SQL objects without accidentally converting to character, which happens when we use unlist().
    component$access_sql <- do.call(c, component$access_sql)
    component_has_been_built <- TRUE
  }
  
  if(!component_has_been_built) {
    # As commented above, for access other than *line* we need to write out the window function call by ourselves,
    # because dbplyr::win_over() does not support RANGE mode.
    params_sql <- make_params_sql()
    
    sql_start <- paste0(use_fn, '(', params_sql, ') over (', over_clause, ' range between ') # 1
    # switch(.pheaglobalenv$engine_code,
      # paste0(use_fn, '(', params_sql, ') over (', over_clause, ' range between ')) # 1
    
    if(!is.null(delay) || !is.null(window)) {
      # Produce access via *delay/window*.
      component$access <- 'delay'
      
      if(is.null(up_to)) {
        sql_txts <- paste0(sql_start,
          ifelse(is.null(window) || window == Inf, 'unbounded',                        window), ' preceding',
          ' and ',
          ifelse(    is.null(delay) || delay == 0, 'current row', paste0(delay, ' preceding')),
          ')')
      } else {
        # In this branch, window is allowed to be == 0 (current row), because we have up_to.
        sql_txts <- paste0(sql_start,
          ifelse(is.null(window) || window == Inf, 'unbounded preceding',
            ifelse(window == 0, 'current row', paste0(window, ' preceding'))),
            ' and ',
          ifelse(                    up_to == Inf, 'unbounded',
                                                paste0( up_to, ' following')),
          ')')
      }
    } else {
      # Produce access via *ahead/up_to*.
      component$access <- 'ahead'
      
      sql_txts <- paste0(sql_start,
        ifelse(                is.null(ahead), 'current row', paste0(ahead, ' following')),
        ' and ',
        ifelse(is.null(up_to) || up_to == Inf,   'unbounded',                       up_to),
        ' following)')
    }
    
    # Produce SQL objects from character
    component$access_sql <- dplyr::sql(sql_txts)
    component_has_been_built <- TRUE
  }
  
# Finalize and return ---------------------------------------------------------------------------------------------
  if(!component_has_been_built) # This is just a "sanity check." This if() should never be TRUE.
    warning('component_has_been_built is FALSE.')
  
  attr(component, 'phea') <- 'component'
  
  component
}


