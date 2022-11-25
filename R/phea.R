# Data ------------------------------------------------------------------------------------------------------------
# TODO: Remove the if()?
if(!exists('.pheaglobalenv'))
  .pheaglobalenv <- new.env(parent=emptyenv())


# Functions -------------------------------------------------------------------------------------------------------
#' Keep [first] row by [window function]
#'
#' Keeps the row containing the group-wise maximum or minimum.
#'
#' Divides lazy_tbl according to `partition`, and in each partition keeps only the row containing the maximum or minimum
#' of column `by`.
#'
#' @export
#' @param lazy_tbl Lazy table to be filtered.
#' @param by Column to filter rows by. Can be quoted or unquoted.
#' @param partition Character. Variable or vector of variables to define the partition.
#' @param win_fn "min" or "max". Which window function to use to filter rows.
#' @return Lazy table with filtered rows.
keep_row_by <- function(lazy_tbl, by, partition, win_fn = 'min') {
  if(!win_fn %in% c('min', 'max'))
    stop("win_fn must be 'min' or 'max'.")
  if(class(substitute(by)) == 'name')
    by_name <- deparse(substitute(by))
  else
    by_name <- by
  partition_txt <- paste0(partition, collapse = '", "')
  sql_txt <- paste0(win_fn, '("', by_name, '") over (partition by "', partition_txt, '")')
  res <- lazy_tbl |>
    dplyr::mutate(
      phea_calc_var = dplyr::sql(sql_txt)) |>
    dplyr::filter(!!rlang::sym(by_name) == phea_calc_var) |>
    dplyr::select(-phea_calc_var)
  res
}


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

# Setup Phea ------------------------------------------------------------------------------------------------------
#' Setup Phea
#'
#' Configures functions `sqlt()` and `sql0()` for use.
#'
#' Stores the DBI connection for later use.
#'
#' @export
#' @param connection DBI-compatible SQL connection (e.g. produced by DBI::dbConnect).
#' @param schema Schema to be used by default in `sqlt()`.
setup_phea <- function(connection, schema) {
  assign('con', connection, envir = .pheaglobalenv)
  assign('schema', schema, envir = .pheaglobalenv)
}


# sqlt & sql0 -----------------------------------------------------------------------------------------------------

#' SQL table
#'
#' Produces lazy table object from the name of a SQL table in the preconfigured schema.
#'
#' This function is a shorthand for `select * from def_schema.table;`.
#'
#' @export
#' @param dbi_connection DBI-compatible SQL connection (e.g. produced by DBI::dbConnect).
#' @param schema Schema to be used by default in `sqlt()`.
#' @return Lazy table equal to `select * from def_schema.table;`.
sqlt <- function(table) {
  dplyr::tbl(.pheaglobalenv$con,
    dbplyr::in_schema(.pheaglobalenv$schema,
      deparse(substitute(table))))
}

#' SQL query
#'
#' Combines with `paste0` the strings in `...`, then runs it as a SQL query.
#'
#' @export
#' @param ... Arguments to be coerced to `character` and concatenated.
#' @return Lazy table corresponding to the query.
sql0 <- function(...) {
  sql_txt <- paste0(...)
  dplyr::tbl(.pheaglobalenv$con,
    dplyr::sql(sql_txt))
}


# Make component --------------------------------------------------------------------------------------------------
#' Make component
#'
#' Produce a Phea component.
#'
#' Creates a component from the given `input_source` record source and optional parameters. If `input_source` is a
#' record source, it is used. If it is a component, it is copied (including its record source) and other paremeters, if
#' provided, overwrite existing ones.
#'
#' @export
#' @param input_source A record source from `make_record_source()`, a component from `make_component()`, or a lazy
#' table. If the latter case, `.ts` and `.pid` must be provided.
#' @param line Interger. Which line to pick. 0 = skip no lines, 1 = skip one line, 2 = skip two lines, etc.
#' @param delay Character. Time interval in SQL language. Minimum time difference between phenotype date and component
#' date.
#' @param window Character. Time interval in SQL language. Maximum time difference between phenotype date and component
#' date.
#' @param rec_name Character. If provided, overwrites the `rec_name` of the record source.
#' @param .ts Unquoted character. If passing a lazy table to `input_source`, `.ts` is used as `ts` to buid a record
#' source.
#' @param .pid Unquoted character. If passing a lazy table to `input_source`, `.pid` is used as `pid` to buid a record
#' source.
#' @return Phea component object.
make_component <- function(input_source, line = NA, delay = NA, window = Inf, rec_name = NA,
  .passthrough = FALSE, .ts = NULL, .pid = NULL) {
  component <- list()

  if(isTRUE(attr(input_source, 'phea') == 'component')) {
    # rec_source is actually a component.
    component <- input_source
    
    # Overwrite if provided.
    if(!is.na(line))
      component$line <- line
    
    if(!is.na(delay)) {
      component$delay <- delay
      # If the user tries to apply a delay, erase the line if not provided.
      if(is.na(line))
        component$line <- NA
    }
  } else {
    if(isTRUE(attr(input_source, 'phea') == 'record_source')) {
      component$rec_source <- input_source
    } else {
      if(isTRUE(attr(input_source, 'phea') == 'phenotype')) {
        # result from formula. Read its value column.
        component$rec_source <- make_record_source(
          records = input_source,
          rec_name = 'value',
          ts = ts,
          pid = pid)
      } else {
        if('tbl_lazy' %in% class(input_source)) {
          component$rec_source <- make_record_source(
            records = input_source,
            .ts = deparse(substitute(.ts)),
            .pid = deparse(substitute(.pid)))
        }
      }
    }
      
    if(is.na(line) && is.na(delay))
      line <- 0
    
    component$line <- line
    component$delay <- delay
    component$comp_window <- window
    component$.passthrough <- .passthrough
  }
  
  if(!is.na(window))
    component$comp_window <- window

  # if(!is.na(rec_name))
  #   component$rec_source$rec_name <- name

  if(!is.na(.passthrough))
    component$.passthrough <- .passthrough

  attr(component, 'phea') <- 'component'

  component
}


# Make record source ----------------------------------------------------------------------------------------------
#' Make record source
#'
#' Create a Phea record source.
#'
#' Creates a record source from a lazy table.
#'
#' @export
#' @param records Lazy table with records to use.
#' @param rec_name Character. Record name.
#' @param ts Unquoted string. Name of the colum in `records` that gives the timestamp.
#' @param pid Unquoted string. Name of the colum in `records` that gives the person (patient) identifier.
#' @param vars Character vector. Name of the colums to make available from `records`. If not supplied, all columns are
#' used.
#' @param .capture_col Unquoted string. Not yet implemented.
#' @return Phea record source object.
make_record_source <- function(records, rec_name = NULL, ts, pid, vars = NULL, .capture_col = NULL, .type = 'direct',
  .ts = NULL, .pid = NULL) {
  rec_source <- list()

  rec_source$records <- records
  
  if(is.null(rec_name)) {
    # Generate random rec_name, 12 characters long, case-insensitive, starting with a letter.
    rec_name <- c(sample(letters, 1), sample(c(letters, 0:9), 12-1)) |>
      as.list() |> do.call(what = paste0)
  }

  if(.type == 'column') {
    stop('Column-type record sources are not supported. rec_name must be a character string.')
    rec_name_name <- deparse(substitute(rec_name))
    rec_source$rec_name <- rec_name_name
    rec_source$type <- 'column'
    if(class(substitute(rec_name)) == 'name')
      rec_source$capture_col <- deparse(substitute(.capture_col))
    else
      rec_source$capture_col <- rec_name_name

    if(is.null(vars)) {
      vars <- records |>
        dplyr::select(!!rlang::sym(rec_name_name)) |> dplyr::distinct() |> dplyr::collect() |> dplyr::pull()
    }
  }
    
  if(.type == 'direct') {
    rec_source$rec_name <- rec_name
    rec_source$type <- 'direct'

    if(is.null(vars))
      vars <- colnames(records)
  }
  
  if(is.null(.ts))
    ts_name <- deparse(substitute(ts))
  else
    ts_name <- .ts
  rec_source$ts <- ts_name

  if(is.null(.pid))
    pid_name <- deparse(substitute(pid))
  else
    pid_name <- .pid
  vars <- setdiff(vars, pid_name)

  rec_source$vars <- vars
  rec_source$rec_pid <- pid_name
  attr(rec_source, 'phea') <- 'record_source'

  rec_source
}


# Calculate formula -----------------------------------------------------------------------------------------------
#' Calculate formula
#'
#' Gathers records according to timestamps and computes a SQL formula.
#'
#' This receives a list of components, and a formula in SQL language, and computes the result by gathering records
#' according to their timestamps.
#'
#' @export
#' @param components List of components, component, record source, or lazy table.
#' @param fml Formula or list of formulas.
#' @param export List of additional variables to export.
#' @param add_components Additional components. Used mostly in case components is not a list of components.
#' @param .ts,.pid,.rec_name,.delay,.line If supplied, these will overwrite those of the given component.
#' @param .require_all If `TRUE`, returns only rows where all components to have been found according to their
#' timestamps (even if their value is NA). If `.dont_require` is provided, `.require_all` is ignored.
#' @param .lim Maximum number of rows to return. This is imposed before the calculation of the formula.
#' @param .dont_require If provided, causes formula to require all components (regardless of .require_all), except for
#' those listed here.
#' @param .cascaded If `TRUE` (default), each formula is computed in a separate, nested SELECT statement. This allows
#' the result of the prior formula to be used in the following, at the potential cost of longer computation times.
#' @param .clip_sql If `TRUE`, instead of lazy table it returns the SQL query as a SQL object (can be converted to
#' character using `as.character()`), and also copies it to the clipboard.
#' @return Lazy table with result of formula or formulas.
calculate_formula <- function(components, fml = NULL, window = NA, export = NULL, add_components = NULL,
  .ts = NULL, .pid = NULL, .rec_name = NULL, .delay = NULL, .line = NULL,
  .require_all = FALSE, .lim = NA, .dont_require = NULL,
  .cascaded = TRUE, .clip_sql = FALSE) {
# Prepare ---------------------------------------------------------------------------------------------------------
  # TODO: Improve the logic regarding these two variables below.
  keep_names_unchanged <- FALSE
  input_is_phenotype <- FALSE
  # 
  
  if(isTRUE(attr(components, 'phea') == 'phenotype')) {
    keep_names_unchanged <- TRUE
    input_is_phenotype <- TRUE
    res_vars <- attr(components, 'phea_res_vars')
    
    new_component <- make_component(
      input_source = components,
      .ts = ts,
      .pid = pid)

    if(!is.null(.rec_name))
      new_component$rec_source$rec_name <- .rec_name

    ts_name <- deparse(substitute(.ts))
    if(ts_name != 'NULL')
      new_component$rec_source$ts <- ts_name

    pid_name <- deparse(substitute(.pid))
    if(pid_name != 'NULL')
      new_component$rec_source$ts <- pid_name

    if(!is.null(.line))
      new_component$line <- .line

    if(!is.null(.delay))
      new_component$delay <- .delay
    
    # Create one component per result var.
    components <- sapply(res_vars, \(x) new_component, USE.NAMES = TRUE, simplify = FALSE)
  }

  if(!is.null(add_components))
    components <- c(components, add_components)

  if(isTRUE(attr(components, 'phea') == 'component'))
    components <- list(components)

  # Build record sources, a deduplicated list of all record sources in the components.
  rec_source_names <- purrr::map(components, ~.$rec_source$rec_name) |> unlist()
  rec_source_mask <- !duplicated(rec_source_names)
  record_sources <- purrr::map(components[rec_source_mask], ~.$rec_source)

  if(is.null(names(components)) &&
      isTRUE(attr(components[[1]], 'phea') == 'component'))
    names(components) <- components[[1]]$rec_source$rec_name
  
  # Build variable map, with all valid combinations of components, record sources, and record source columns.
  var_map <- purrr::map2(names(components), components, \(component_name, component) {
    if(component$.passthrough || keep_names_unchanged) {
      res <- dplyr::tibble(
        component_name = component_name,
        rec_name = component$rec_source$rec_name,
        column = component$rec_source$vars,
        composed_name = component$rec_source$vars)
    } else {
      res <- dplyr::tibble(
        component_name = component_name,
        rec_name = component$rec_source$rec_name,
        column = component$rec_source$vars,
        composed_name = paste0(component_name, '_', component$rec_source$vars))
    }
    return(res)
  }) |>
    dplyr::bind_rows()
  
  # Read input formula or formulas.
  if(!is.null(fml)) {
    # Make sure the formulas have names.
    number_and_return <- function(fmll, prefix, p = 0) {
      for(i in seq(fmll)) {
        if(class(fmll[[i]]) == 'list') {
          retval <- number_and_return(fmll[[i]], prefix, p)
          p <- retval$p
          fmll[[i]] <- retval$fmll
        } else {
          if(is.null(names(fmll)[i]) || nchar(names(fmll)[i]) == 0) {
            p <- p + 1
            names(fmll)[i] <- paste0(prefix,
              ifelse(p == 1, '', p))
          }
        }
      }
      return(list(fmll = fmll, p = p))
    }
    fml <- fml |>
      number_and_return('value') |>
      purrr::pluck('fmll')
    
    # Extract components from formula.
    g_vars <- unlist(fml) |>
      stringr::str_match_all('([A-z][A-z0-9_]+)') |>
      unlist() |> unique()

    # Filter bogus matches (eg. SQL keywords in the formula) by keeping only the composed_names that can possibly come from
    # the given combination of record sources and components.
    g_vars <- g_vars[g_vars %in% var_map$composed_name]
  } else {
    # If no formula, no composed_names to export from it.
    g_vars <- NULL
  }

  # Add variables requested to export.
  g_vars <- c(g_vars, export)
  
  # Filter the variable map to contain only what we'll need.
  var_map <- dplyr::filter(var_map, composed_name %in% g_vars) |>
    dplyr::distinct()

# Prepare record sources ------------------------------------------------------------------------------------------
  prepare_record_source <- function(record_source) {
    rec_name <- record_source$rec_name

    # Normalize the column names.
    if(record_source$type == 'column') {
      stop('Column-type record source is not yet implemented.')
      capture_col <- record_source$capture_col
      sql_txt <- paste0("concat('", rec_name, "_', \"", rec_name, '")')
      export_records <- dplyr::transmute(record_source$records,
        name = dplyr::sql(sql_txt),
        pid = !!rlang::sym(record_source$rec_pid),
        ts = !!rlang::sym(record_source$ts),
        capture_col = !!rlang::sym(capture_col))
    } else {
      # Select only the columns that will be needed later, according to g_vars. This requires checking all applicable
      # components.
      out_vars <- var_map |>
        dplyr::filter(rec_name == .env$rec_name) |>
        dplyr::pull(column) |>
        unique()
      
      export_records <- record_source$records |>
        dplyr::transmute(
          name = local(rec_name),
          pid = !!rlang::sym(record_source$rec_pid),
          ts = !!rlang::sym(record_source$ts),
          !!!rlang::syms(out_vars))
    }

    return(export_records)
  }

  board <- record_sources |>
    purrr::map(prepare_record_source) |>
    purrr::reduce(dplyr::union_all)

# Produce components ----------------------------------------------------------------------------------------------
  produce_component <- function(comp_name, component) {
    columns <- var_map |>
      dplyr::filter(component_name == comp_name) |>
      dplyr::pull(column) |>
      unique()

    if(!is.na(component$delay) && !is.na(component$line)) {
      stop(paste0('Component ', comp_name, ': line and delay simultaneously is not implemented.'))
      new_records <- make_component(component,
        line = NA,
        delay = component$delay,
        .passthrough = TRUE) |>
        calculate_formula(
          export = columns)
      
      component$rec_source$records <- new_records |>
        dplyr::rename(
          !!sym(component$rec_source$rec_pid) := pid,
          !!sym(component$rec_source$ts) := ts) |>
        select(-tidyr::any_of(c('row_id', 'window')))
      
      component$delay <- NA
      
      browser()
    }
    
    # Add timestamp column
    if(!component$.passthrough)
      columns <- c(columns, 'ts')
    
    if(component$rec_source$type == 'direct') {
      if(component$.passthrough || keep_names_unchanged)
        component_columns <- columns
      else
          component_columns <- paste0(comp_name, '_', columns)
    } else
      component_columns <- columns
    
    # Por algum motivo, o "lag()" não funciona com "range between window preceeding and delay preceeding", i.e. não é
    # possível aplicar line *e* delay em uma só chamada à função de window. O last_value() funciona com esse "range".
    if(component$rec_source$type == 'column') {
      stop('Column-type record source is not yet supported.')
      capture_col <- component$rec_source$capture_col
      columns_sql <- paste0('case when "name" = \'', columns, '\' then "capture_col" else null end')
    }
    else {
      columns_sql <- paste0(
        'case when "name" = \'', component$rec_source$rec_name, '\' then "', columns, '" else null end')
    }

    over_clause <- paste0('partition by "pid", "name" order by "ts"')

    # Give priority to access via *line*.
    if(!is.na(component$line)) {
      # TODO: Test if last_value() + find_last_ignore_nulls() has better performance than just find_last_ignore_nulls().
      sql_txts <- paste0('last_value(', columns_sql, ') over (', over_clause,
        ' rows between unbounded preceding and ', component$line, ' preceding)')
    } else {
      # Otherwise, produce access via *delay*.
      range_start_clause <- ifelse(component$comp_window == Inf,
        'range between unbounded preceding',
        paste0('range between \'', component$comp_window, '\'::interval preceding'))

      sql_txts <- paste0('last_value(', columns_sql, ') over (', over_clause, ' ',
        range_start_clause, ' and \'', component$delay, '\'::interval preceding', ')')
    }

    commands <- purrr::map2(component_columns, sql_txts,
      ~rlang::exprs(!!..1 := dplyr::sql(!!..2))) |>
      unlist()

    return(commands)
  }

  # First, generate commands.
  commands <- purrr::map2(names(components), components, produce_component) |> unlist()
  
  # Then, apply on the board.
  board <- dplyr::transmute(board,
    row_id = dplyr::sql('row_number() over ()'),
    pid, ts,
    !!!commands)

  # Then fill the blanks downward with the last non-blank value.
  board <- board |>
    dbplyr::window_order(pid, ts) |>
    tidyr::fill(!c(row_id, pid, ts))

# Compute window --------------------------------------------------------------------------------------------------
  # The front (most recent point) of the window is column ts of the current line. The back (oldest point) is the
  # smallest among the ts's of the components.
  if(length(unique(var_map$component_name)) > 1 && !input_is_phenotype)
    sql_ts_least <- paste0('least(', paste0(paste0(unique(var_map$component_name), '_ts'), collapse = ', '), ')')
  else
    sql_ts_least <- 'ts'

  board <- board |>
    dplyr::mutate(
      window = ts - dplyr::sql(sql_ts_least),
      ts_row = dplyr::sql('last_value(row_id) over (partition by "pid", "ts")'))

  # Keep only the most complete computation in each timestamp. The most recent computation is the last one in each
  # timestamp. 'max(row_id) over (partition by "pid", "ts")' could find the row with the largest (most complete) row_id
  # in each timestamp, but last_value() in this context gives the same result
  # We also need to potentially require all fields be filled. Let's compact that into a single call to dplyr::filter(),
  # hence a single WHERE statement.
  if(.require_all || !is.null(.dont_require)) {
    # If .dont_require is provided, then all components, except those specified, will be required, even if .require_all
    # is FALSE.
    required_components <- setdiff(names(components), .dont_require)
    if(length(required_components) > 0) {
      sql_txt <- paste0(required_components, '_ts is not null') |> paste(collapse = ' and ')
      board <- dplyr::filter(board,
        row_id == ts_row && dplyr::sql(sql_txt))
    } else {
      # No required components after all, because all were excluded by .dont_require.
      board <- dplyr::filter(board,
        row_id == ts_row)
    }
  } else {
    board <- board |>
      dplyr::filter(row_id == ts_row)
  }

  # Clean temporary variables used for computing the formula and the window.
  board <- board |>
    dplyr::select(row_id, pid, ts, window, !!!g_vars)

# Filter and calculate --------------------------------------------------------------------------------------------
  # Impose the time window, if any.
  if(!is.na(window))
    board <- dplyr::filter(board,
      window < local(window))

  # Limit rows.
  if(!is.na(.lim))
    board <- board |>
      head(n = lim)

  # Calculate the formula, if any.
  res_vars <- NULL
  if(!is.null(fml)) {
    if(.cascaded) {
      # Compute one at a time, so that the prior result can be used in the next formula.
      for(i in seq(fml)) {
        if(class(fml[[i]]) == 'list') {
          if(any(lapply(fml[[i]], class) == 'list'))
            stop('Formulas cannot be nested deeper than 1 level.')
          
          res_vars <- c(res_vars, names(fml[[i]]))
          commands <- map2(names(fml[[i]]), fml[[i]],
              ~rlang::exprs(!!..1 := dplyr::sql(!!..2))) |>
            unlist()
          board <- dplyr::mutate(board,
            !!!commands)
        } else {
          res_vars <- c(res_vars, names(fml)[i])
          sql_txt <- fml[[i]]
          board <- dplyr::mutate(board,
            !!rlang::sym(names(fml)[i]) := dplyr::sql(sql_txt))
        }
      }
    } else {
      # Check if these formulas are meant to be cascaded.
      if(any(lapply(fml, class) == 'list'))
        stop('Nested formulas require .cascaded = TRUE.')
      
      # Compute them all in one statement, so that computation time is (potentially, haven't tested) minimized.
      res_vars <- c(res_vars, names(fml))
      commands <- purrr::map2(names(fml), fml,
        ~rlang::exprs(!!..1 := dplyr::sql(!!..2))) |> unlist()

      board <- dplyr::mutate(board,
        !!!commands)
    }
  }

# Collapse SQL and return -----------------------------------------------------------------------------------------
  # For some reason, apparently a bug in dbplyr's SQL translation, we need to "erase" an ORDER BY "pid", "ts" that is
  # left over in the translated query. That ORDER BY persists even if you posteriorly do a dplyr::group_by() on the
  # result of the phenotype (i.e. the board at this point). This causes the SQL server's query engine to raise an error,
  # saying that "ts" must also be part of the GROUP BY. This left over ORDER BY "pid", "ts" apparently comes from the
  # dbplyr::window_order() call that was necessary to guarantee the intended behavior of the call to
  # tidyr::fill.lazy_tbl() above.
  board <- board |>
    arrange()
  
  # Calling collapse() is necessary to minimize the accumulation of "lazy table generating code". That accumulation
  # can produce error "C stack usage is too close to the limit", especially when compounding phenotypes (i.e. using 
  # phenotypes as components of other phenotypes). See https://github.com/tidyverse/dbplyr/issues/719. Thanks, mgirlich!
  board <- board |>
    dplyr::collapse(cte = TRUE)

  # Write attributes used to communicate internally in Phea.
  attr(board, 'phea') <- 'phenotype'
  attr(board, 'phea_res_vars') <- res_vars
  
  if(.clip_sql) {
    sql_txt <- dbplyr::sql_render(board)
    writeClipboard(sql_txt)
    return(invisible(sql_txt))
  } else {
    return(board)
  }
}

