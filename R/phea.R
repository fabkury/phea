# Data ------------------------------------------------------------------------------------------------------------
if(!exists('.pheaglobalenv'))
  .pheaglobalenv <- new.env(parent=emptyenv())


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
setup_phea <- function(connection, schema, .verbose = TRUE) {
  assign('con', connection, envir = .pheaglobalenv)
  assign('schema', schema, envir = .pheaglobalenv)
  assign('verbose', .verbose, envir = .pheaglobalenv)
}


# Keep row by & keep change of ------------------------------------------------------------------------------------
#' Keep [first or last] row by [window function]
#'
#' Keeps the row containing the group-wise maximum or minimum.
#'
#' Divides lazy_tbl according to `partition`, and in each partition keeps only the row containing the maximum or minimum
#' of column `by`.
#'
#' @export
#' @param lazy_tbl Lazy table to be filtered.
#' @param by Column to pick rows by.
#' @param partition Character vector. Variable or variables to define the partition.
#' @param pick_last Logical. If `TRUE`, will pick the last row, instead of first.
#' @return Lazy table with filtered rows.
keep_row_by <- function(lazy_tbl, by, partition, pick_last = FALSE) {
  # sql_txt <- paste0(ifelse(use_max, 'max', 'min'), '("', by, '") ',
  #   'over (partition by "', paste0(partition, collapse = '", "'), '")')
  if(pick_last)
    sql_txt <- paste0('cume_dist() over (partition by "', 
      paste0(partition, collapse = '", "'), '" order by "', by, '")')
  else
    sql_txt <- paste0('row_number() over (partition by "', 
      paste0(partition, collapse = '", "'), '" order by "', by, '")')

  res <- lazy_tbl |>
    dplyr::mutate(
      phea_calc_var = dplyr::sql(sql_txt)) |>
    dplyr::filter(phea_calc_var == 1) |>
    dplyr::select(-phea_calc_var)
  res
}

#' Keep change of [a column or SQL expression]
#'
#' Keeps only the rows where the value of `of` changes. `of` can be the name of a column, or any SQL expression valid
#' inside a `SELECT` statement.
#' 
#' If `partition` is provided, changes are limited to within it. If `order` provided, rows
#' are ordered (and change is detected) according to that column or columns. The first row (within a partition or not)
#' is always kept.  
#'
#' @export
#' @param lazy_tbl Lazy table to be filtered.
#' @param of Character. Column or SQL expression. Only rows where `of` changes are kept.
#' @param partition Character. Optional. Variable or variables to define the partition.
#' @param order Character. Optional. If provided, this or these column(s) will define the ordering of the rows and hence
#' how changes are detected.
#' @return Lazy table with only rows where `of` changes in comparison to the previous row.
keep_change_of <- function(lazy_tbl, of, partition = NULL, order = NULL, con = NULL) {
  if(is.null(con)) {
    if(is.null(.pheaglobalenv$con))
      stop('Connection must be provided or setup previously with setup_phea().')
    else
      con <- .pheaglobalenv$con
  }
  
  if(!is.null(partition))
    of <- c(partition, of)
  
  of_names <- paste0('phea_kco_var', seq(of))
  
  commands_a <- purrr::map2(of_names, of,
    ~rlang::exprs(!!..1 := dplyr::sql(!!..2))) |>
    unlist()
  
  lag_names <- paste0('phea_kco_lag', seq(of))
  
  lag_sql <- paste0('lag(', of_names, ')')
  
  commands_b <- purrr::map2(lag_names, lag_sql, ~rlang::exprs(
    !!..1 := dbplyr::win_over(
      sql(!!..2),
      partition = partition,
      order = order,
      con = con))) |>
    unlist()
  
  # Keep rows where either
  # - prior row is different from current row
  # - if there is no prior row, keep current row if current row is not NA
  # - if all rows are NA, nothing will be in the output.
  
  commands_c <- paste0('(is.na(', lag_names, ') && !is.na(', of_names, ')) || ', lag_names, ' != ', of_names) |>
    paste0(collapse = ' || ') |>
    str2lang()
  
  lazy_tbl |>
    mutate(!!!commands_a) |>
    mutate(!!!commands_b) |>
    filter(!!commands_c) |>
    select(-all_of(lag_names), -all_of(of_names))
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


# sql0, sqlt & sqla -----------------------------------------------------------------------------------------------
#' SQL table
#'
#' Produces lazy table object of `table` in the preconfigured default schema.
#'
#' This function is a shorthand for `select * from def_schema.table;`. `def_schema` is the schema passed to
#' `setup_phea()` previously.
#'
#' @export
#' @param table Unquoted name of the table to be accessed within `def_schema`.
#' @param schema Optional. Name of schema to use. If provided, overrides `def_schema`.  
#' @return Lazy table equal to `select * from def_schema.table;`.
#' @seealso [sql0()] to run arbitrary SQL. [sqla()] to run arbitrary SQL with lazy tables.
#' @examples
#' `sqlt(person)`
#' `sqlt(condition_occurrence)`
sqlt <- function(table, schema = NULL) {
  if(!exists('con', envir = .pheaglobalenv))
    stop('SQL connection not found. Please call setup_phea() before sqlt().')
  
  if(is.null(schema))
    schema <- .pheaglobalenv$schema
  
  dplyr::tbl(.pheaglobalenv$con,
    dbplyr::in_schema(schema,
      deparse(substitute(table))))
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
#'   table. If the latter case, `.ts` and `.pid` must be provided.
#' @param line Interger. Which line to pick. 0 = skip no lines, 1 = skip one line, 2 = skip two lines, etc.
#' @param delay Character. Time interval in SQL language. Minimum time difference between phenotype date and component
#'   date.
#' @param window Character. Time interval in SQL language. Maximum time difference between phenotype date and component
#'   date.
#' @param .ts Unquoted character. If passing a lazy table to `input_source`, `.ts` is used as `ts` to buid a record
#'   source.
#' @param .pid Unquoted character. If passing a lazy table to `input_source`, `.pid` is used as `pid` to buid a record
#'   source.
#' @seealso [make_record_source()] to create a record source.
#' @return Phea component object.
make_component <- function(input_source, line = NA, delay = NA, window = NA, rec_name = NA,
  .passthrough = FALSE, .ts = NULL, .pid = NULL, .fn = NA, .ts_fn = NULL,
  ahead = NA, up_to = NA) {
  component <- list()

  # if((!is.na(ahead) || !is.na(up_to)) && (!is.na(delay) || !is.na(window))) {
  #   stop('Cannot utilize ahead/up_to together with delay/window.')
  # }
  
  if(isTRUE(attr(input_source, 'phea') == 'component')) {
    # rec_source is actually a component.
    component <- input_source
  } else {
    if(isTRUE(attr(input_source, 'phea') == 'record_source')) {
      component$rec_source <- input_source
    } else {
      if(isTRUE(attr(input_source, 'phea') == 'phenotype')) {
        # Result is from calculate_formula().
        component$rec_source <- make_record_source(
          records = input_source, ts = ts, pid = pid)
      } else {
        if('tbl_lazy' %in% class(input_source)) {
          component$rec_source <- make_record_source(
            records = input_source,
            .ts = deparse(substitute(.ts)),
            .pid = deparse(substitute(.pid)))
        } else {
          stop('Unable to recognize input_source.')
        }
      }
    }
      
    if(is.na(line) && is.na(delay) && is.na(window) && is.na(ahead) && is.na(up_to))
      line <- 0
    
    # Guarantee existance of the objects, even if it's NA.
    component$line <- line
    component$delay <- delay
    component$comp_window <- window
    component$.passthrough <- .passthrough
    component$ahead <- ahead
    component$up_to <- up_to
    component$fn <- .fn
  }
  
  ## Overwrite if provided.
  
  if(!is.na(line))
    component$line <- line
  
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
  
  if(!is.null(.ts_fn))
    component$ts_fn <- .ts_fn
  else
    component$ts_fn <- 'last_value' # component$fn
  
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
#' @seealso [make_component()] to create a component from a record source.
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
#' Calculate phenotype formula(s)
#'
#' Receives a list of components, and a formula (or list of formulas) in SQL language, and computes the result by 
#' gathering records according to their timestamps.
#'
#' The data type of the columns from the components (only those that are actually used or exported) cannot be Boolean.
#'
#' @export
#' @param components A list of components, a record source, or a lazy table. If a record source or lazy table is 
#'   provided, a default component will be made from it.
#' @param fml Formula or list of formulas.
#' @param export List of additional variables to export.
#' @param add_components Additional components. Used mostly in case components is not a list of components.
#' @param .ts,.pid,.delay,.line If supplied, these will overwrite those of the given component.
#' @param .require_all If `TRUE`, returns only rows where all components to have been found according to their
#'   timestamps (even if their value is NA). If `.dont_require` is provided, `.require_all` is ignored.
#' @param .lim Maximum number of rows to return. This is imposed before the calculation of the formula.
#' @param .dont_require If provided, causes formula to require all components (regardless of .require_all), except for
#'   those listed here.
#' @param .cascaded If `TRUE` (default), each formula is computed in a separate, nested SELECT statement. This allows
#'   the result of the prior formula to be used in the following, at the potential cost of longer computation times.
#' @param .clip_sql If `TRUE`, instead of lazy table it returns the SQL query as a SQL object (can be converted to
#'   character using `as.character()`), and also copies it to the clipboard.  
#' @param .filter Character vector. Logical conditions to satisfy. These go into the SQL `WHERE`
#'   clause. Only rows satisfying all conditions provided will be returned.  
#' @param .out_window Character vector. Names of components to not be included when calculating the window.
#' @param .dates Tibble. Column names must be `pid` (person ID) and `ts` (timestamp). If provided, these dates (for each
#' person ID) are added to the board, so that the phenotype computation can be attempted at those times.
#' @param .kco Logical. If `TRUE` (default), output will include only rows where the result of any of the formulas
#' change. If `FALSE`, all dates from the components will be present.
#' @return Lazy table with result of formula or formulas.
calculate_formula <- function(components, fml = NULL, window = NA, export = NULL, add_components = NULL,
  .ts = NULL, .pid = NULL, .delay = NULL, .line = NULL, .require_all = FALSE, .lim = NA, .dont_require = NULL,
  .filter = NULL, .cascaded = TRUE, .clip_sql = FALSE, .out_window = NULL, .dates = NULL, .kco = TRUE) {
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
  
  # Read input filter or filters
  if(!is.null(.filter)) {
    # Extract components from filters.
    filter_vars <- unlist(.filter) |>
      stringr::str_match_all('([A-z][A-z0-9_]+)') |>
      unlist() |> unique()
    
    # Filter bogus matches (eg. SQL keywords in the formula) by keeping only the composed_names that can possibly come from
    # the given combination of record sources and components.
    filter_vars <- filter_vars[filter_vars %in% var_map$composed_name]
  } else {
    # If no filter, no composed_names to export from it.
    filter_vars <- NULL
  }

  # Add variables required by the filters.
  g_vars <- c(g_vars, filter_vars)
  
  # Add variables requested to export.
  g_vars <- c(g_vars, export)
  
  # Remove duplicates in case of any, just in case.
  g_vars <- unique(g_vars)
  
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
  
  # Add extra dates, if any.
  if(!is.null(.dates)) {
    dates <- paste0("SELECT * FROM (VALUES ",
      paste0("(", .dates$pid, ", '", .dates$ts, "'::date)", collapse = ', '),
      ") AS pid_ts (pid, ts)") |>
      sql0()
    board <- dplyr::union_all(board, dates)
  }

# Produce components ----------------------------------------------------------------------------------------------
  produce_component <- function(comp_name, component) {
    columns <- var_map |>
      dplyr::filter(component_name == comp_name) |>
      dplyr::pull(column) |>
      unique()

    if((!is.na(component$delay) || !is.na(component$comp_window)) && !is.na(component$line)) {
      stop(paste0('Component ', comp_name, ': line and delay (or window) simultaneously is not implemented.'))
      new_records <- make_component(component,
        line = NA,
        delay = component$delay,
        .passthrough = TRUE) |>
        calculate_formula(
          export = columns)
      
      component$rec_source$records <- new_records |>
        dplyr::rename(
          !!rlang::sym(component$rec_source$rec_pid) := pid,
          !!rlang::sym(component$rec_source$ts) := ts) |>
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
    
    if(!is.na(component$line)) {
      # Give priority to access via *line*.
      sql_start <- paste0('last_value(', columns_sql, ') over (', over_clause, ' ')
      
      sql_txts <- paste0(sql_start,
        'rows between unbounded preceding and ',
        ifelse(component$line == 0, 'current row', paste0(component$line, ' preceding')), ')')
    } else {
      if(!is.na(component$delay) || !is.na(component$comp_window)) {
        # Otherwise, produce access via *delay*.
        comp_fn <- component$fn
        if(is.na(comp_fn))
          comp_fn <- 'last_value' # Lookbehind defaults to last_value
        
        ts_fn <- component$ts_fn
        if(is.na(ts_fn))
          ts_fn <- comp_fn
        
        use_fn <- rep(comp_fn, length(columns))
        use_fn[columns == 'ts'] <- ts_fn
        
        sql_start <- paste0(use_fn, '(', columns_sql, ') over (', over_clause, ' ')
        
        if(is.na(component$up_to)) {
          sql_txts <- paste0(sql_start, 'range between ',
            ifelse(is.na(component$comp_window) || component$comp_window == -Inf, 'unbounded',
              paste0('\'', component$comp_window, '\'::interval')), ' preceding ',
            'and ', ifelse(is.na(component$delay), 'current row',
              paste0('\'', component$delay, ' days\'::interval preceding')),
            ')')
        } else {
          sql_txts <- paste0(sql_start, 'range between ',
            ifelse(is.na(component$comp_window) || component$comp_window == -Inf, 'unbounded',
              paste0('\'', component$comp_window, '\'::interval')), ' preceding ',
            'and ', ifelse(component$up_to == Inf, 'unbounded',
              paste0('\'', component$up_to, '\'::interval')),
            ' following)')
        }
      } else {
        # Otherwise, produce access via *ahead/up_to*.
        if(is.na(component$ahead) && is.na(component$up_to)) {
          stop('Unable to identify type of component. All parameters are empty.')
        }
        
        comp_fn <- component$fn
        if(is.na(comp_fn))
          comp_fn <- 'first_value' # Lookahead defaults to first_value
        
        ts_fn <- component$ts_fn
        if(is.na(ts_fn))
          ts_fn <- comp_fn
        
        use_fn <- rep(comp_fn, length(columns))
        use_fn[columns == 'ts'] <- ts_fn
        
        sql_start <- paste0(use_fn, '(', columns_sql, ') over (', over_clause, ' ')
        
        sql_txts <- paste0(sql_start, 'range between ',
          ifelse(is.na(component$ahead), '0',
            paste0('\'', component$ahead, '\'::interval')), ' following ',
          'and ', ifelse(is.na(component$up_to) || component$up_to == Inf, 'unbounded',
            paste0('\'', component$up_to, '\'::interval')),
          ' following)')
      }
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
  
  # Then fill the blanks downward with the last non-blank value, within the patient.
  board <- board |>
    dbplyr::window_order(pid, ts) |>
    dplyr::group_by(pid) |>
    tidyr::fill(!c(row_id, pid, ts)) |>
    ungroup()
  
  # For some reason, apparently a bug in dbplyr's SQL translation, we need to "erase" an ORDER BY "pid", "ts" that is
  # left over in the translated query. That ORDER BY persists even if you posteriorly do a dplyr::group_by() on the
  # result of the phenotype (i.e. the board at this point). This causes the SQL server's query engine to raise an error,
  # saying that "ts" must also be part of the GROUP BY. This left over ORDER BY "pid", "ts" apparently comes from the
  # dbplyr::window_order() call that was necessary to guarantee the intended behavior of the call to
  # tidyr::fill.lazy_tbl() above.
  board <- board |>
    arrange()

# Compute window --------------------------------------------------------------------------------------------------
  # TODO: REVISE THIS: The front (most recent point) of the window is column ts of the current line. The back (oldest
  # point) is the smallest among the ts's of the components.
  window_components <- setdiff(var_map$component_name, .out_window) |> unique()
  if(length(window_components) > 1 && !input_is_phenotype) {
    sql_ts_least <- paste0('least(', paste0(paste0(window_components, '_ts'), collapse = ', '), ')')
    sql_ts_greatest <- paste0('greatest(', paste0(paste0(window_components, '_ts'), collapse = ', '), ')')
  }
  else {
    # TODO: Improve this a bit.
    sql_ts_least <- 'ts'
    sql_ts_greatest <- 'ts'
  }

  board <- board |>
    dplyr::mutate(
      window = dplyr::sql(sql_ts_greatest) - dplyr::sql(sql_ts_least),
      phea_ts_row = dplyr::sql('last_value(row_id) over (partition by "pid", "ts")'))

# Keep the most complete computations -----------------------------------------------------------------------------
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
        row_id == phea_ts_row && dplyr::sql(sql_txt))
    } else {
      # No required components after all, because all were excluded by .dont_require.
      board <- dplyr::filter(board,
        row_id == phea_ts_row)
    }
  } else {
    board <- board |>
      dplyr::filter(row_id == phea_ts_row)
  }
  
# Filter and calculate --------------------------------------------------------------------------------------------
  if(!is.null(.filter)) {
    sql_txt <- paste0('(', paste0(.filter, collapse = ') AND ('), ')')
    board <- board |>
      filter(sql(sql_txt))
  }

  # Clean temporary variables used for computing the formula and the window.
  board <- board |>
    dplyr::select(row_id, pid, ts, window, !!!g_vars)
  
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
          
          commands <- purrr::map2(names(fml[[i]]), fml[[i]],
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
  # Keep change of, if requested.
  if(class(.kco) == 'logical') {
    if(length(.kco) > 1)
      stop('If logical, .kco must be of length 1.')
    
    if(.kco)
      board <- board |>
        keep_change_of(res_vars, partition = 'pid', order = 'ts')
  } else {
    if(class(.kco) != 'character')
      stop('.kco must be logical or character vector.')
    
    board <- board |>
      keep_change_of(.kco, partition = 'pid', order = 'ts')
  }
  
  # Calling collapse() is necessary to minimize the accumulation of "lazy table generating code". That accumulation
  # can produce error "C stack usage is too close to the limit", especially when compounding phenotypes (i.e. using 
  # phenotypes as components of other phenotypes). See https://github.com/tidyverse/dbplyr/issues/719. Thanks, mgirlich!
  board <- board |>
    dplyr::collapse(cte = TRUE)

  # Write attributes used to communicate internally in Phea.
  attr(board, 'phea') <- 'phenotype'
  attr(board, 'phea_res_vars') <- res_vars
  attr(board, 'phea_out_vars') <- g_vars
  
  if(.clip_sql) {
    sql_txt <- dbplyr::sql_render(board)
    writeClipboard(sql_txt)
    return(invisible(sql_txt))
  } else {
    return(board)
  }
}


# Plot phenotype --------------------------------------------------------------------------------------------------
#' Plot a phenotype.
#'
#' Plots the result from `calculate_formula()` in an interactive timeline chart using `plotly`.
#'
#' Collects (downloads) the results and creates interactive timeline chart using the `plotly` library.
#'
#' @export
#' @param board Phenotype object returned by `calculate_formula()`.
#' @param pid Required. ID of the patient to be included in the chart.
#' @param exclude Optional. Names of columns to not plot.
#' @param verbose If TRUE, will let you know how long it takes to `collect()` the data.
#' @return Plot created by `plotly::plot_ly()` within `plotly::subplot()`.
phea_plot <- function(board, pid, plot_title = NULL, exclude = NULL, verbose = NULL) {
  board <- board |>
    dplyr::filter(pid == local(pid))
  
  # If not provided, use global default set by setup_phea().
  if(is.null(verbose))
    verbose <- .pheaglobalenv$verbose
  
  if(verbose)
    cat('Collecting lazy table, ')
  board_data <- dplyr::collect(board)
  if(verbose)
    cat('done. (turn this message off with `verbose = FALSE`)\n')
  
  # Plot all columns except some.
  if(all(c('row_id', 'pid', 'ts', 'window') %in% colnames(board_data))) {
    # The board has the base columns of a phenotype result. Remove them.
    chart_items <- setdiff(colnames(board_data), c('row_id', 'pid', 'ts', 'window'))
  }
  
  if(!is.null(exclude))
    chart_items <- setdiff(chart_items, exclude)
  
  make_chart <- function(chart_item) {
    chart_data <- board_data |>
      select(ts, value = !!sym(chart_item))
    
    if(any(!is.na(chart_data$value)))
      range <- c(min(chart_data$value, na.rm = TRUE), max(chart_data$value, na.rm = TRUE))
    else
      range <- NA
    
    res_plot <- chart_data |>
      plotly::plot_ly(x = ~ts) |>
      plotly::add_lines(y = ~value,
        name = chart_item,
        line = list(shape = 'hv')) |>
      plotly::layout(
        dragmode = 'pan',
        legend = list(orientation = 'h'),
        yaxis = list(
          range = range,
          fixedrange = TRUE))
    
    return(res_plot)
  }
  
  plots <- sapply(chart_items, make_chart, simplify = FALSE, USE.NAMES = TRUE)
  
  subplot_args <- c(plots,
    nrows = length(plots),
    shareX = TRUE,
    titleX = FALSE)
  
  res <- do.call(plotly::subplot, subplot_args)
  
  return(res)
}

