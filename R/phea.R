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
#' @param schema Schema to be used by default in `sqlt()`. If no schema, use `NA`.
#' @param .verbose Logical. Optional. If TRUE (default), functions will print to console at times.
#' @param .fix_dbplyr_spark Logical. Optional. Very niche functionality. Set to `TRUE` to attempt to fix the use of
#' `IGNORE NULLS` by the OBDC driver connected to a Spark SQL server/cluster.
setup_phea <- function(connection, schema, .verbose = TRUE, .fix_dbplyr_spark = FALSE) {
  assign('con', connection, envir = .pheaglobalenv)
  assign('schema', schema, envir = .pheaglobalenv)
  assign('verbose', .verbose, envir = .pheaglobalenv)
  
  if(.fix_dbplyr_spark) {
    if(connection@info$dbms.name == "Spark SQL") {
      # Fix dbplyr's last_value() implementation.
      `last_value_sql.Spark SQL` <<- function(con, x) {
        dbplyr:::build_sql("LAST_VALUE(", ident(as.character(x)), ", true)", con = con)
      }
    }
  }
}


# Pick row by & keep change of ------------------------------------------------------------------------------------
#' Pick [first or last] row by [window function]
#'
#' Pick the rows that contain the group-wise aggregate value in each partition.
#'
#' Divides `lazy_tbl` according to `partition`, and in each one keeps only the row picked by the result of a window
#' function `.fn`.
#' 
#' If `.fn` is not provided, defaults to picking the rows where `by` is maximum.
#' 
#' If `pick_last` is `TRUE`, defaults instead to rows where `by` is minimum.
#' 
#' If `.val` is provided, keeps only the rows where the result of `.fn(by)` in each partition is equal to `.val`.
#'
#' @export
#' @param lazy_tbl Lazy table to be filtered.
#' @param by Character. Column(s) to pick rows by.
#' @param partition Character vector. Column name(s) to define the partition.
#' @param pick_last Logical. If `TRUE`, will pick the last row, instead of first.
#' @param .fn Character. Name of the aggregate function to use, *without parentheses*. E.g.: `max`, `cume_dist`.
#' @param .val Character or numeric. Literal value to compare to result of `.fn`.
#' @return Lazy table with filtered rows.
pick_row_by <- function(lazy_tbl, by, partition, pick_last = FALSE,
  .fn = NULL, .fn_arg = NULL, .val = NULL) {
  if(is.null(.fn)) {
    # The default operation is done using `row_number() == 1`/`cume_dist() == 1`, as opposed to `max()`/`min()`, to
    # spare the need to create a new variable (column) to perform the computation, since window functions cannot go in
    # `WHERE` clauses.
    if(pick_last)
      .fn <- 'cume_dist'
    else
      .fn <- 'row_number'
    
    if(is.null(.val))
      .val <- 1
  }
  
  sql_txt <- paste0(.fn, '(', .fn_arg, ') over (partition by "', 
    paste0(partition, collapse = '", "'), '" order by "', by, '")')
  
  if(is.null(.val)) {
    # The user provided .fn but not .val. If .fn had _not_ been provided, .val would be non-null.
    # This means the user just provided the function name, but no target value. In this case we compare the function
    # result to its input value, .fn(x) == x.
    res <- lazy_tbl |>
      dplyr::mutate(
        phea_calc_var = dplyr::sql(sql_txt)) |>
      dplyr::filter(!!by == phea_calc_var) |>
      dplyr::select(-phea_calc_var)
  } else {
    # neither .val or .fn are null.
    res <- lazy_tbl |>
      dplyr::mutate(
        phea_calc_var = dplyr::sql(sql_txt)) |>
      dplyr::filter(phea_calc_var == local(.val)) |>
      dplyr::select(-phea_calc_var)
  }
  
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
#' @param of Character vector. Name of column(s) or SQL expression(s). Only rows where the value of `of` changes are
#' kept. If multiple columns or expressions are provided, a change in any of them causes the row to be in the output.
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
  
  lag_names <- paste0('phea_kco_lag', seq(of))
  lag_sql <- paste0('lag(', of, ')')
  
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
  commands_c <- paste0('(is.na(', lag_names, ') && !is.na(', of, ')) || ', lag_names, ' != ', of) |>
    paste0(collapse = ' || ') |>
    str2lang()
  
  lazy_tbl |>
    mutate(!!!commands_b) |>
    filter(!!commands_c) |>
    select(-all_of(lag_names))
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
    str_replace(';$', '')
  
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
#' Creates a component from the given `input_source` record source and optional parameters.
#' 
#' If `input_source` is a record source, it is used.
#' 
#' If `input_source` is a component, it is copied, and any other paremeter if provided overwrites the original one.
#' 
#' If `input_source` is a lazy table, a record source is generated from it, and used. In this case, arguments .pid and
#' .ts must also be provided.
#'
#' @export
#' @param input_source A record source from `make_record_source()`, a component from `make_component()`, or a lazy
#'   table. If the latter case, `.ts` and `.pid` must be provided.
#' @param line Interger. Which line to pick. 0 = skip no lines, 1 = skip one line, 2 = skip two lines, etc.
#' @param delay Character. Minimum time difference between phenotype date and component date. Time interval in SQL
#'   language, including any necessary type casting according to the SQL flavor of the server. Examples in PostgreSQL:
#'   `'3 months'::interval`, `'20 seconds'::interval`, `'1.5 hours'::interval`.
#' @param window Character. Maximum time difference between phenotype date and component date. Time interval in SQL
#'   language (see argument `delay`). 
#' @param .ts Unquoted characters. If passing a lazy table to `input_source`, `.ts` is used as `ts` to buid a record
#'   source. See \code{\link{make_record_source}}.
#' @param .pid Unquoted characters. If passing a lazy table to `input_source`, `.pid` is used as `pid` to buid a record
#'   source. See \code{\link{make_record_source}}.
#' @seealso [make_record_source()] to create a record source.
#' @return Phea component object.
#' @examples
#' diabetes_mellitus <- sqlt(condition_occurrence) |>
#'   filter(condition_concept_id == 201820) |>
#'   make_component(
#'     .pid = person_id,
#'     .ts = condition_start_datetime)
#'     
#' diabetes_mellitus_6_mo_ago <- sqlt(condition_occurrence) |>
#'   filter(condition_concept_id == 201820) |>
#'   make_component(
#'     .pid = person_id,
#'     .ts = condition_start_datetime,
#'     delay = "'6 months'::interval")
#'     
make_component <- function(input_source, line = NA, bound = NA, delay = NA, window = NA, ahead = NA, up_to = NA,
  .passthrough = FALSE, .ts = NULL, .pid = NULL, .fn = NA, .ts_fn = NA,
  .rows = NULL, .range = NULL) {
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
      if(isTRUE(attr(input_source, 'phea') == 'phenotype')) {
        # Input is a phenotype. Make a record source from it.
        if(is.null(.ts) || is.null(.pid))
          component$rec_source <- make_record_source(
            records = input_source, ts = ts, pid = pid)
        else # .pid and .ts were provided. Use them.
          component$rec_source <- make_record_source(
            records = input_source, .ts = .ts, .pid = .pid)
      } else {
        if('tbl_lazy' %in% class(input_source)) {
          if(is.null(.ts) || is.null(.pid))
            stop('If providing a lazy table to make_component(), you must also provide both .ts and .pid.')
          
          # Input is a lazy table. Make a record source from it.
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
    component$bound <- bound
    component$delay <- delay
    component$comp_window <- window
    component$.passthrough <- .passthrough
    component$ahead <- ahead
    component$up_to <- up_to
    component$fn <- .fn
    component$ts_fn <- .ts_fn
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
  
  if(!is.null(.rows) && !is.null(.range))
    stop(paste0('.rows and .range cannot be used simultaneously.'))
  
  if((!is.na(component$delay) || !is.na(component$comp_window)) && !is.na(component$line))
    stop(paste0('line and delay/window cannot be used simultaneously.'))
  
# Build window function SQL ---------------------------------------------------------------------------------------
  component$columns <- component$rec_source$vars
  
  # Add timestamp column
  if(!component$.passthrough)
    component$columns <- c(component$columns, 'ts')
  
  columns_sql <- paste0('case when ', dbQuoteIdentifier(.pheaglobalenv$con, 'name'), ' = ',
    dbQuoteString(.pheaglobalenv$con, component$rec_source$rec_name),
    ' then ', dbQuoteIdentifier(.pheaglobalenv$con, component$columns), ' else null end')
  
  over_clause <- paste0('partition by ', dbQuoteIdentifier(.pheaglobalenv$con, 'pid'), ', ',
    dbQuoteIdentifier(.pheaglobalenv$con, 'name'), ' order by ', dbQuoteIdentifier(.pheaglobalenv$con, 'ts'))
  
  if(is.na(component$ts_fn))
    component$ts_fn <- 'last_value'
  
  # component_has_been_built is just to help us trim the code identation, as opposed to using nested if-else`s.
  component_has_been_built <- FALSE
  
  if(!is.null(.rows)) {
    # TODO
    component$access <- 'rows'
    component_has_been_built <- TRUE
  }
  
  if(!component_has_been_built && !is.null(.range)) {
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
    }) |>
      unlist(recursive = FALSE)
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
    
    component$window_sql <- sql(sql_txts)
    component_has_been_built <- TRUE
  }
  
# Finalize and return ---------------------------------------------------------------------------------------------
  if(!component_has_been_built)
    warning('Error: component_has_been_built is FALSE.')
  
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
#' @param records Lazy table with records to be used.
#' @param ts Unquoted characters. Name of the colum in `records` that gives the timestamp.
#' @param pid Unquoted characters. Name of the colum in `records` that gives the person (patient) identifier.
#' @param rec_name Character. Optional. Record name.
#' @param vars Character vector. Optional. Name of the colums to make available from `records`. If not supplied, all
#' columns are used.
#' @seealso [make_component()] to create a component from a record source.
#' @return Phea record source object.
make_record_source <- function(records, ts, pid, rec_name = NULL, vars = NULL, .ts = NULL, .pid = NULL) {
  rec_source <- list()

  rec_source$records <- records
  
  if(is.null(rec_name)) {
    # Generate random rec_name, 8 characters long, case-insensitive, starting with a letter.
    name_len <- 8
    rec_name <- c(sample(letters, 1), sample(c(letters, 0:9), name_len-1)) |>
      as.list() |> do.call(what = paste0)
  }

  rec_source$rec_name <- rec_name

  if(is.null(vars))
    vars <- colnames(records)
  
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
  rec_source$pid <- pid_name
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
#'   timestamps. If the timestamp is not null, the component is cosidered present even if its other values are null. If 
#'   `.dont_require` is provided, `.require_all` is ignored.
#' @param .lim Maximum number of rows to return. This is imposed before the calculation of the formula.
#' @param .dont_require If provided, causes formula to require all components (regardless of .require_all), except for
#'   those listed here.
#' @param .cascaded If `TRUE` (default), each formula is computed in a separate, nested SELECT statement. This allows
#'   the result of the prior formula to be used in the following, at the potential cost of longer computation times.
#' @param .clip_sql If `TRUE`, instead of a lazy table the return value is the code of the SQL query, and also copies it
#'   to the clipboard.  
#' @param .filter Character vector. Logical conditions to satisfy. Only rows satisfying all conditions provided will be
#'   returned. These go into the SQL `WHERE` clause.   
#' @param .out_window Character vector. Names of components to *not* be included when calculating the window.
#' @param .dates Tibble. Column names must be `pid` (person ID) and `ts` (timestamp). If provided, these dates (for each
#' person ID) are added to the board, so that the phenotype computation can be attempted at those times.
#' @param .kco Logical. "Keep change of". This is a shorthand to call `keep_change_of()` after computing the phenotype.
#' If `TRUE` (default), output will include only rows where the result of any of the formulas change. If `FALSE`,
#' `keep_change_of()` is not called and therefore all dates from every component will be present. This argument can
#' alternatively be a character vector of names of columns and/or SQL expressions, in which case `calculate_formula()`
#' will return only rows where the value of those columns or expressions change.
#' @return Lazy table with result of formula or formulas.
calculate_formula <- function(components, fml = NULL, window = NA, export = NULL, add_components = NULL,
  .ts = NULL, .pid = NULL, .delay = NULL, .line = NULL, .require_all = FALSE, .lim = NA, .dont_require = NULL,
  .filter = NULL, .cascaded = TRUE, .clip_sql = FALSE, .out_window = NULL, .dates = NULL, .kco = FALSE) {
# Prepare ---------------------------------------------------------------------------------------------------------
  # TODO: Improve the logic regarding these two variables below.
  keep_names_unchanged <- FALSE
  input_is_phenotype <- FALSE

# Parameter overload ----------------------------------------------------------------------------------------------
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

  # TODO: Document the operation below.
  if(is.null(names(components)) && isTRUE(attr(components[[1]], 'phea') == 'component'))
    names(components) <- components[[1]]$rec_source$rec_name

# Build variable map ----------------------------------------------------------------------------------------------
  # Variable map has all valid combinations of components, record sources, and record source columns.
  var_map <- purrr::map2(names(components), components, \(comp_name, component) {
    if(component$.passthrough || keep_names_unchanged) {
      res <- dplyr::tibble(
        component_name = comp_name,
        rec_name = component$rec_source$rec_name,
        column = component$columns,
        composed_name = component$columns,
        window_sql = component$window_sql)
    } else {
      res <- dplyr::tibble(
        component_name = comp_name,
        rec_name = component$rec_source$rec_name,
        column = component$columns,
        composed_name = paste0(comp_name, '_', component$columns),
        window_sql = component$window_sql)
    }
    return(res)
  }) |>
    dplyr::bind_rows()
  
# Read input formula ----------------------------------------------------------------------------------------------
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
            names(fmll)[i] <- paste0(prefix, ifelse(p == 1, '', p))
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

# Read input filter -----------------------------------------------------------------------------------------------
  if(!is.null(.filter)) {
    # Extract components from filters.
    filter_vars <- unlist(.filter) |>
      stringr::str_match_all('([A-z][A-z0-9_]+)') |>
      unlist() |> unique()
    
    # Filter bogus matches (eg. SQL keywords in the formula) by keeping only the composed_names that can possibly come
    # from the given combination of record sources and components.
    filter_vars <- filter_vars[filter_vars %in% var_map$composed_name]
  } else {
    # If no filter, no composed_names to export from it.
    filter_vars <- NULL
  }

# Preprocess g_vars and var_map -----------------------------------------------------------------------------------
  # Add variables required by the filters.
  g_vars <- c(g_vars, filter_vars)
  
  # Add variables requested to export.
  g_vars <- c(g_vars, export)
  
  # Remove duplicates in case of any, just in case.
  g_vars <- unique(g_vars)
  
  # Filter the variable map to contain only what we'll need.
  var_map <- dplyr::filter(var_map, column == 'ts' | composed_name %in% g_vars) |>
    dplyr::distinct()

# Prepare record sources ------------------------------------------------------------------------------------------
  prepare_record_source <- function(record_source) {
    rec_name <- record_source$rec_name

    # Select only the columns that will be needed later.
    out_vars <- var_map |>
      dplyr::filter(rec_name == .env$rec_name) |>
      dplyr::pull(column) |>
      unique()

    export_records <- record_source$records |>
      dplyr::transmute(
        name = local(rec_name),
        pid = !!rlang::sym(record_source$pid),
        ts = !!rlang::sym(record_source$ts),
        !!!rlang::syms(out_vars))

    return(export_records)
  }

  board <- record_sources |>
    purrr::map(prepare_record_source) |>
    purrr::reduce(dplyr::union_all)
  
# Add extra dates -------------------------------------------------------------------------------------------------
  if(!is.null(.dates)) {
    message('Warning: .dates is yet to be properly tested.')
    dates_table <- dbplyr::copy_inline(.pheaglobalenv$con, .dates)
    board <- dplyr::union_all(board, dates_table)
  }

# Apply components ------------------------------------------------------------------------------------------------
  # First, generate the commands.
  commands <- purrr::map2(var_map$composed_name, var_map$window_sql,
    ~rlang::exprs(!!..1 := !!dplyr::sql(..2))) |>
    unique() |>
    unlist(recursive = FALSE)
  # The unique() above is just in case, but is it needed? Seems like the only way there could be duplicates is if the
  # same component gets added twice to the call to calculate_formula(). 
  
  row_id_sql_txt <- paste0('row_number() over (order by ', dbQuoteIdentifier(.pheaglobalenv$con, 'pid'), ', ',
    dbQuoteIdentifier(.pheaglobalenv$con, 'ts'), ')')
  
  # Second, apply commands to the board all at once, so we only generate a single layer of "(SELECT ...)".
  board <- dplyr::transmute(board,
    row_id = dplyr::sql(row_id_sql_txt),
    pid, ts,
    !!!commands)
  
  # Third and final, fill the blanks downward with the last non-blank value, within the patient.
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
  window_components <- setdiff(var_map$component_name, .out_window)
  if(!input_is_phenotype && length(window_components) > 1) { # Window only makes sense if there is > 1 component.
    window_components_sql <- window_components |>
      unique() |>
      paste0('_ts') |>
      dbQuoteIdentifier(conn = .pheaglobalenv$con) |>
      paste0(collapse = ', ')
    
    sql_ts_least <- paste0('least(', window_components_sql, ')')
    sql_ts_greatest <- paste0('greatest(', window_components_sql, ')')
  }
  else {
    # TODO: Improve this a bit?
    # If there is only one component, window is zero. But if we just set window = 0, we mess with the data type.
    sql_ts_least <- dbQuoteIdentifier(.pheaglobalenv$con, 'ts')
    sql_ts_greatest <- sql_ts_least
  }
  
  # phea_ts_row is used to pick the best computation within each date. This is for the case when multiple data points
  # exist on the same date. The best computation for each date is the last row within that date.
  board <- board |>
    dplyr::mutate(
      window = dplyr::sql(sql_ts_greatest) - dplyr::sql(sql_ts_least),
      phea_ts_row = dplyr::sql('last_value(row_id) over (partition by pid, ts)'))

# Filter rows -----------------------------------------------------------------------------------------------------
  # The most complete computation is the last one in each timestamp. 'max(row_id) over (partition by "pid", "ts")' could
  # find the row with the largest (most complete) row_id in each timestamp, but last_value() in this context gives the
  # same result, and I suspect is potentially faster due to optimizations inside the SQL server.
  # We also need to:
  #  - potentially require all fields be filled.
  #  - potentially impose the time window.
  # Let us compact those three things into a single call to dplyr::filter(), in order to produce a single WHERE
  # statement, instead of three layers of SELECT ... WHERE.
  
  if(.require_all || !is.null(.dont_require)) {
    # If .dont_require is provided, then all components, except those specified, will be required, even if .require_all
    # is FALSE.
    required_components <- setdiff(names(components), .dont_require)
    if(length(required_components) > 0) {
      sql_txt <- required_components |>
        paste0('_ts') |>
        dbQuoteIdentifier(conn = .pheaglobalenv$con) |>
        paste0(' is not null') |>
        paste(collapse = ' and ')
      
      if(is.na(window)) {
        board <- dplyr::filter(board,
          row_id == phea_ts_row &&
            dplyr::sql(sql_txt))
      } else {
        board <- dplyr::filter(board,
          row_id == phea_ts_row &&
            dplyr::sql(sql_txt) &&
            window < local(window))
      }
    } else {
      # No required components after all, because all were excluded by .dont_require. Let's just filter by the most
      # complete computation.
      if(is.na(window)) {
        board <- dplyr::filter(board,
          row_id == phea_ts_row)
      } else {
        board <- dplyr::filter(board,
          row_id == phea_ts_row &&
            window < local(window))
      }
    }
  } else {
    # No need to require all components. Let's just filter by the most complete computation.
    if(is.na(window)) {
      board <- board |>
        dplyr::filter(row_id == phea_ts_row)
    } else {
      board <- board |>
        dplyr::filter(row_id == phea_ts_row &&
            window < local(window))
    }
  }
  
  # Apply filters, if provided.
  if(!is.null(.filter)) {
    sql_txt <- paste0('(', paste0(.filter, collapse = ') AND ('), ')')
    board <- board |>
      filter(sql(sql_txt))
  }
  
  # Limit number of output rows, if requested.
  if(!is.na(.lim))
    board <- board |>
      head(n = lim)

# Calculate formula -----------------------------------------------------------------------------------------------
  # Remove the original columns of the record sources, leaving only those produced by the components.
  board <- board |>
    dplyr::select(row_id, pid, ts, window, !!!g_vars)

  # Calculate the formulas, if any.
  res_vars <- NULL
  if(!is.null(fml)) {
    if(.cascaded) {
      # Compute one at a time, so that the prior result can be used in the next formula.
      for(i in seq(fml)) {
        cur_fml <- fml[[i]]
        
        # Is cur_fml a list?
        if(class(cur_fml) == 'list') {
          # cur_fml is itself a list. Compute the items in cur_fml.
          
          # Check if any of the items of cur_fml is itself a list.
          if(any(lapply(cur_fml, class) == 'list'))
            stop('Formulas cannot be nested deeper than 1 level.')
          
          # Export the names of the items of cur_fml to res_vars.
          res_vars <- c(res_vars, names(cur_fml))
          
          # Produce the vector of commands that calculates the items of cur_fml.
          commands <- purrr::map2(names(cur_fml), cur_fml,
              ~rlang::exprs(!!..1 := dplyr::sql(!!..2))) |>
            unlist()
          
          # Apply them, producing a layer of SELECT ... FROM (SELECT ...).
          board <- dplyr::mutate(board,
            !!!commands)
        } else {
          # cur_fml is not a list. Compute cur_fml itself.
          
          # Get the name from the parent object, fml. 
          res_vars <- c(res_vars, names(fml)[i])
          
          # The formula is the SQL.
          # sql_txt <- cur_fml
          
          # Apply to the board, producing a layer of SELECT ... FROM (SELECT ...).
          board <- dplyr::mutate(board,
            !!rlang::sym(names(fml)[i]) := dplyr::sql(cur_fml))
        }
      }
    } else {
      # .cascaded is turned off.
      # Let's check if any of the formulas is itself a list, which means .cascaded was supposed to be on.
      if(any(lapply(fml, class) == 'list'))
        stop('Nested formulas require .cascaded = TRUE.')
      
      # Export the names to res_vars.
      res_vars <- c(res_vars, names(fml))
      
      # Compute all formulas in one statement, so that computation time is (potentially, haven't tested) minimized.
      commands <- purrr::map2(names(fml), fml,
        ~rlang::exprs(!!..1 := dplyr::sql(!!..2))) |> unlist()

      board <- dplyr::mutate(board,
        !!!commands)
    }
  }

# Collapse SQL and return -----------------------------------------------------------------------------------------
  # Keep change of, if requested.
  # Parameter overload: .kco can be:
  # - logical: TRUE (apply kco over all result columns) or FALSE.
  # - character vector: Names of columns to apply kco.
  if(class(.kco) == 'logical') {
    if(length(.kco) > 1)
      stop('If logical, .kco must be of length 1.')
    
    # If no res_vars, keep_change_of would collapse to one row per partition.
    if(.kco && !is.null(res_vars))
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
#' @param verbose If TRUE, will let you know how long it takes to `collect()` the data. If not provided, defaults to
#' `.verbose` provided to `setup_phea()`, which itself defaults to `TRUE` if not provided.
#' @param .board Optional. Local data frame. Provide this argument together with `board = NULL` to use local data
#' directly, instead of `collect()`ing the `board`.
#' @return Plot created by `plotly::plot_ly()` within `plotly::subplot()`.
phea_plot <- function(board, pid, plot_title = NULL, exclude = NULL, verbose = NULL, .board = NULL) {
  # If not provided, use global default set by setup_phea().
  if(is.null(verbose))
    verbose <- .pheaglobalenv$verbose
  
  if(!is.null(.board)) {
    board_data <- .board |>
      dplyr::filter(pid == local(pid))
  } else {
    if(verbose)
      cat('Collecting lazy table, ')
    board_data <- board |>
      dplyr::filter(pid == local(pid)) |>
      dplyr::collect()
    if(verbose)
      cat('done. (turn this message off with `verbose` or `.verbose` in setup_phea())\n')
  }
  
  # Plot all columns except some.
  chart_items <- colnames(board_data)
  if(sum(c('row_id', 'pid', 'ts', 'window') %in% colnames(board_data)) > 3) {
    # The board has the base columns of a phenotype result. Remove them.
    chart_items <- setdiff(chart_items, c('row_id', 'pid', 'ts', 'window'))
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

