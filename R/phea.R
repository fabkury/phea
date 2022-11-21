# Data ------------------------------------------------------------------------------------------------------------
.pkgglobalenv <- new.env(parent=emptyenv())


# Functions -------------------------------------------------------------------------------------------------------
colnames_dbplyr <- function(records) {
  records |>
    utils::head(1) |> # Coletar 1 line.
    dplyr::collect() |>
    colnames()
}

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

#' Setup Phea
#'
#' Configures Phea for use.
#'
#' Stores the DBI connection for later use, and verifies if Phea's user-defined aggregates are available. If they're
#' not, runs the needed SQL queries to install them.
#'
#' @export
#' @param dbi_connection DBI-compatible SQL connection (e.g. produced by DBI::dbConnect).
#' @param schema Schema to be used by default in `sqlt()`.
setup_phea <- function(dbi_connection, def_schema) {
  assign('con', dbi_connection, envir = .pkgglobalenv)
  assign('schema', def_schema, envir = .pkgglobalenv)

  function_exists <- DBI::dbGetQuery(.pkgglobalenv$con,
    'select * from
      pg_proc p
      join pg_namespace n
      on p.pronamespace = n.oid
      where proname =\'phea_coalesce_r_sfunc\';') |>
    nrow()
  if(function_exists != 1) {
    message('Installing phea_coalesce_r_sfunc.')
    DBI::dbExecute(.pkgglobalenv$con,
      "create function phea_coalesce_r_sfunc(state anyelement, value anyelement)
      returns anyelement
      immutable parallel safe
      as
      $$
        select coalesce(value, state);
      $$ language sql;")
  }

  function_exists <- DBI::dbGetQuery(.pkgglobalenv$con,
    'select * from
      pg_proc p
      join pg_namespace n
      on p.pronamespace = n.oid
      where proname =\'phea_find_last_ignore_nulls\';') |>
    nrow()

  if(function_exists != 1) {
    message('Installing phea_find_last_ignore_nulls.')
    DBI::dbExecute(.pkgglobalenv$con,
      "create aggregate phea_find_last_ignore_nulls(anyelement) (
        sfunc = phea_coalesce_r_sfunc,
        stype = anyelement
      );")
  }
}

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
  dplyr::tbl(.pkgglobalenv$con,
    dbplyr::in_schema(.pkgglobalenv$schema,
      deparse(substitute(table))))
}

#' SQL query
#'
#' Combines with `paste0` the strings in `...`, then runs it as a SQL query.
#'
#' @export
#' @param ... Character string, or vector of.
#' @return Lazy table corresponding to the query.
sql0 <- function(...) {
  sql_txt <- paste0(...)
  dplyr::tbl(.pkgglobalenv$con,
    dplyr::sql(sql_txt))
}

#' Make component
#'
#' Produce a Phea component.
#'
#' Creates a component from the given `input_source` record source and optional parameters. If `input_source` is a
#' record source, it is used. If it is a component, it is copied (including its record source) and other paremeters, if
#' provided, overwrite existing ones.
#'
#' @export
#' @param input_source A record source, a component, or a lazy table.
#' @param line Interger. Which line to pick. 0 = skip no lines, 1 = skip one line, 2 = skip two lines, etc.
#' @param delay Character. Time interval in SQL language. Minimum time difference between phenotype date and component
#' date.
#' @param window Character. Time interval in SQL language. Maximum time difference between phenotype date and component
#' date.
#' @param rec_name Character. If provided, overwrites the `rec_name` of the record source.
#' @return Phea component object.
make_component <- function(input_source, line = NA, delay = NA, window = Inf, rec_name = NA) {
  component <- list()

  if(isTRUE(attr(input_source, 'phea') == 'component')) {
    # rec_source is actually a component.
    old_component <- input_source
    component$rec_source <- old_component$rec_source
    component$line <- old_component$line
    component$delay <- old_component$delay
    component$comp_window <- old_component$window

    # Overwrite if provided.
    if(!is.na(line))
      component$line <- line
    if(!is.na(delay)) {
      component$delay <- delay
      # If the user tries to apply a delay, erase the line if not provided.
      if(is.na(line))
        component$line <- NA
    }
    if(!is.na(window))
      component$comp_window <- window
  } else {
    if(isTRUE(attr(input_source, 'phea') == 'record_source')) {
      component$rec_source <- input_source
    } else {
      if('tbl_lazy' %in% class(input_source)) {
        # Assume result from formula. Read its value column.
        component$rec_source <- make_record_source(
          records = input_source,
          rec_name = 'value',
          ts = ts,
          pid = pid)
      } else {
        component$rec_source <- input_source
      }
    }

    if(is.na(line) && is.na(delay))
      line <- 0
    component$line <- line
    component$delay <- delay
    component$comp_window <- window
  }

  if(!is.na(rec_name))
    component$rec_source$rec_name <- name

  attr(component, 'phea') <- 'component'

  component
}

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
make_record_source <- function(records, rec_name, ts, pid, vars = NULL, .capture_col = NULL) {
  rec_source <- list()

  rec_source$records <- records

  if(class(substitute(rec_name)) == 'name') {
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

  } else {
    rec_source$rec_name <- rec_name
    rec_source$type <- 'direct'

    if(is.null(vars))
      vars <- records |>
      colnames_dbplyr()
  }

  ts_name <- deparse(substitute(ts))
  rec_source$ts <- ts_name


  pid_name <- deparse(substitute(pid))
  vars <- setdiff(vars, pid_name)

  rec_source$vars <- vars
  rec_source$rec_pid <- pid_name
  attr(rec_source, 'phea') <- 'record_source'

  rec_source
}


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
#' @param .exclude_na If `TRUE`, returns only rows where the result of the formula is not NA.
#' @param .require_all If `TRUE`, returns only rows where all components to have been found according to their
#' timestamps (even if their value is NA). If `.dont_require` is provided, `.require_all` is ignored.
#' @param .lim Maximum number of rows to return. This is imposed before the calculation of the formula.
#' @param .dont_require If provided, causes formula to require all components (regardless of .require_all), except for
#' those listed here.
#' @param .cascaded If `TRUE`, each formula is computed in a separate, nested SELECT statement. This allows the result
#' of the prior formula to be used in the following, at the potential cost of longer computation times.
#' @param .clip_sql If `TRUE`, instead of lazy table it returns the SQL query as a SQL object (can be converted to
#' character using `as.character()`), and also copies it to the clipboard.
#' @return Lazy table with result of formula or formulas.
calculate_formula <- function(components, fml = NULL, window = NA, export = NULL, add_components = NULL,
  .ts = NULL, .pid = NULL, .rec_name = NULL, .delay = NULL, .line = NULL,
  .exclude_na = FALSE, .require_all = FALSE, .lim = NA, .dont_require = NULL,
  .cascaded = FALSE, .clip_sql = FALSE) {

  if('tbl_lazy' %in% class(components)) {
    # components is actually a lazy table. Make a component out of it. The function make_component() is also overloaded
    # and will produce a record source from the lazy table.
    new_component <- make_component(components)

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

    components <- list(new_component = new_component)
    names(components) <- new_component$rec_source$rec_name
  }

  if(!is.null(add_components))
    components <- c(components, add_components)

  if(isTRUE(attr(components, 'phea') == 'component'))
    components <- list(components)

  # Build record sources
  rec_source_names <- purrr::map(components, ~.$rec_source$rec_name) |> unlist()
  rec_source_mask <- !duplicated(rec_source_names)
  record_sources <- purrr::map(components[rec_source_mask], ~.$rec_source)

  if(is.null(names(components)) &&
      isTRUE(attr(components[[1]], 'phea') == 'component')) {
    names(components) <- components[[1]]$rec_source$rec_name
  }

  if(!is.null(fml)) {
    # Check if user provided names in fml.
    # If not, use default name 'value'.
    if(is.null(names(fml))) {
      # Apply a number in front of the default names, except for the first.
      char_numbers <- as.character(1:length(fml))
      char_numbers[char_numbers == '1'] <- ''
      names(fml) <- paste0('value', char_numbers)
    }

    # Extract used components from formula.
    g_vars <- stringr::str_match_all(fml, '([A-z][A-z0-9_]+)') |> purrr::reduce(rbind)
    g_vars <- g_vars[,2]

    # Filter bogus matches (eg. SQL keywords in the formula) by keeping only the variables that can possibly come from
    # the given combination of record sources and components.
    out_g_vars_mask <- purrr::map(record_sources, \(record_source) {
      vars <- record_source$vars
      comp_mask <- purrr::map(components, ~.$rec_source$rec_name) == record_source$rec_name
      related_components <- names(components)[comp_mask]
      pairs <- purrr::cross2(related_components, vars) # Create all combinations of components and vars.
      out_vars <- purrr::map(pairs, \(x) paste0(x[[1]], '_', x[[2]])) |> unlist()
      out_vars_mask <- g_vars %in% out_vars
      return(out_vars_mask)
    }) |> purrr::reduce(`|`) # Do element-wise OR operation. Combine with OR.

    g_vars <- g_vars[out_g_vars_mask]
  } else {
    # If no formula, no variables to export from it.
    g_vars <- NULL
  }

  # Add variables requested to export.
  g_vars <- c(g_vars, export)

  # calc_suffix is used for the temporary columns required to circumvent the lack of IGNORE NULLs.
  calc_suffix <- 'calc'

  prepare_record_source <- function(record_source) {
    ts <- record_source$ts
    rec_name <- record_source$rec_name
    rec_pid <- record_source$rec_pid
    rec_type <- record_source$type

    # Normalize the column names.
    if(rec_type == 'column') {
      capture_col <- record_source$capture_col
      sql_txt <- paste0("concat('", rec_name, "_', \"", rec_name, '")')
      export_records <- dplyr::transmute(record_source$records,
        name = dplyr::sql(sql_txt),
        pid = !!rlang::sym(rec_pid),
        ts = !!rlang::sym(ts),
        capture_col = !!rlang::sym(capture_col))
    } else {
      export_records <- dplyr::mutate(record_source$records,
        name = local(rec_name),
        pid = !!rlang::sym(rec_pid),
        ts = !!rlang::sym(ts))

      # Select only the columns that will be needed later, according to g_vars. This requires checking all applicable
      # components.
      vars <- record_source$vars
      comp_mask <- purrr::map(components, ~.$rec_source$rec_name) == rec_name
      related_components <- names(components)[comp_mask]
      pairs <- purrr::cross2(related_components, vars) # Create all combinations of components and vars.
      pairs_mask <- purrr::map(pairs, \(x) paste0(x[[1]], '_', x[[2]]) %in% g_vars) |> unlist()

      if(any(pairs_mask)) {
        out_vars <- pairs[unlist(pairs_mask)] |>
          purrr::map(~.[[2]]) |> unlist()

        export_records <- export_records |>
          dplyr::select(name, pid, ts,
            all_of(out_vars))
      } else {
        export_records <- export_records |>
          dplyr::select(name, pid, ts)
      }
    }

    return(export_records)
  }

  board <- purrr::map(record_sources, prepare_record_source) |>
    purrr::reduce(dplyr::union_all)

  # Criar colunas dos components -----------------------------------------------------------------------------------
  extract_vars <- function(comp_name, component) {
    rec_name <- component$rec_source$rec_name
    # Locate the record source, so we know what variables to extract.
    rec_mask <- purrr::map(record_sources, ~.$rec_name) == rec_name
    rec_vars <- purrr::map(record_sources[rec_mask], \(rec_source) {
      if(rec_source$type == 'direct')
        return(rec_source$vars)
      else {
        rec_vars <- rec_source$records |>
          dplyr::select(!!rlang::sym(rec_source$rec_name)) |>
          dplyr::distinct() |>
          dplyr::pull()
        rec_vars <- paste0(rec_source$rec_name, '_', rec_vars)
        return(rec_vars)
      }
    }) |> unlist()

    if(component$rec_source$type == 'direct') {
      vars <- c(rec_vars, 'ts')
      ts_var <- paste0(comp_name, '_ts')
      vars_mask <- paste0(comp_name, '_', vars) %in% c(g_vars, ts_var)
    } else {
      vars <- unlist(rec_vars)
      vars_mask <- vars %in% g_vars
    }
    vars[vars_mask]
  }

  produce_component <- function(comp_name, component) {
    rec_name <- component$rec_source$rec_name

    delay <- component$delay
    line <- component$line
    comp_window <- component$comp_window

    vars <- extract_vars(comp_name, component)

    if(component$rec_source$type == 'direct')
      colunas <- paste0(comp_name, '_', vars, '_', calc_suffix)
    else
      colunas <- paste0(vars, '_', calc_suffix)

    # Por algum motivo, o "lag()" não funciona com "range between window preceeding and delay preceeding", i.e. não é
    # possível aplicar line *e* delay em uma só chamada à função de window. O last_value() funciona com esse "range".
    if(component$rec_source$type == 'column') {
      capture_col <- component$rec_source$capture_col
      vars_sql <- paste0('case when "name" = \'', vars, '\' then "capture_col" else null end')
    }
    else
      vars_sql <- paste0('case when "name" = \'', rec_name, '\' then "', vars, '" else null end')

    over_clause <- paste0('partition by "pid", "name" order by "ts"') #paste0('partition by "pid", "name" order by "ts"')

    # Dar a preferência ao acesso via *line*.
    if(!is.na(line)) {
      # TODO: Test if last_value() + find_last_ignore_nulls() has better performance than just find_last_ignore_nulls().
      sql_txts <- paste0('last_value(', vars_sql, ') over (', over_clause,
        ' rows between unbounded preceding and ', line, ' preceding)')
    } else {
      # Caso contrário, produzir acesso via *delay*.
      range_start_clause <- ifelse(comp_window == Inf,
        'range between unbounded preceding',
        paste0('range between \'', comp_window, '\'::interval preceding'))

      sql_txts <- paste0('last_value(', vars_sql, ') over (', over_clause, ' ',
        range_start_clause, ' and \'', delay, '\'::interval preceding', ')')
    }

    comandos <- purrr::map2(colunas, sql_txts,
      ~rlang::exprs(!!..1 := dplyr::sql(!!..2))) |>
      unlist()

    return(comandos)
  }

  # First, generate commands.
  commands <- purrr::map2(names(components), components, produce_component) |> unlist()

  # Then, apply on the board.
  board <- dplyr::transmute(board,
    pid, ts,
    row_id = dplyr::sql('row_number() over ()'),
    !!!commands)

  # Preencher os valores em branco. ---------------------------------------------------------------------------------
  # Neste ponto o componente já foi adicionado à board de valores, porém cada componente só tem valor em sua line de
  # origem. Se o PostgreSQL suportasse IGNORE NULLS no last_value(), o trabalho terminaria aqui. Como ele não
  # suporta, precisamos contornar o problema.
  extract_ts_variables <- function(i) {
    component <- components[[i]]
    if(component$rec_source$type == 'direct') {
      return(paste0(names(components)[i], '_ts'))
    } else {
      rec_vars <- extract_vars(names(components)[i], component)
      return(paste0(rec_vars, '_ts'))
    }
  }

  ts_variables <- purrr::map(seq(components), extract_ts_variables) |> unlist()

  calc_vars <- unique(c(g_vars, ts_variables))
  sql_txts <- paste0('phea_find_last_ignore_nulls("', calc_vars, '_', calc_suffix, '") over ',
    '(partition by "pid" order by "ts" rows unbounded preceding)')

  commands <- purrr::map2(calc_vars, sql_txts,
    ~rlang::exprs(!!.x := dplyr::sql(!!.y))) |>
    unlist()

  # The front (most recent point) of the window is column ts of the current line. The back (oldest point) is the
  # smallest among the ts's of the components.
  sql_ts_least <- paste0('least(', paste0(ts_variables, collapse = ', '), ')')

  # Apply the commands.
  # The use of transmute + mutate + select(-...) is to keep the query "tight." By "tight" I mean no use of select *,
  # that is, every step of the process passes forward only strictly what the next step needs. The hope is that this
  # will help the SQL query optimizer and computation engine as a whole.
  board <- board |>
    dplyr::transmute(
      row_id, pid, ts,
      !!!commands,
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

  # Clean temporary variables used for computing the formula.
  board <- board |>
    dplyr::select(
      -ts_row,
      -dplyr::any_of(setdiff(calc_vars, g_vars)))

  # Filtrar e calcular ----------------------------------------------------------------------------------------------
  # Impose the time window, if any.
  if(!is.na(window))
    board <- dplyr::filter(board,
      window < local(window))

  # Limit rows.
  if(!is.na(.lim))
    board <- board |>
      head(n = lim)

  # Calculate the formula, if any.
  if(!is.null(fml)) {
    if(.cascaded) {
      # Compute one at a time, so that the prior result can be used in the next formula.
      for(i in seq(fml)) {
        sql_txt <- fml[[i]]
        board <- dplyr::mutate(board,
          !!rlang::sym(names(fml)[i]) := dplyr::sql(sql_txt))
      }
    } else {
      # Compute them all in one statement, so that computation time is (potentially, haven't tested) minimized.
      commands <- purrr::map2(names(fml), fml,
        ~rlang::exprs(!!..1 := dplyr::sql(!!..2))) |>
        unlist()

      board <- dplyr::mutate(board,
        !!!commands)
    }
  }

  # Require result to exist.
  if(.exclude_na)
    board <- dplyr::filter(board,
      !is.na(!!rlang::sym(.out_var)))

  # Colapsar e retornar -------------------------------------------------------------------------------------------
  # Chamar o collapse() é necessário para evitar acúmulo de código lazy com eventual erro "C stack usage is too close to
  # the limit". Vide https://github.com/tidyverse/dbplyr/issues/719. Obrigado, mgirlich!
  board <- dplyr::collapse(board, cte = TRUE)

  if(.clip_sql) {
    sql_txt <- dbplyr::sql_render(board)
    writeClipboard(sql_txt)
    return(invisible(sql_txt))
  } else {
    return(board)
  }
}

