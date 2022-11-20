# phea.R ----------------------------------------------------------------------------------------------------------
#' @title phe2a
#' ---
#' PHEnotyping Algebra
#' By Fabrício Kury, fab at kury.dev.
#' 2022-11-17 19:07 EST
#'

.pkgglobalenv <- new.env(parent=emptyenv())


# Functions -------------------------------------------------------------------------------------------------------
# Declare function colnames_dbplyr(records).
colnames_dbplyr <- function(records) {
  records |>
    head(1) |> # Coletar 1 line.
    collect() |>
    colnames()
}

#' @export
setup_phea <- function(dbi_connection, schema) {
  assign('con', dbi_connection, envir = .pkgglobalenv)
  assign('schema', schema, envir = .pkgglobalenv)
}

#' @export
sqlt <- function(table) {
  tbl(.pkgglobalenv$con,
    in_schema(.pkgglobalenv$schema,
      deparse(substitute(table))))
}

#' @export
sql0 <- function(...) {
  sql_txt <- paste0(...)
  tbl(.pkgglobalenv$con,
    sql(sql_txt))
}

#' @export
make_component <- function(rec_source, line = NA, delay = NA, window = Inf, rec_name = NA) {
  component <- list()

  if(isTRUE(attr(rec_source, 'phea') == 'component')) {
    # rec_source is actually a component.
    old_component <- rec_source
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
    if(isTRUE(attr(rec_source, 'phea') == 'record_source')) {
      component$rec_source <- rec_source
    } else {
      if('tbl_lazy' %in% class(rec_source)) {
        # Assume result from formula. Read its value column.
        new_rec_source <- make_record_source(
          records = rec_source,
          rec_name = 'value',
          ts = ts,
          pid = pid)
        component$rec_source <- new_rec_source
      } else {
        component$rec_source <- rec_source
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

#' make_record_source
#'
#' This function...
#'
#' @param records List of records.
#' @param rec_name Record name.
#' @return Record source..
#' @export
make_record_source <- function(records, rec_name, ts, pid, vars = NULL, .capture_col = NULL) {
  rec_source <- list()

  rec_source$records <- records

  if(class(substitute(rec_name)) == 'name') {
    rec_name_name <- deparse(substitute(rec_name))
    rec_source$rec_name <- rec_name_name
    rec_source$type <- 'column'
    if(class(substitute(rec_name)) == 'name')
      rec_source$capture_col <- deparse(substitute(.capture_col))
    else
      rec_source$capture_col <- rec_name_name

    if(is.null(vars)) {
      vars <- records |>
        select(!!rec_name_name) |> distinct() |> collect() |> pull()
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


# Calculate formula -----------------------------------------------------------------------------------------------

#' Calculate formula
#'
#' This function gathers records according to their timestamps and computes a SQL formula.
#'
#' @param components List of components,  component, record source, or lazy table.
#' @param fml Formula or list of formulas.
#' @param export List of additional variables to export.
#' @param add_components Additional components to add in case components is not a list of components.
#' @return Lazy table with result of formula or formulas.
#' @export
#' @examples
#' teste
calculate_formula <- function(components, fml = NULL, window = NA, export = NULL, add_components = NULL,
  .ts = NULL, .pid = NULL, .rec_name = NULL, .delay = NULL, .line = NULL,
  # These are used to provide a lazy table directly, instead of components.
  .ordem = NULL, .exclui_na = FALSE, .require_all = FALSE, .lim = NA, .dont_require = NULL,
  .cascaded = FALSE) {
  # Prepare ---------------------------------------------------------------------------------------------------------
  if('tbl_lazy' %in% class(components)) {
    #' components is actually a lazy table. Make a component out of it. The function make_component() is also overloaded
    #' and will produce a record source from the lazy table.
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
  rec_source_names <- map(components, ~.$rec_source$rec_name) |> unlist()
  rec_source_mask <- !duplicated(rec_source_names)
  record_sources <- map(components[rec_source_mask], ~.$rec_source)

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
    g_vars <- str_match_all(fml, '([A-z][A-z0-9_]+)') |> reduce(rbind)
    g_vars <- g_vars[,2]

    #' Filter bogus matches (eg. SQL keywords in the formula) by keeping only the variables that can possibly come from
    #' the given combination of record sources and components.
    out_g_vars_mask <- map(record_sources, \(record_source) {
      vars <- record_source$vars
      comp_mask <- map(components, ~.$rec_source$rec_name) == record_source$rec_name
      related_components <- names(components)[comp_mask]
      pairs <- cross2(related_components, vars) # Create all combinations of related components and rec_source's vars.
      out_vars <- map(pairs, \(x) paste0(x[[1]], '_', x[[2]])) |> unlist()
      out_vars_mask <- g_vars %in% out_vars
      return(out_vars_mask)
    }) |> reduce(`|`) # Do element-wise OR operation. Combine with OR.

    g_vars <- g_vars[out_g_vars_mask]
  } else {
    # If no formula, no variables to export from it.
    g_vars <- NULL
  }

  # Add variables requested to export.
  g_vars <- c(g_vars, export)

  # calc_suffix is used for the temporary columns required to circumvent the lack of IGNORE NULLs.
  calc_suffix <- 'calc'

  # Create board from components ----------------------------------------------------------------------------------
  prepare_record_source <- function(record_source) {
    ts <- record_source$ts
    rec_name <- record_source$rec_name
    rec_pid <- record_source$rec_pid
    rec_type <- record_source$type

    # Normalize the column names.
    if(rec_type == 'column') {
      capture_col <- record_source$capture_col
      sql_txt <- paste0("concat('", rec_name, "_', \"", rec_name, '")')
      export_records <- transmute(record_source$records,
        name = sql(sql_txt),
        pid = !!sym(rec_pid),
        ts = !!sym(ts),
        capture_col = !!sym(capture_col))
    } else {
      export_records <- mutate(record_source$records,
        name = local(rec_name),
        pid = !!sym(rec_pid),
        ts = !!sym(ts))

      #' Select only the columns that will be needed later, according to g_vars. This requires checking all applicable
      #' components.
      vars <- record_source$vars
      comp_mask <- map(components, ~.$rec_source$rec_name) == rec_name
      related_components <- names(components)[comp_mask]
      pairs <- cross2(related_components, vars) # Create all combinations of related components and rec_source's vars.
      pairs_mask <- map(pairs, \(x) paste0(x[[1]], '_', x[[2]]) %in% g_vars) |> unlist()

      if(any(pairs_mask)) {
        out_vars <- pairs[unlist(pairs_mask)] |>
          map(~.[[2]]) |> unlist()

        export_records <- export_records |>
          select(name, pid, ts,
            all_of(out_vars))
      } else {
        export_records <- export_records |>
          select(name, pid, ts)
      }
    }

    return(export_records)
  }

  board <- map(record_sources, prepare_record_source) |>
    reduce(union_all)

  # Criar colunas dos components -----------------------------------------------------------------------------------
  # Primeiro, gerar os comandos.
  produce_component <- function(comp_name, component) {
    rec_name <- component$rec_source$rec_name

    delay <- component$delay
    line <- component$line
    comp_window <- component$comp_window

    # Locate the record source, so we know what variables to extract.
    rec_mask <- map(record_sources, ~.$rec_name) == rec_name
    rec_vars <- map(record_sources[rec_mask], \(rec_source) {
      if(rec_source$type == 'direct')
        return(rec_source$vars)
      else {
        rec_vars <- rec_source$records |>
          select(!!sym(rec_source$rec_name)) |>
          distinct() |>
          pull()
        rec_vars <- paste0(rec_source$rec_name, '_', rec_vars)
        return(rec_vars)
      }
    })

    if(component$rec_source$type == 'direct') {
      vars <- c(rec_vars, 'ts')
      ts_var <- paste0(comp_name, '_ts')
      vars_mask <- paste0(comp_name, '_', vars) %in% c(g_vars, ts_var)
      vars <- vars[vars_mask]
      colunas <- paste0(comp_name, '_', vars, '_', calc_suffix)
    } else {
      vars <- unlist(rec_vars)
      ts_vars <- paste0(vars, '_ts')
      vars <- c(vars, ts_vars)
      vars_mask <- vars %in% g_vars
      vars <- vars[vars_mask]
      colunas <- paste0(vars, '_', calc_suffix)
    }

    #' Por algum motivo, o "lag()" não funciona com "range between window preceeding and delay preceeding", i.e. não é
    #' possível aplicar line *e* delay em uma só chamada à função de window. O last_value() funciona com esse "range".
    if(component$rec_source$type == 'column') {
      capture_col <- component$rec_source$capture_col
      vars_sql <- paste0('case when "name" = \'', vars, '\' then "capture_col" else null end')
    }
    else
      vars_sql <- paste0('case when "name" = \'', rec_name, '\' then "', vars, '" else null end')

    over_clause <- paste0('partition by "pid" order by "ts"') #paste0('partition by "pid", "name" order by "ts"')

    #' Dar a preferência ao acesso via *line*.
    if(!is.na(line)) {
      # TODO: Test if last_value() + find_last_ignore_nulls() has better performance than just find_last_ignore_nulls().
      sql_txts <- paste0('last_value(', vars_sql, ') over (', over_clause,
        ' rows between unbounded preceding and ', line, ' preceding)')
    } else {
      #' Caso contrário, produzir acesso via *delay*.
      range_start_clause <- ifelse(comp_window == Inf,
        'range between unbounded preceding',
        paste0('range between \'', comp_window, '\'::interval preceding'))

      sql_txts <- paste0('last_value(', vars_sql, ') over (', over_clause, ' ',
        range_start_clause, ' and \'', delay, '\'::interval preceding', ')')
    }

    comandos <- map2(colunas, sql_txts,
      ~rlang::exprs(!!..1 := sql(!!..2))) |>
      unlist()

    return(comandos)
  }

  commands <- map2(names(components), components, produce_component) |> unlist()

  # Então, aplicá-los na board.
  board <- transmute(board,
    pid, ts,
    row_id = sql('row_number() over ()'),
    !!!commands)

  # Preencher os valores em branco. ---------------------------------------------------------------------------------
  #' Neste ponto o componente já foi adicionado à board de valores, porém cada componente só tem valor em sua line de
  #' origem. Se o PostgreSQL suportasse IGNORE NULLS no last_value(), o trabalho terminaria aqui. Como ele não
  #' suporta, precisamos contornar o problema.
  extract_vars <- function(comp_name, component) {
    rec_name <- component$rec_source$rec_name
    # Locate the record source, so we know what variables to extract.
    rec_mask <- map(record_sources, ~.$rec_name) == rec_name
    rec_vars <- map(record_sources[rec_mask], \(rec_source) {
      if(rec_source$type == 'direct')
        return(rec_source$vars)
      else {
        rec_vars <- rec_source$records |>
          select(!!sym(rec_source$rec_name)) |>
          distinct() |>
          pull()
        rec_vars <- paste0(rec_source$rec_name, '_', rec_vars)
        return(rec_vars)
      }
    })

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

  extract_ts_variables <- function(i) {
    component <- components[[i]]
    if(component$rec_source$type == 'direct') {
      return(paste0(names(components)[i], '_ts'))
    } else {
      rec_vars <- extract_vars(names(components)[i], component)
      return(paste0(rec_vars, '_ts'))
    }
  }

  ts_variables <- map(seq(components), extract_ts_variables) |> unlist()
# browser()
  calc_vars <- unique(c(g_vars, ts_variables))
  sql_txts <- paste0('find_last_ignore_nulls("', calc_vars, '_', calc_suffix, '") OVER ',
    '(PARTITION BY "pid" ORDER BY "ts" ROWS UNBOUNDED PRECEDING)')

  comandos <- map2(calc_vars, sql_txts,
    ~rlang::exprs(!!.x := sql(!!.y))) |>
    unlist()

  #' The front (most recent point) of the window is column ts of the current line. The back (oldest point) is the
  #' smallest among the ts's of the components.
  sql_datahora_min <- paste0('least(', paste0(ts_variables, collapse = ', '), ')')

  # Apply the commands.
  #' The use of transmute + mutate + select(-...) is to keep the query "tight." By "tight" I mean no use of select *,
  #' that is, every step of the process passes forward only strictly what the next step needs. The hope is that this
  #' will help the SQL query optimizer and computation engine as a whole.
  board <- board |>
    transmute(
      row_id, pid, ts,
      !!!comandos,
      window = ts - sql(sql_datahora_min),
      ts_row = sql('last_value(row_id) over (partition by "pid", "ts")'))

  #' Keep only the most complete computation in each timestamp. The most recent computation is the last one in each
  #' timestamp. 'max(row_id) over (partition by "pid", "ts")' could find the row with the largest (most complete) row_id
  #' in each timestamp, but last_value() in this context gives the same result
  #' We also need to potentially require all fields be filled. Let's compact that into a single call to filter(), hence
  #' a single WHERE statement.
  if(.require_all || !is.null(.dont_require)) {
    #' If .dont_require is provided, then all components, except those specified, will be required, even if .require_all
    #' is FALSE.
    required_components <- setdiff(names(components), .dont_require)
    if(length(required_components) > 0) {
      sql_txt <- paste0(required_components, '_ts is not null') |> paste(collapse = ' and ')
      board <- filter(board,
        row_id == ts_row && sql(sql_txt))
    } else {
      # No required components after all, because all were excluded by .dont_require.
      board <- filter(board,
        row_id == ts_row)
    }
  } else {
    board <- board |>
      filter(row_id == ts_row)
  }

  # Clean temporary variables used for computing the formula.
  board <- board |>
    select(
      -ts_row,
      -any_of(setdiff(calc_vars, g_vars)))

  # Filtrar e calcular ----------------------------------------------------------------------------------------------
  # Impose the time window, if any.
  if(!is.na(window))
    board <- filter(board,
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
        board <- mutate(board,
          !!sym(names(fml)[i]) := sql(sql_txt))
      }
    } else {
      # Compute them all in one statement, so that computation time is (potentially, haven't tested) minimized.
      commands <- map2(names(fml), fml,
        ~rlang::exprs(!!..1 := sql(!!..2))) |>
        unlist()

      board <- mutate(board,
        !!!commands)
    }
  }

  # Requerer que o resultado exista.
  if(.exclui_na)
    board <- filter(board,
      !is.na(!!sym(.out_var)))

  # Colapsar e retornar -------------------------------------------------------------------------------------------
  # Chamar o collapse() é necessário para evitar acúmulo de código lazy com eventual erro "C stack usage is too close to
  # the limit". Vide https://github.com/tidyverse/dbplyr/issues/719. Obrigado, mgirlich!
  board <- collapse(board, cte = TRUE)

  board
}

