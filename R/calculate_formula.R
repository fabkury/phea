# Phea ------------------------------------------------------------------------------------------------------------
# "Phenotyping Algebra"
# By Fabrício Kury, 2022
# fab at kury.dev.
#


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
#' @param .ts,.pid,delay,line If supplied, these will overwrite those of the given component.
#' @param require_all If `TRUE`, returns only rows where all components to have been found according to their
#'   timestamps. If the timestamp is not null, the component is cosidered present even if its other values are null. If 
#'   `dont_require` is provided, `require_all` is ignored.
#' @param limit Maximum number of rows to return. This is imposed before the calculation of the formula.
#' @param dont_require If provided, causes formula to require all components (regardless of require_all), except for
#'   those listed here.
#' @param cascaded If `TRUE` (default), each formula is computed in a separate, nested SELECT statement. This allows
#'   the result of the prior formula to be used in the following, at the potential cost of longer computation times.
#' @param clip_sql If `TRUE`, instead of a lazy table the return value is the code of the SQL query, and also copies it
#'   to the clipboard.  
#' @param filters Character vector. Logical conditions to satisfy. Only rows satisfying all conditions provided will be
#'   returned. These go into the SQL `WHERE` clause.   
#' @param out_window Character vector. Names of components to *not* be included when calculating the window.
#' @param dates Tibble. Column names must be `pid` (person ID) and `ts` (timestamp). If provided, these dates (for each
#' person ID) are added to the board, so that the phenotype computation can be attempted at those times.
#' @param kco Logical. "Keep change of". This is a shorthand to call `keep_change_of()` after computing the phenotype.
#' If `TRUE` (default), output will include only rows where the result of any of the formulas change. If `FALSE`,
#' `keep_change_of()` is not called and therefore all dates from every component will be present. This argument can
#' alternatively be a character vector of names of columns and/or SQL expressions, in which case `calculate_formula()`
#' will return only rows where the value of those columns or expressions change.
#' @return Lazy table with result of formula or formulas.
calculate_formula <- function(components, fml = NULL, window = NULL, export = NULL, add_components = NULL,
  require_all = FALSE, limit = NA, dont_require = NULL, filters = NULL, cascaded = TRUE, get_sql = FALSE,
  out_window = NULL, dates = NULL, kco = FALSE, dates_from = NULL,
  .ts = NULL, .pid = NULL, line = NULL, delay = NULL, component_window = NULL, ahead = NULL, up_to = NULL) {
  # Prepare ---------------------------------------------------------------------------------------------------------
  # TODO: Improve the logic regarding these two variables below.
  keep_names_unchanged <- FALSE
  input_is_phenotype <- FALSE
  #
  
  # If TRUE, will add extra column to filter dates.
  filtering_dates <- !is.null(dates_from)
  
  # This is just to make code easier to read.
  dbQuoteId <- function(x) DBI::dbQuoteIdentifier(.pheaglobalenv$con, x)
  dbQuoteStr <- function(x) DBI::dbQuoteString(.pheaglobalenv$con, x)
  has_content <- function(x) isFALSE(is.na(x))
  
  # Parameter overload ----------------------------------------------------------------------------------------------
  if(isTRUE(attr(components, 'phea') == 'phenotype')) {
    keep_names_unchanged <- TRUE
    input_is_phenotype <- TRUE
    
    res_vars <- attr(components, 'phea_res_vars')
    
    new_component <- make_component(
      input_source = components,
      .ts = ts,
      .pid = pid)
    
    # Overwrite if provided.
    ts_name <- deparse(substitute(.ts))
    if(ts_name != 'NULL')
      new_component$rec_source$ts <- ts_name
    
    pid_name <- deparse(substitute(.pid))
    if(pid_name != 'NULL')
      new_component$rec_source$pid <- pid_name
    
    # TODO: Move this to make_component()
    if(!is.null(line)) new_component$line <- line
    if(!is.null(delay)) new_component$delay <- delay
    if(!is.null(component_window)) new_component$window <- component_window
    if(!is.null(ahead)) new_component$ahead <- ahead
    if(!is.null(up_to)) new_component$up_to <- up_to
    
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
    if(component$passthrough || keep_names_unchanged)
      composed_named <- component$columns
    else
      composed_name <- paste0(comp_name, '_', component$columns)
    
    res <- dplyr::tibble(
      component_name = comp_name,
      rec_name = component$rec_source$rec_name,
      column = component$columns,
      composed_name = composed_name,
      access_sql = component$access_sql)
    # , pick = !(is.null(component$pick_by) || is.na(component$pick_by) || component$pick_by == '')
    # , pick_by = component$pick_by)
    
    # if(filtering_dates)
    #   res <- mutate(res, date_out = comp_name %in% dates_from)
    
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
  if(!is.null(filters)) {
    # Extract components from filters.
    filter_vars <- unlist(filters) |>
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
    
    # if(filtering_dates) {
    #   # Are the dates from this record source being exported?
    #   date_out <- any(var_map[var_map$rec_name == rec_name,]$date_out)
    #   
    #   export_records <- record_source$records |>
    #     dplyr::transmute(
    #       name = local(rec_name),
    #       pid = !!rlang::sym(record_source$pid),
    #       ts = !!rlang::sym(record_source$ts),
    #       phea_date_out = local(date_out),
    #       !!!rlang::syms(out_vars))
    # } else {
      export_records <- record_source$records |>
        dplyr::transmute(
          name = local(rec_name),
          pid = !!rlang::sym(record_source$pid),
          ts = !!rlang::sym(record_source$ts),
          !!!rlang::syms(out_vars))
    # }
    
    return(export_records)
  }
  
  board <- record_sources |>
    purrr::map(prepare_record_source) |>
    purrr::reduce(dplyr::union_all)
  
  # Add extra dates -------------------------------------------------------------------------------------------------
  if(!is.null(dates)) {
    message('Warning: dates is yet to be properly tested.')
    dates_table <- dbplyr::copy_inline(.pheaglobalenv$con, dates)
    board <- dplyr::union_all(board, dates_table)
  }
  
  # Apply components ------------------------------------------------------------------------------------------------
  ## First, generate the commands.
  commands <- purrr::map2(var_map$composed_name, var_map$access_sql,
    ~rlang::exprs(!!..1 := !!dplyr::sql(..2))) |>
    unique() |>
    unlist(recursive = FALSE)
  # The unique() above is just in case, but is it needed? Seems like the only way there could be duplicates is if the
  # same component gets added twice to the call to calculate_formula(). 
  
  ## Second, apply commands.
  phea_row_id_sql_txt <- paste0('row_number() over (order by ', dbQuoteId('pid'), ', ', dbQuoteId('ts'), ')')
  
  if(F) {
    # if(any(var_map$pick)) {
    #   var_map_picks <- var_map[var_map$pick,]
    #   
    #   picks <- purrr::map(seq(nrow(var_map_picks)), \(i) {
    #     phea_col_name <- var_map_picks$composed_name[i]
    #     orig_col_name <- var_map_picks$column[i]
    #     component_name <- var_map_picks$component_name[i]
    #     rec_name <- var_map_picks$rec_name[i]
    #     pick_by <- var_map_picks$pick_by[i]
    #     sql_txt <- paste0('case when ',
    #       dbQuoteId('name'), ' = ', dbQuoteStr(rec_name), ' and ',
    #       dbQuoteId(pick_by), ' = ', dbQuoteId(paste0(component_name, '_', pick_by)),
    #       ' then ', dbQuoteId(orig_col_name), ' else NULL end')
    #     rlang::exprs(!!phea_col_name := !!dplyr::sql(sql_txt))
    #   }) |>
    #     unique() |>
    #     unlist(recursive = FALSE)
    # # }
    # 
    # # pick_sql <- list(
    # #   x = components,
    # #   y = names(components)) |>
    # #   purrr::pmap(\(x, y) {
    # #     if(x$pick) {
    # #       browser()
    # #       variables_to_hide <- 
    # #       return(paste0('case when name != \'', x$rec_source$rec_name,
    # #         '\' then true else ', y, '_', x$pick_by, ' = ', x$pick_by, ' end'))
    # #     } else
    # #       return(NULL)
    # #   }) |>
    # #   purrr::discard(is.null) |>
    # #   paste0(collapse = ' and ')
    # 
    # # if(pick_sql != '') {
    #   # Apply commands, filter, then drop unneded columns.
    #   board <- dplyr::mutate(board,
    #     phea_row_id = dplyr::sql(phea_row_id_sql_txt),
    #     !!!commands)
    #   
    #   browser()
    #   board <- filter(board, sql(pick_sql))
    #   
    #   commands_names <- names(commands)
    #   if(filtering_dates) {
    #     board <- dplyr::select(board,
    #       phea_row_id, pid, ts, phea_date_out, all_of(commands_names))
    #   } else {
    #     board <- dplyr::transmute(board,
    #       phea_row_id, pid, ts, all_of(commands_names))
    #   }
    # } else {
  }
  
  # Apply commands to the board all at once, so we only generate a single layer of "SELECT ... FROM (SELECT ...)".
  if(filtering_dates) {
    board <- dplyr::transmute(board,
      phea_row_id = dplyr::sql(phea_row_id_sql_txt),
      pid, ts, name,
      !!!commands)
  } else {
    board <- dplyr::transmute(board,
      phea_row_id = dplyr::sql(phea_row_id_sql_txt),
      pid, ts,
      !!!commands)
  }
  # }
  
  # browser()
  ## Third and final, fill the blanks downward with the last non-blank value, within the patient.
  board <- board |>
    dbplyr::window_order(pid, ts) |>
    dplyr::group_by(pid) |>
    tidyr::fill(!any_of(c('phea_row_id', 'pid', 'ts', 'name'))) |>
    ungroup()
  
  # For some reason, apparently a bug in dbplyr's SQL translation, we need to "erase" an ORDER BY "pid", "ts" that is
  # left over in the translated query. That ORDER BY persists even if you posteriorly do a dplyr::group_by() on the
  # result of the phenotype (i.e. the board at this point). This causes the SQL server's query engine to raise an error,
  # saying that "ts" must also be part of the GROUP BY. This left over ORDER BY "pid", "ts" apparently comes from the
  # dbplyr::window_order() call that was necessary to guarantee the intended behavior of the call to
  # tidyr::fill.lazy_tbl() above.
  board <- board |>
    arrange()
  
  # dates_from ------------------------------------------------------------------------------------------------------
  if(filtering_dates) {
    # Obtain `rec_name`s from the record sources of the target components.
    rec_names <- unique(var_map[var_map$component_name %in% dates_from,]$rec_name)

    sql_txt <- paste0(dbQuoteId('name'), ' in (', paste0(dbQuoteStr(rec_names), collapse = ', '), ')')
    
    board <- board |>
      filter(sql(sql_txt))
  }
  
  # Compute window --------------------------------------------------------------------------------------------------
  window_components <- setdiff(var_map$component_name, out_window)
  if(!input_is_phenotype && length(window_components) > 1) { # Window only makes sense if there is > 1 component.
    window_components_sql <- window_components |>
      unique() |>
      paste0('_ts') |>
      DBI::dbQuoteIdentifier(conn = .pheaglobalenv$con) |>
      paste0(collapse = ', ')
    
    sql_ts_least <- paste0('least(', window_components_sql, ')')
    sql_ts_greatest <- paste0('greatest(', window_components_sql, ')')
  }
  else {
    # TODO: Improve this a bit?
    # If there is only one component, window is zero. But if we just set window = 0, we mess with the data type.
    sql_ts_least <- dbQuoteId('ts')
    sql_ts_greatest <- sql_ts_least
  }
  
  # phea_ts_row is used to pick the best computation within each date. This is for the case when multiple data points
  # exist on the same date. The best computation for each date is the last row within that date.
  # The most complete computation is the last one in each timestamp. 'max(phea_row_id) over (partition by "pid", "ts")'
  # finds the row with the largest (most complete) phea_row_id in each timestamp. last_value() could give the same
  # result, and could be potentially faster (wild assumption) due to optimizations, but that's just an idea.
  sql_txt <- paste0('MAX(', dbQuoteId('phea_row_id'), ') OVER (PARTITION BY ',
    dbQuoteId('pid'), ', ', dbQuoteId('ts'), ')')
  
  board <- board |>
    dplyr::mutate(
      window = dplyr::sql(sql_ts_greatest) - dplyr::sql(sql_ts_least),
      phea_ts_row = dplyr::sql(sql_txt))
  
  # Filter rows -----------------------------------------------------------------------------------------------------
  # We also need to:
  #  - potentially require all fields be filled.
  #  - potentially impose the time window.
  # Let us compact those three things into a single call to dplyr::filter(), in order to produce a single WHERE
  # statement, instead of three layers of SELECT ... WHERE.
  
  if(require_all || !is.null(dont_require)) {
    # If dont_require is provided, then all components, except those specified, will be required, even if require_all
    # is FALSE.
    required_components <- setdiff(names(components), dont_require)
    if(length(required_components) > 0) {
      sql_txt <- required_components |>
        paste0('_ts') |>
        DBI::dbQuoteIdentifier(conn = .pheaglobalenv$con) |>
        paste0(' is not null') |>
        paste(collapse = ' and ')
      
      if(has_content(window)) {
        board <- dplyr::filter(board,
          phea_row_id == phea_ts_row &&
            dplyr::sql(sql_txt) &&
            window < local(window))
      } else {
        board <- dplyr::filter(board,
          phea_row_id == phea_ts_row &&
            dplyr::sql(sql_txt))
      }
    } else {
      # No required components after all, because all were excluded by dont_require. Let's just filter by the most
      # complete computation.
      if(has_content(window)) {
        board <- dplyr::filter(board,
          phea_row_id == phea_ts_row &&
            window < local(window))
      } else {
        board <- dplyr::filter(board,
          phea_row_id == phea_ts_row)
      }
    }
  } else {
    # No need to require all components. Let's just filter by the most complete computation.
    if(has_content(window)) { # This covers case if `window` is NULL
      board <- board |>
        dplyr::filter(phea_row_id == phea_ts_row &&
            window < local(window))
    } else {
      board <- board |>
        dplyr::filter(phea_row_id == phea_ts_row)
    }
  }
  
  # Apply filters, if provided.
  if(!is.null(filters) && any(!is.na(filters))) {
    sql_txt <- paste0('(', paste0(filters[!is.na(filters)], collapse = ') AND ('), ')')
    board <- board |>
      filter(sql(sql_txt))
  }
  
  # Limit number of output rows, if requested.
  if(isFALSE(is.na(limit))) # This covers case if limit = NULL
    board <- board |>
      head(n = lim)
  
  # Calculate formula -----------------------------------------------------------------------------------------------
  # Remove the original columns of the record sources, leaving only those produced by the components.
  board <- board |>
    dplyr::select(phea_row_id, pid, ts, window, !!!g_vars)
  
  # Calculate the formulas, if any.
  res_vars <- NULL
  if(!is.null(fml)) {
    if(cascaded) {
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
      # cascaded is turned off.
      # Let's check if any of the formulas is itself a list, which means cascaded was supposed to be on.
      if(any(lapply(fml, class) == 'list'))
        stop('Nested formulas require cascaded = TRUE.')
      
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
  # Parameter overload: kco can be:
  # - logical: TRUE (apply kco over all result columns) or FALSE.
  # - character vector: Names of columns to apply kco.
  if(class(kco) == 'logical') {
    if(length(kco) > 1)
      stop('If logical, kco must be of length 1.')
    
    # If no res_vars, keep_change_of would collapse to one row per partition.
    if(kco && !is.null(res_vars))
      board <- board |>
        keep_change_of(res_vars, partition = 'pid', order = 'ts')
  } else {
    if(class(kco) != 'character')
      stop('kco must be logical or character vector.')
    
    board <- board |>
      keep_change_of(kco, partition = 'pid', order = 'ts')
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
  
  if(get_sql) {
    return(dbplyr::sql_render(board))
  } else {
    return(board)
  }
}

