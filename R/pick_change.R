# Phea ------------------------------------------------------------------------------------------------------------
# "Phenotyping Algebra"
# By Fabrício Kury, 2022
# fab at kury.dev.
#

# Pick row by & keep change of ------------------------------------------------------------------------------------
#' Pick row by window function
#'
#' Pick (keep) the row(s) that contain the frame-wise aggregate value of window function `fn`. If `fn` is not provided,
#' defaults to picking the rows where the value of `by` is minimum (i.e. first row in ascending order). If `pick_max` is
#' `TRUE`, defaults instead to rows where `by` is maximum (i.e. last row in ascending order).
#' 
#' If `val` is provided, keeps the rows where the result of `fn(by)` is equal to `val` (if any).
#' 
#' If `partition` is provided, it is used when calling the window function.
#'
#' @export
#' @param lazy_tbl Lazy table to be filtered.
#' @param by Character. Column(s) to pick rows by.
#' @param partition Character vector. Column name(s) to define the partition.
#' @param pick_max Logical. If `TRUE`, will pick the row with largest `by`, instead of smallest.
#' @param fn Character. Window function to use, with arguments if needed. E.g.: `row_number()`,
#' `perc_dist(0.5)`.
#' @param val Character or numeric. Literal value to compare to result of `.fn`.
#' @return Lazy table with filtered rows.
pick_row_by <- function(lazy_tbl, by, partition = NULL, pick_max = FALSE, fn = NULL, val = NULL) {
  if(is.null(.pheaglobalenv$con))
    stop('Connection must be setup previously with setup_phea().')
  
  if(is.null(fn)) {
    # The default operation is done using `row_number() == 1`/`cume_dist() == 1`, as opposed to `max()`/`min()`, to
    # spare the need to create a new variable (column) to perform the computation, since window functions cannot go in
    # `WHERE` clauses.
    if(pick_max)
      fn <- 'cume_dist()'
    else
      fn <- 'row_number()'
    
    if(is.null(val))
      val <- 1
  }
  
  # Produce the window function
  win_sql <- dbplyr::win_over(
    expr = sql(fn),
    partition = partition,
    order = by,
    con = .pheaglobalenv$con)
  
  # Calculate the window function.
  res <- lazy_tbl |>
    dplyr::mutate(phea_calc_var = win_sql)
  
  if(is.null(val)) {
    # The user provided fn but not val. If fn had _not_ been provided, val would be non-null.
    # This means the user just provided the function name, but no target value. In this case we compare the function
    # result to its input value, .fn(x) == x.
    res <- dplyr::filter(res, !!by == phea_calc_var)
  } else {
    # val and fn are both non-null.
    res <- dplyr::filter(res, phea_calc_var == local(val))
  }
  
  # Remove field created for the computation.
  res <- dplyr::select(res, -phea_calc_var)
  
  res
}

#' Keep change of [a column or SQL expression]
#'
#' Keeps only the rows where the value of `of` changes. `of` can be the name of a column, or any SQL expression valid
#' inside a `SELECT` statement.
#' 
#' If `partition` is provided, changes are limited to within it. If `order` provided, rows are ordered and change is
#' detected according to the specified column or columns. The first row (within a partition or not) is always kept.  
#'
#' @export
#' @param lazy_tbl Lazy table to be filtered.
#' @param of Character vector. Name of column(s) or SQL expression(s). Only rows where the value of `of` changes are
#' kept. If multiple columns or expressions are provided, a change in any of them causes the row to be in the output.
#' @param partition Character. Optional. Variable or variables to define the partition.
#' @param order Character. Optional. If provided, this or these column(s) will define the ordering of the rows and hence
#' how changes are detected.
#' @return Lazy table with only rows where `of` changes in comparison to the previous row.
keep_change_of <- function(lazy_tbl, of, partition = NULL, order = NULL) {
  if(is.null(.pheaglobalenv$con))
    stop('Connection must be setup previously with setup_phea().')
  con <- .pheaglobalenv$con
  
  if(!is.null(partition))
    of <- c(partition, of)
  
  of_ids <- DBI::dbQuoteIdentifier(.pheaglobalenv$con, of)
  lag_names <- paste0('phea_kco_lag', seq(of))
  lag_sql <- paste0('lag(', of_ids, ')')
  
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
  # lag_names_ids <- DBI::dbQuoteIdentifier(.pheaglobalenv$con, lag_names)
  # TODO: Revise if I need to call DBI::dbIdentifierQuote() for lag_names and of in commands_c. This conflicts with the
  # use of str2lang().
  commands_c <- paste0('(is.na(', lag_names, ') && !is.na(', of, ')) || ', lag_names, ' != ', of) |>
    paste0(collapse = ' || ') |>
    str2lang()
    
  lazy_tbl |>
    mutate(!!!commands_b) |>
    filter(!!commands_c) |>
    select(-all_of(lag_names))
}


