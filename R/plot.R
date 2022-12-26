# Phea ------------------------------------------------------------------------------------------------------------
# "Phenotyping Algebra"
# By Fabrício Kury, 2022
# fab at kury.dev.
#

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

