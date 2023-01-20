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
#' @param titles Logical, or list of named characters. If `FALSE`, each chart won't have its individual title.
#' Otherwise, pass named characters to override the titles. Example:
#' `list(platelet_value_as_number = 'Platelet count')`.
#' directly, instead of `collect()`ing the `board`.
#' @return Plot created by `plotly::plot_ly()` within `plotly::subplot()`.
phea_plot <- function(board, pid, plot_title = NULL, exclude = NULL, verbose = NULL, .board = NULL,
  titles = NULL, titles_font_size = 11, modes = NULL) {
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
  
  if(sum(c('phea_row_id', 'pid', 'ts', 'window') %in% colnames(board_data)) > 2) {
    # The board has the base columns of a phenotype result. Remove them.
    chart_items <- setdiff(chart_items, c('phea_row_id', 'pid', 'ts', 'window'))
  }
  
  if(!is.null(exclude))
    chart_items <- setdiff(chart_items, exclude)
  
  chart_items <- sort(chart_items)
  
  make_chart <- function(chart_item, chart_mode) {
    chart_data <- board_data |>
      select(ts, value = !!sym(chart_item))
    
    if(any(!is.na(chart_data$value)))
      range <- c(min(chart_data$value, na.rm = TRUE), max(chart_data$value, na.rm = TRUE))
    else
      range <- NA
    
    # browser()
    res_plot <- chart_data |>
      plotly::plot_ly(x = ~ts)
    
    if(grepl('lines', chart_mode)) {
      res_plot <- res_plot |>
        plotly::add_lines(y = ~value,
          name = chart_item,
          line = list(shape = 'hv'))
    } else {
      res_plot <- res_plot |>
        plotly::add_trace(y = ~value,
          mode = chart_mode,
          name = chart_item,
          type = 'scatter')
    }
    
    res_plot <- res_plot |>
      plotly::layout(
        dragmode = 'pan',
        # legend = list(orientation = 'h'),
        showlegend = FALSE,
        xaxis = list(
          title = NA),
        yaxis = list(
          # title = chart_item,
          range = range
          # , fixedrange = TRUE
          ))
    
    return(res_plot)
  }
  
  use_modes <- sapply(chart_items, \(x) 'lines', USE.NAMES = TRUE)
  
  if(!is.null(modes))
    for(i in seq(length(modes)))
      use_modes[names(use_modes) == names(modes)[i]] <- modes[i]
  
  plots <- purrr::map2(chart_items, use_modes, make_chart)
  
  subplot_args <- c(plots,
    nrows = length(plots),
    shareX = TRUE)
  
  res <- do.call(plotly::subplot, subplot_args)
  
  if(!isFALSE(titles)) {
    # Prepare names
    if(length(titles) > 0) {
      if(is.null(names(titles)))
        stop('titles must have names.')
      for(i in 1:length(titles))
        chart_items[chart_items == names(titles)[i]] <- titles[i]
    }
    
    # Add titles on the subplots as annotations
    y_positions <- seq(1 - 1/length(chart_items), 0, 0 - 1/length(chart_items)) + (1/length(chart_items))/2
    annotations <- purrr::map2(y_positions, chart_items, \(y_pos, item) {
      list(x = 1, y = y_pos, text = item, showarrow = F, xref = 'paper', yref = 'paper',
        textangle = -90, xanchor = "right", yanchor = "middle", align = "center", valign = "center",
        font = list(size = titles_font_size))
    })
    
    res <- res |> plotly::layout(annotations = annotations)
  }
  
  return(res)
}

