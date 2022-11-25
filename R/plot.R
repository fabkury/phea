# Control ---------------------------------------------------------------------------------------------------------
# do_plot <- TRUE


# Phea plot -------------------------------------------------------------------------------------------------------
# if(do_plot) {
#   library(plotly)
#   # #' @export
#   phea_plot <- function(board, pids, verbose = TRUE) {
#     make_step_chart_data <- function(board_data) {
#       union_all(
#         board_data |>
#           dplyr::arrange(pid, ts) |>
#           dplyr::group_by(pid, name) |>
#           dplyr::mutate(linha = row_number()) |>
#           dplyr::ungroup(),
#         board_data |>
#           dplyr::arrange(pid, ts) |>
#           dplyr::group_by(pid, name) |>
#           dplyr::mutate(linha = row_number()) |>
#           dplyr::mutate(ts = lead(ts)) |>
#           dplyr::ungroup()) |>
#         dplyr::arrange(pid, ts, linha)
#     }
#     
#     make_plotly_chart <- function(board_lazy, pids = NULL) {
#       if(verbose)
#         cat('Collecting lazy table, ')
#       board_data <- dplyr::collect(board_lazy)
#       if(verbose)
#         cat('done.\n')
#       
#       comp_board <- make_step_chart_data(board_data)
#       
#       chart_items <- comp_board |>
#         dplyr::select(name) |>
#         dplyr::distinct() |>
#         dplyr::pull()
#       
#       plot_args <- purrr::map(chart_items, \(chart_item) {
#         res_board <- comp_board |>
#           dplyr::filter(name == chart_item) |>
#           dplyr::filter(!is.na(value))
#         
#         if(nrow(res_board) > 0) {
#           res_plot <- res_board |>
#             plotly::plot_ly(
#               x = ~ts,
#               y = ~value,
#               type = 'scatter',
#               mode = 'lines',
#               name = chart_item) |>
#             layout(
#               legend = list(orientation = 'h'),
#               yaxis = list(
#                 range = c(
#                   min(res$value, na.rm = TRUE),
#                   max(res$value, na.rm = TRUE)),
#                 fixedrange = TRUE))
#           return(res_plot)
#         } else {
#           return(NULL)
#         }
#       })
#       
#       names(plot_args) <- chart_items
#       
#       plot_args <- plot_args |>
#         purrr::discard(is.null)
#       
#       plot_args <- c(plot_args, nrows = length(plot_args), shareX = TRUE)
#       return(
#         do.call(plotly::subplot, plot_args))
#     }
#     
#     board <- board |>
#       dplyr::filter(pid %in% local(pids))
#     
#     board_long <- board |>
#       tidyr::pivot_longer(
#         cols = !c(row_id, pid, ts, window))
#     
#     return(make_plotly_chart(board_long))
#   }
#   
#   if(!exists('pids'))
#     pids <- bmi |>
#       dplyr::select(pid) |>
#       dplyr::distinct() |>
#       dplyr::pull()
#   
#   res_plot <- phea_plot(bmi, sample(pids, 1))
# }

