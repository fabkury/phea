% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/phea.R
\name{phea_plot}
\alias{phea_plot}
\title{Plot a phenotype.}
\usage{
phea_plot(board, pid, verbose = TRUE)
}
\arguments{
\item{board}{The object returned by \code{calculate_formula()}.}

\item{pid}{Required. ID of the patient to be included in the chart.}

\item{verbose}{If TRUE, will let you know how long it takes to \code{collect()} the data.}
}
\value{
Plot created by \code{plotly::plot_ly()} and \code{plotly::subplot()}.
}
\description{
Plots the result from \code{calculate_formula()} in an interactive timeline chart using \code{plotly}.
}
\details{
Collects (downloads) the results and creates interactive timeline chart using the \code{plotly} library.
}