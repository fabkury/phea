% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pick_change.R
\name{keep_change_of}
\alias{keep_change_of}
\title{Keep change of \link{a column or SQL expression}}
\usage{
keep_change_of(lazy_tbl, of, partition = NULL, order = NULL)
}
\arguments{
\item{lazy_tbl}{Lazy table to be filtered.}

\item{of}{Character vector. Name of column(s) or SQL expression(s). Only rows where the value of \code{of} changes are
kept. If multiple columns or expressions are provided, a change in any of them causes the row to be in the output.}

\item{partition}{Character. Optional. Variable or variables to define the partition.}

\item{order}{Character. Optional. If provided, this or these column(s) will define the ordering of the rows and hence
how changes are detected.}
}
\value{
Lazy table with only rows where \code{of} changes in comparison to the previous row.
}
\description{
Keeps only the rows where the value of \code{of} changes. \code{of} can be the name of a column, or any SQL expression valid
inside a \code{SELECT} statement.
}
\details{
If \code{partition} is provided, changes are limited to within it. If \code{order} provided, rows are ordered and change is
detected according to the specified column or columns. The first row (within a partition or not) is always kept.
}
