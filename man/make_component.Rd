% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/component.R
\name{make_component}
\alias{make_component}
\title{Make component}
\usage{
make_component(
  input_source,
  line = NULL,
  bound = NULL,
  delay = NULL,
  window = NULL,
  ahead = NULL,
  up_to = NULL,
  pid = NULL,
  ts = NULL,
  .ts = NULL,
  .pid = NULL,
  .fn = NULL,
  .ts_fn = NULL,
  .passthrough = FALSE
)
}
\arguments{
\item{input_source}{A record source from \code{make_record_source()}, a component from \code{make_component()}, or a lazy
table. If the latter case, arguments \code{ts} and \code{pid} must also be provided.}

\item{line}{Integer. Which line to pick. 0 = skip no lines, 1 = skip one line, 2 = skip two lines, etc.}

\item{delay}{Character. Minimum time difference between phenotype date and component date. Time interval in SQL
language. Include any necessary type casting according to the SQL flavor of the server. Examples in PostgreSQL:
\code{'3 months'::interval}, \code{'20 seconds'::interval}, \code{'1.5 hours'::interval}.}

\item{window}{Character. Maximum time difference between phenotype date and the date of any component. Time interval
in SQL language (see argument \code{delay}).}

\item{pid}{Character. If passing a lazy table to \code{input_source}, \code{pid} is passed to \code{make_record_source()}
to buid a record source. See \code{\link{make_record_source}}.}

\item{ts}{Character. If passing a lazy table to \code{input_source}, \code{ts} is passed to \code{make_record_source()} to
buid a record source. See \code{\link{make_record_source}}.}

\item{.ts}{Unquoted characters. Optional. Use this argument to pass unquoted characters to the \code{ts} argument (see
examples). If \code{ts} is provided, \code{.ts} is ignored.}

\item{.pid}{Unquoted characters. Optional. Use this argument to pass unquoted characters to the \code{pid} argument (see
examples). If \code{pid} is provided, \code{.pid} is ignored.}
}
\value{
Phea component object.
}
\description{
Produce a Phea component.
}
\details{
Creates a component from the given \code{input_source} record source and optional parameters.

If \code{input_source} is a record source, it is used.

If \code{input_source} is a component, it is copied, and any other paremeter if provided overwrites the original one.

If \code{input_source} is a lazy table, a record source is generated from it, and used. In this case, arguments \code{pid}
and \code{ts} (or \code{.pid}, \code{.ts}) must also be provided.
}
\examples{
# This:
diabetes_mellitus <- sqlt(condition_occurrence) |>
  filter(condition_concept_id == 201820) |>
  make_component(
    .pid = person_id,
    .ts = condition_start_datetime,
    delay = "'6 months'::interval")
    
# Is the same as:
diabetes_mellitus <- sqlt(condition_occurrence) |>
  filter(condition_concept_id == 201820) |>
  make_component(
    pid = 'person_id',
    ts = 'condition_start_datetime',
    delay = "'6 months'::interval")
    
# Which is also the same as:
diabetes_mellitus <- sqlt(condition_occurrence) |>
  filter(condition_concept_id == 201820) |>
  make_record_source(
    pid = 'person_id',
    ts = 'condition_start_datetime') |>
  make_component(
    delay = "'6 months'::interval")
}
\seealso{
\code{\link[=make_record_source]{make_record_source()}} to create a record source.
}
