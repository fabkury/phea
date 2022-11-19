# phea.R ----------------------------------------------------------------------------------------------------------
#' ---
#' PHEnotyping Algebra
#' By Fabrício Kury, fab at kury.dev.
#' 2022-11-17 19:07 EST
#'


# OMOP source tables ----------------------------------------------------------------------------------------------
colnames_dbplyr <- function(records) {
  records |>
    head(1) |> # Coletar 1 line.
    collect() |>
    colnames()
}

omop <- function(table, schema) {
  tbl(db$con, in_schema(
    deparse(substitute(schema)),
    deparse(substitute(table))))
}

make_component <- function(rec_name, line = NA, delay = NA, window = Inf) {
  if(is.na(line) && is.na(delay))
    line <- 0

  component <- list()

  component$rec_name <- rec_name
  component$line <- line
  component$delay <- delay
  component$window <- window

  component
}

make_record_source <- function(records, rec_name, ts, pid, vars = NULL) {
  rec_source <- list()

  rec_source$records <- records

  rec_source$rec_name <- rec_name

  ts_name <- deparse(substitute(ts))
  rec_source$ts <- ts_name

  if(is.null(vars))
    vars <- records |>
    colnames_dbplyr()

  if(is.null(vars))
    vars <- records |>
    colnames_dbplyr() # Coletar 1 line.

  pid_name <- deparse(substitute(pid))
  vars <- setdiff(vars, pid_name)

  rec_source$vars <- vars
  rec_source$rec_pid <- pid_name

  rec_source
}


# Calculate formula -----------------------------------------------------------------------------------------------
calculate_formula <- function(record_sources, components, fml = "1", window = NA, export = NULL,
  .ordem = NULL, .exclui_na = FALSE, .requer_todos = TRUE, .lim = NA) {
  # Prepare ---------------------------------------------------------------------------------------------------------
  # Extract used components from formula.
  g_vars <- str_match_all(fml, '([A-z][A-z0-9_]+)')[[1]][,2]
  # Remove some very basic SQL keywords.
  g_vars <- setdiff(g_vars,
    c('in', 'and', 'or', 'IN', 'AND', 'OR', 'EXISTS', 'exists', 'case', 'when', 'end', 'else', 'ELSE', 'WHEN', 'END',
      'IF', 'IS', 'NULL', 'is', 'null'))
  # Add variables requested to export.
  g_vars <- c(g_vars, export)

  # Create board from components ----------------------------------------------------------------------------------
  for(i in seq(record_sources)) {
    ts <- record_sources[[i]]$ts
    rec_name <- record_sources[[i]]$rec_name
    rec_pid <- record_sources[[i]]$rec_pid

    # Normalize the column names.
    record_sources[[i]]$records <- mutate(record_sources[[i]]$records
      name = local(rec_name),
      pid = !!sym(rec_pid),
      ts = !!sym(ts))

    #' Select only the columns that will be needed later, according to g_vars. This requires checking all applicable
    #' components.
    vars <- record_sources[[i]]$vars
    comp_mask <- map(components, ~.$rec_name) == rec_name
    related_components <- names(components)[comp_mask]
    pairs <- cross2(related_components, vars) # Create all combinations of related components and rec_source's vars.
    pairs_mask <- map(pairs, \(x) paste0(x[[1]], '_', x[[2]]) %in% g_vars) |> unlist()

    if(any(pairs_mask)) {
      out_vars <- pairs[unlist(pairs_mask)] |>
        map(~.[[2]]) |> unlist()

      record_sources[[i]]$records <- record_sources[[i]]$records |>
        select(name, pid, ts,
          all_of(out_vars))
    }
  }

  board <- map(record_sources, ~.$records) |>
    reduce(union_all)

  # Criar colunas dos components -----------------------------------------------------------------------------------
  # Primeiro, gerar os comandos.
  produce_component <- function(comp_name, component) {
    rec_name <- component$rec_name

    delay <- component$delay
    line <- component$line
    window <- component$window

    # Locate the record source, so we know what variables to extract.
    rec_mask <- map(record_sources, ~.$rec_name) == rec_name
    rec_source <- record_sources[rec_mask][[1]]

    vars <- c(rec_source$vars, 'ts')
    ts_var <- paste0(comp_name, '_ts')
    vars_mask <- paste0(comp_name, '_', vars) %in% c(g_vars, ts_var)
    vars <- vars[vars_mask]

    colunas <- paste0(comp_name, '_', vars, '_calc')

    #' Por algum motivo, o "lag()" não funciona com "range between window preceeding and delay preceeding", i.e. não é
    #' possível aplicar line *e* delay em uma só chamada à função de window. O last_value() funciona com esse "range".
    vars_sql <- paste0('case when "name" = \'', rec_name, '\' then "', vars, '" else null end')
    over_clause <- paste0('partition by "pid" order by "ts"') #paste0('partition by "pid", "name" order by "ts"')

    #' Dar a preferência ao acesso via *line*.
    if(!is.na(line)) {
      sql_txts <- paste0('last_value(', vars_sql, ') over (', over_clause,
        ' rows between unbounded preceding and ', line, ' preceding)')
    } else {
      #' Caso contrário, produzir acesso via *delay*.
      range_start_clause <- ifelse(window == Inf,
        'range between unbounded preceding',
        paste0('range between \'', window, '\'::interval preceding'))

      sql_txts <- paste0('last_value(', vars_sql, ') over (', over_clause, ' ',
        range_start_clause, ' and \'', delay, '\'::interval preceding', ')')
    }

    comandos <- list(colunas, sql_txts) |>
      pmap(~rlang::exprs(!!..1 := sql(!!..2))) |>
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
  calc_vars <- c(g_vars, paste0(names(components), '_ts'))

  sql_txts <- paste0('find_last_ignore_nulls("', calc_vars, '_calc") OVER ',
    '(PARTITION BY "pid" ORDER BY "ts" ROWS UNBOUNDED PRECEDING)')

  comandos <- map2(calc_vars, sql_txts,
    ~rlang::exprs(!!.x := sql(!!.y))) |>
    unlist()

  #' A frente (ponto mais recente) da window é a ts da line atual. A trás (ponto mais antigo) é o mínimo dos
  #' components.
  sql_datahora_min <- paste0('least(', paste0(names(components), '_ts', collapse = ', '), ')')

  # Apply them.
  #' The use of transmute + mutate + select(-ts) is to keep the query "tight." By "tight" I mean no use of select *,
  #' that is, every step of the process passes forward only strictly what the next step needs. The hope is that this
  #' will help the SQL query optimizer.
  board <- board |>
    transmute(
      pid, row_id, ts,
      !!!comandos,
      window = ts - sql(sql_datahora_min))

  # Require all fields be filled.
  if(.requer_todos) {
    sql_txt <- paste0(names(components), '_ts is not null') |> paste(collapse = ' and ')
    board <- filter(board,
      sql(sql_txt))
  }

  board <- board |>
    select(
      -ts,
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

  # Calculate the formala.
  if(class(fml) == 'function') {
    board <- board |>
      collect()
    board <- mutate(board,
      value = fml(board))
  } else {
    sql_txt <- fml
    names(sql_txt) <- NULL # Importante para o funcionamento da sql(), devido à non-standard evaluation.
    board <- mutate(board,
      value = sql(sql_txt))
  }

  # Requerer que o resultado exista.
  if(.exclui_na)
    board <- filter(board,
      !is.na(value))

  # Colapsar e retornar -------------------------------------------------------------------------------------------
  # Chamar o collapse() é necessário para evitar acúmulo de código lazy com eventual erro "C stack usage is too close to
  # the limit". Vide https://github.com/tidyverse/dbplyr/issues/719. Obrigado, mgirlich!
  board <- collapse(board, cte = TRUE)

  board
}
