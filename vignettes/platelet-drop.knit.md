---
title: "Calculate drop in body weight"
output: rmarkdown::html_vignette
vignette: >
  %\VignetteIndexEntry{Calculate drop in body weight}
  %\VignetteEngine{knitr::rmarkdown}
  %\VignetteEncoding{UTF-8}
---
This vignette assumes a SQL server at `localhost` (we use PostgreSQL), with an OMOP Common Data Model v5.4 in schema `cdm_new_york3` containing patient records. The patient records shown in this example are synthetic data from [Synthea Patient Generator](https://github.com/synthetichealth/synthea).  


```r
library(knitr)
library(phea)
suppressPackageStartupMessages(library(dplyr))

# Connect to SQL server.
dbcon <- DBI::dbConnect(RPostgres::Postgres(),
    host = 'localhost', port = 7654,
    dbname = 'fort', user = cred$pg$user, password = cred$pg$pass)

# Provide the connection to Phea so we can use the sqlt() and sql0() shorthands.
setup_phea(dbcon, 'cdm_new_york3')
```

Say we want to find patients whose weight has dropped by more than 25% in the last 1 years to 2 years. The formula for the number we want is very simple:  

`weight_drop = (weight_current - weight_previous) / weight_previous`  

If `weight_drop` is ever -0.25 or smaller, that means a drop of 25% or bigger. But unlike in other formulas, here the component `weight_previous` is not the most recently available weight record for the given patient, at the given point in time.  

While the _record source_ is the same as `weight_current`, the _component_ is different. When we call `make_component()`, we need to specify the time frame using the `delay` option, and optionally the `window` as well.  


```r
# Weight records
# Loinc 29463-7 Body weight, concept ID 3025315
weight_record_source = sqlt(measurement) |>
  filter(measurement_concept_id == 3025315) |>
  make_record_source(
    ts = measurement_datetime,
    pid = person_id)

# Most recent weight count record
weight_current = make_component(
  input_source = weight_record_source)

# weight count record at least 24 hours older than phenotype date, up to limit of 48 hours
weight_previous = make_component(
  input_source = weight_record_source,
  delay = '24 hours',
  window = '48 hours')
```

_`delay` will pick the most recent record that is **at least as old** as the specified amount of time._  

_`window` imposes a **limit on how old** the record is allowed to be._  

Both are expressed in SQL language, including the automatic parsing, by the server, of terms such as `'2 years'`, `'2.5 hours'`, `'1 week'`, etc.  

If no such record exists for the patient, at the given point in time, the value is `NULL`.  

## Calculate the phenotype  
Armed with the two components, we call `calculate_formula()`:  

```r
phen <- calculate_formula(
  components = list(
    weight_current = weight_current,
    weight_previous = weight_previous),
  fml = list(
    weight_drop = '(weight_current_value_as_number - weight_previous_value_as_number) /
      weight_previous_value_as_number',
    weight_25p_drop = "case when weight_drop <= -0.25 then 'yes' else 'no' end")) |>
  filter(!is.na(weight_drop))
```
## Plot the phenotype result  


```r
# Plot phenotype for patient whose pid = 2105 
phen |>
  select(-weight_25p_drop) |>
  phea_plot(pid = 2105)
#> Collecting lazy table, done.
```

```{=html}
<div id="htmlwidget-c91d6dc9c01e11a83c0b" style="width:100%;height:500px;" class="plotly html-widget"></div>
<script type="application/json" data-for="htmlwidget-c91d6dc9c01e11a83c0b">{"x":{"data":[{"x":["2003-07-02 00:00:00.000000","2004-07-07 00:00:00.000000","2004-07-07 00:00:00.000000","2005-07-13 00:00:00.000000","2005-07-13 00:00:00.000000","2006-07-19 00:00:00.000000","2006-07-19 00:00:00.000000","2007-07-25 00:00:00.000000","2007-07-25 00:00:00.000000","2008-07-30 00:00:00.000000","2008-07-30 00:00:00.000000","2009-08-05 00:00:00.000000","2009-08-05 00:00:00.000000","2010-08-11 00:00:00.000000","2010-08-11 00:00:00.000000","2011-08-17 00:00:00.000000","2011-08-17 00:00:00.000000","2012-08-22 00:00:00.000000","2012-08-22 00:00:00.000000","2013-08-28 00:00:00.000000","2013-08-28 00:00:00.000000","2014-09-03 00:00:00.000000","2014-09-03 00:00:00.000000","2015-09-09 00:00:00.000000","2015-09-09 00:00:00.000000","2016-09-14 00:00:00.000000","2016-09-14 00:00:00.000000","2017-09-20 00:00:00.000000","2017-09-20 00:00:00.000000","2018-09-26 00:00:00.000000","2018-09-26 00:00:00.000000","2019-10-02 00:00:00.000000","2019-10-02 00:00:00.000000","2020-10-07 00:00:00.000000","2020-10-07 00:00:00.000000","2021-10-13 00:00:00.000000","2021-10-13 00:00:00.000000","2022-10-19 00:00:00.000000","2022-10-19 00:00:00.000000"],"y":[28.4,28.4,32.6,32.6,36.5,36.5,42.1,42.1,48.7,48.7,54.7,54.7,58.2,58.2,60.7,60.7,62.2,62.2,64,64,67.1,67.1,71.7,71.7,72.8,72.8,74.5,74.5,76.5,76.5,78,78,79.4,79.4,80.5,80.5,82.3,82.3,83.6],"mode":"lines","name":"weight_current_value_as_number","type":"scatter","marker":{"color":"rgba(31,119,180,1)","line":{"color":"rgba(31,119,180,1)"}},"error_y":{"color":"rgba(31,119,180,1)"},"error_x":{"color":"rgba(31,119,180,1)"},"line":{"color":"rgba(31,119,180,1)"},"xaxis":"x","yaxis":"y","frame":null},{"x":["2003-07-02 00:00:00.000000","2004-07-07 00:00:00.000000","2004-07-07 00:00:00.000000","2005-07-13 00:00:00.000000","2005-07-13 00:00:00.000000","2006-07-19 00:00:00.000000","2006-07-19 00:00:00.000000","2007-07-25 00:00:00.000000","2007-07-25 00:00:00.000000","2008-07-30 00:00:00.000000","2008-07-30 00:00:00.000000","2009-08-05 00:00:00.000000","2009-08-05 00:00:00.000000","2010-08-11 00:00:00.000000","2010-08-11 00:00:00.000000","2011-08-17 00:00:00.000000","2011-08-17 00:00:00.000000","2012-08-22 00:00:00.000000","2012-08-22 00:00:00.000000","2013-08-28 00:00:00.000000","2013-08-28 00:00:00.000000","2014-09-03 00:00:00.000000","2014-09-03 00:00:00.000000","2015-09-09 00:00:00.000000","2015-09-09 00:00:00.000000","2016-09-14 00:00:00.000000","2016-09-14 00:00:00.000000","2017-09-20 00:00:00.000000","2017-09-20 00:00:00.000000","2018-09-26 00:00:00.000000","2018-09-26 00:00:00.000000","2019-10-02 00:00:00.000000","2019-10-02 00:00:00.000000","2020-10-07 00:00:00.000000","2020-10-07 00:00:00.000000","2021-10-13 00:00:00.000000","2021-10-13 00:00:00.000000","2022-10-19 00:00:00.000000","2022-10-19 00:00:00.000000"],"y":[74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8,74.8],"mode":"lines","name":"weight_previous_value_as_number","type":"scatter","marker":{"color":"rgba(255,127,14,1)","line":{"color":"rgba(255,127,14,1)"}},"error_y":{"color":"rgba(255,127,14,1)"},"error_x":{"color":"rgba(255,127,14,1)"},"line":{"color":"rgba(255,127,14,1)"},"xaxis":"x","yaxis":"y2","frame":null},{"x":["2003-07-02 00:00:00.000000","2004-07-07 00:00:00.000000","2004-07-07 00:00:00.000000","2005-07-13 00:00:00.000000","2005-07-13 00:00:00.000000","2006-07-19 00:00:00.000000","2006-07-19 00:00:00.000000","2007-07-25 00:00:00.000000","2007-07-25 00:00:00.000000","2008-07-30 00:00:00.000000","2008-07-30 00:00:00.000000","2009-08-05 00:00:00.000000","2009-08-05 00:00:00.000000","2010-08-11 00:00:00.000000","2010-08-11 00:00:00.000000","2011-08-17 00:00:00.000000","2011-08-17 00:00:00.000000","2012-08-22 00:00:00.000000","2012-08-22 00:00:00.000000","2013-08-28 00:00:00.000000","2013-08-28 00:00:00.000000","2014-09-03 00:00:00.000000","2014-09-03 00:00:00.000000","2015-09-09 00:00:00.000000","2015-09-09 00:00:00.000000","2016-09-14 00:00:00.000000","2016-09-14 00:00:00.000000","2017-09-20 00:00:00.000000","2017-09-20 00:00:00.000000","2018-09-26 00:00:00.000000","2018-09-26 00:00:00.000000","2019-10-02 00:00:00.000000","2019-10-02 00:00:00.000000","2020-10-07 00:00:00.000000","2020-10-07 00:00:00.000000","2021-10-13 00:00:00.000000","2021-10-13 00:00:00.000000","2022-10-19 00:00:00.000000","2022-10-19 00:00:00.000000"],"y":[-0.620320855614973,-0.620320855614973,-0.564171122994652,-0.564171122994652,-0.512032085561497,-0.512032085561497,-0.43716577540107,-0.43716577540107,-0.348930481283422,-0.348930481283422,-0.268716577540107,-0.268716577540107,-0.22192513368984,-0.22192513368984,-0.188502673796791,-0.188502673796791,-0.168449197860963,-0.168449197860963,-0.144385026737968,-0.144385026737968,-0.102941176470588,-0.102941176470588,-0.0414438502673797,-0.0414438502673797,-0.0267379679144385,-0.0267379679144385,-0.00401069518716578,-0.00401069518716578,0.0227272727272727,0.0227272727272727,0.0427807486631016,0.0427807486631016,0.0614973262032086,0.0614973262032086,0.0762032085561497,0.0762032085561497,0.100267379679144,0.100267379679144,0.117647058823529],"mode":"lines","name":"weight_drop","type":"scatter","marker":{"color":"rgba(44,160,44,1)","line":{"color":"rgba(44,160,44,1)"}},"error_y":{"color":"rgba(44,160,44,1)"},"error_x":{"color":"rgba(44,160,44,1)"},"line":{"color":"rgba(44,160,44,1)"},"xaxis":"x","yaxis":"y3","frame":null}],"layout":{"xaxis":{"domain":[0,1],"automargin":true,"anchor":"y3"},"yaxis3":{"domain":[0,0.313333333333333],"automargin":true,"range":[-0.620320855614973,0.117647058823529],"fixedrange":true,"anchor":"x"},"yaxis2":{"domain":[0.353333333333333,0.646666666666667],"automargin":true,"range":74.8,"fixedrange":true,"anchor":"x"},"yaxis":{"domain":[0.686666666666667,1],"automargin":true,"range":[28.4,83.6],"fixedrange":true,"anchor":"x"},"annotations":[],"shapes":[],"images":[],"margin":{"b":40,"l":60,"t":25,"r":10},"legend":{"orientation":"h"},"hovermode":"closest","showlegend":true},"attrs":{"2c5c4c6d290e":{"x":{},"y":{},"mode":"lines","name":"weight_current_value_as_number","alpha_stroke":1,"sizes":[10,100],"spans":[1,20],"type":"scatter"},"2c5c454e2338":{"x":{},"y":{},"mode":"lines","name":"weight_previous_value_as_number","alpha_stroke":1,"sizes":[10,100],"spans":[1,20],"type":"scatter"},"2c5c6f3c701b":{"x":{},"y":{},"mode":"lines","name":"weight_drop","alpha_stroke":1,"sizes":[10,100],"spans":[1,20],"type":"scatter"}},"source":"A","config":{"modeBarButtonsToAdd":["hoverclosest","hovercompare"],"showSendToCloud":false},"highlight":{"on":"plotly_click","persistent":false,"dynamic":false,"selectize":false,"opacityDim":0.2,"selected":{"opacity":1},"debounce":0},"subplot":true,"shinyEvents":["plotly_hover","plotly_click","plotly_selected","plotly_relayout","plotly_brushed","plotly_brushing","plotly_clickannotation","plotly_doubleclick","plotly_deselect","plotly_afterplot","plotly_sunburstclick"],"base_url":"https://plot.ly"},"evals":[],"jsHooks":[]}</script>
```
## Compute aggregates of the phenotype result  

## Obtain the SQL query that computes the phenotype  
To see the SQL query underlying the phenotype, use helper function `code_shot()`, or `dbplyr::sql_render()`, or the `.clip_sql` option in `calculate_formula()`.  

```r
code_shot(phen)
```
```sql
SELECT *
FROM (
SELECT
  *,
  case when weight_drop <= -0.25 then 'yes' else 'no' end AS "weight_25p_drop"
FROM (
  SELECT
    "row_id",
    "pid",
    "ts",
    "window",
    "weight_current_value_as_number",
    "weight_previous_value_as_number",
    (weight_current_value_as_number - weight_previous_value_as_number) /
      weight_previous_value_as_number AS "weight_drop"
  FROM (
    SELECT
      *,
      "ts" - least(weight_current_ts, weight_previous_ts) AS "window",
      last_value(row_id) over (partition by "pid", "ts") AS "ts_row"
    FROM (
      SELECT
        "row_id",
        "pid",
        "ts",
        MAX("weight_current_value_as_number") OVER (PARTITION BY "..dbplyr_partion_1") AS "weight_current_value_as_number",
        MAX("weight_current_ts") OVER (PARTITION BY "..dbplyr_partion_2") AS "weight_current_ts",
        MAX("weight_previous_value_as_number") OVER (PARTITION BY "..dbplyr_partion_3") AS "weight_previous_value_as_number",
        MAX("weight_previous_ts") OVER (PARTITION BY "..dbplyr_partion_4") AS "weight_previous_ts"
      FROM (
        SELECT
          *,
          SUM(CASE WHEN (("weight_current_value_as_number" IS NULL)) THEN 0 ELSE 1 END) OVER (ORDER BY "pid", "ts" ROWS UNBOUNDED PRECEDING) AS "..dbplyr_partion_1",
          SUM(CASE WHEN (("weight_current_ts" IS NULL)) THEN 0 ELSE 1 END) OVER (ORDER BY "pid", "ts" ROWS UNBOUNDED PRECEDING) AS "..dbplyr_partion_2",
          SUM(CASE WHEN (("weight_previous_value_as_number" IS NULL)) THEN 0 ELSE 1 END) OVER (ORDER BY "pid", "ts" ROWS UNBOUNDED PRECEDING) AS "..dbplyr_partion_3",
          SUM(CASE WHEN (("weight_previous_ts" IS NULL)) THEN 0 ELSE 1 END) OVER (ORDER BY "pid", "ts" ROWS UNBOUNDED PRECEDING) AS "..dbplyr_partion_4"
        FROM (
          SELECT
            row_number() over () AS "row_id",
            "pid",
            "ts",
            last_value(case when "name" = 'gf27cn8ubdzp' then "value_as_number" else null end) over (partition by "pid", "name" order by "ts" rows between unbounded preceding and 0 preceding) AS "weight_current_value_as_number",
            last_value(case when "name" = 'gf27cn8ubdzp' then "ts" else null end) over (partition by "pid", "name" order by "ts" rows between unbounded preceding and 0 preceding) AS "weight_current_ts",
            last_value(case when "name" = 'gf27cn8ubdzp' then "value_as_number" else null end) over (partition by "pid", "name" order by "ts" range between '48 hours'::interval preceding and '24 hours'::interval preceding) AS "weight_previous_value_as_number",
            last_value(case when "name" = 'gf27cn8ubdzp' then "ts" else null end) over (partition by "pid", "name" order by "ts" range between '48 hours'::interval preceding and '24 hours'::interval preceding) AS "weight_previous_ts"
          FROM (
            SELECT
              'gf27cn8ubdzp' AS "name",
              "person_id" AS "pid",
              "measurement_datetime" AS "ts",
              "value_as_number"
            FROM "cdm_new_york3"."measurement"
            WHERE ("measurement_concept_id" = 3025315.0)
          ) "q01"
        ) "q02"
      ) "q03"
    ) "q04"
  ) "q05"
  WHERE ("row_id" = "ts_row")
) "q06"
) "q01"
WHERE (NOT(("weight_drop" IS NULL)))
```


