# phea
#### _PHEnotyping Algebra_

R package to perform electronic patient phenotyping using formulas and without using SQL joins.  

Phea connects to any SQL table or query providing patient data in long form, that is, one row per event. At a minimum the data must have a patient identifier column and a time stamp column. From there, you can provide Phea with a formula such as `body_mass_index = weight / (height * height)`, and it will produce the SQL query that computes that formula, or formulas.
  
Install with devtools:  
`devtools::install_github('fabkury/phea')`  
`library(phea)`  
  
### Learn how to use
The materials below are meant to showcase Phea and teach how to use it.

 - https://www.youtube.com/watch?v=10GFtQREC0A: 30-minute video presentation (slides + voice) of Phea to the OHDSI Phenotype Development & Evaluation Working Group on January 27th, 2023. Explains what Phea is, shows examples, and teaches how to use.
 - [Computing body mass index](https://fabkury.github.io/phea/computing_bmi.html): A very brief look at how Phea works.   
 - [Getting started with Phea](https://fabkury.github.io/phea/): A little bit more explanations about the intuition behind Phea.   
 - [Calculate increase in body weight over the past 2-3 years](https://fabkury.github.io/phea/weight-increase.html): Calculating change in values over time.  
 - [Find acute myocardial infarction in obese patients](https://fabkury.github.io/phea/obese_ami.html): Finding acute myocardial infarctions happening _while_ a patient was obese, considering a person can go in and out of obesity over time.  
 - [Find increase in serum creatinine or low glomerular filtration rate](https://fabkury.github.io/phea/aki.html): Finding laboratory manifestations of kidney injury.  
 - [Stress-testing Phea, test A](https://fabkury.github.io/phea/stress_test_a.html): Compute formulas with up to 150 variables, each coming from a different SQL query.  
 - [Calculating the ASCVD Risk Estimator Plus score](https://fabkury.github.io/phea/ascvd.html): Compute a clinical risk score involving exponentiation and separate formulas for each of 4 subgroups of patients.  
  
  
### Compatibility with SQL dialects
 Phea leverages the `dbplyr` framework in R but also extends it in some parts. Phea's SQL generation engine offers two modes: **compatibility mode**, and **regular mode**. Compatibility mode has less features but works on more SQL dialects. Regular mode has more features and produces queries that are more efficient. The table below gives the mode currently available for each SQL dialect.  
 | Engine                   | Current mode | Expected final mode | Notes                                                                                                                                                                                                                                                                                                                                            |
|--------------------------|----------------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| postgres                 | regular        |                 | Phea was developed using Postgres 15. Minimum required version is 11. Regular mode requires access rights to install user-defined functions (UDFs), which are already implemented.                                                                                                                                                                                       |
| mysql                    | compatibility  | regular         |  Regular mode requires access rights to install user-defined functions, which have *not* yet been implemented.                                                                                                                                                                                                                                 |
| redshift                 | compatibility  | compatibility   |  Regular mode will not be possible until AWS Redshift supports RANGE clause for window functions.                                                                                                                                                                                                                                                     |
| databricks   (spark SQL) | regular        |                 |                                                                                                                                                                                                                                                                                                                                                  |
| oracle                   | _not tested_     | regular         |                                                                                                                                                                                                                                                                                                                                                  |
| bigquery                 | _not tested_     | regular         |                                                                                                                                                                                                                                                                                                                                                  |
| sqlserver                | _not tested_     | _unknown_         | Testing could show that regular mode is not possible: _"Depending on the ranking, aggregate, or analytic function used with the OVER clause, ORDER BY clause and/or the ROWS and RANGE clauses may not be supported."_ (https://learn.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql?view=sql-server-ver16) |
  
By Fabrício Kury  
Author contact: github@kury.dev
