# phea
#### _PHEnotyping Algebra_

R package to perform electronic patient phenotyping using formulas and without using SQL joins.  

Phea connects to any SQL table or query providing patient data in long form, that is, one row per event. At a minimum the data must have a patient identifier column and a time stamp column. From there, you can provide Phea with a formula such as `body_mass_index = weight / (height * height)`, and it will produce the SQL query that computes that formula, or formulas.
  
Install with devtools:  
`devtools::install_github('fabkury/phea')`  
`library(phea)`  
  
To learn how to use, please see:  

 - [Computing body mass index](https://fabkury.github.io/phea/computing_bmi.html): A very brief look at how Phea works.   
 - [Getting started with Phea](https://fabkury.github.io/phea/): A little bit more explanations about the intuition behind Phea.   
 - [Calculate increase in body weight over the past 2-3 years](https://fabkury.github.io/phea/weight-increase.html): Using Phea to calculate change in values over time.  
  
By Fabrício Kury  
Author contact: github@kury.dev
