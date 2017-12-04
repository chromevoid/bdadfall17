Our goal is to analyze the crime rate in Chicago. 

We have three datasets: “Daily crime data in the City of Chicago”, “Public Health Statistics - Births to mothers aged 15-19 years old in Chicago”, and “Taxi Trips in the City of Chicago”. 


We use “year, community” to join “Crime” dataset and “Births” dataset, and we use “year, date, community” to join “Crime” dataset and “taxi” dataset.

We use Correlation to find the statistical dependence between two of the datasets. And for now the Correlation coefficients between crimes and births is big but the correlation coefficients between crime and taxis is small, so we are considering use another model to analyze the relationship.

For execute the code we have now:
1. save the code into .scala files
2. enter the path where those .scala files exist
3. run commands below in sequence
   ~/sbt/bin./sbt
   compile
   package  
   ./execute.sh
4. the output is saved into the clean.log file, so use 
   cat clean.log
   to show the result.


