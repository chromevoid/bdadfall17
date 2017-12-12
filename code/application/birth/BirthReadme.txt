General Info:

Dumbo set spark 2.2.0:
 module load java/1.8.0_72  
 module load spark/2.2.0

Build Compile Package:
 install sbt
 under linear/randomforest folder
 run `/sbt/bin/./sbt` to start building project
 enter `compile` to compile
 enter `package` tp package
 press ctrl + Z to stop building
 run `./exec.sh`
 get an output.txt file


Folders and Files:

code
  app * (see sparkproject folder)
    src
      main
        App.scala
          -- To predicted the crimes rate using model built with random forest regression
          -- In this file, we hard coded 201701 inputs, and the output is the predicted crimes rate of month 201702
    exec.sh
    build.sbt
  App.txt
    -- Application code for spark-shell
  getAppInputs.txt
    -- To get the inputs of the application: the crimes, taxi, and coffee of a certain month
data
  test17
    crimes
      -- The crimes input of month January 2017
    distance
      -- The normalized distance between two communities, used as the weight for calculating geo feature
    help
      -- Two help files to help insert missing data in poi and taxi datasets
    poi
      -- The poi input of month January 2017
    population
      -- The population data of each community, used to calculate the crimes rate
    taxi
      -- The taxi input of month January 2017
output
  output.csv
    -- the crimes rate prediction of each community, January 2017