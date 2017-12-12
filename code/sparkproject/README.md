# General Info
- Dumbo set spark 2.2.0:
  - `module load java/1.8.0_72`  
  - `module load spark/2.2.0`

# Build Compile Package
- install sbt
- under linear/randomforest folder
- run `/sbt/bin/./sbt` to start building project
- enter `compile` to compile
- enter `package` tp package
- press ctrl + Z to stop building
- run `./exec.sh`
- get an output.txt file


# Notice
In order to use "app", please run "randomforest" first to get the path to the model
"hdfs://babar.es.its.nyu.edu:8020/user/jy2234/test17/spark-random-forest-regression-model"


# Folders and Files
```
.
├──app
│  ├── src
│  │   └── main
│  │       └──scala
│  │          └── App.scala
│  │              # To predicted the crimes rate using model built with random forest regression
│  │              # In this file, we hard coded 201701 inputs, and the output is the predicted crimes rate of month 201702
│  ├── exec.sh # Shell script to execute the spark-submit
│  └── build.sbt # The project build configure
├──birth
│  ├── src
│  │   └── main
│  │       └──scala
│  │          └── Birth.scala
│  │              # To predicted the crimes rate using births data
│  │              # In this file, we hard coded 1999 inputs, and the output is the predicted crimes rate of year 2015
│  ├── exec.sh # Shell script to execute the spark-submit
│  └── build.sbt # The project build configure
├──linear
│  ├── src
│  │   └── main
│  │       └──scala
│  │          └── LinearRegression.scala
│  │              # To build model using linear regression
│  │              # [training] predicted month 201502-201706, [testing] predicted month 201707
│  ├── exec.sh # Shell script to execute the spark-submit
│  └── build.sbt # The project build configure
├──randomforest
│  ├── src
│  │   └── main
│  │       └──scala
│  │          └── LinearRegression.scala
│  │              # To build model using random forest regression
│  │              # [training] predicted month 201502-201706, [testing] predicted month 201707
│  ├── exec.sh # Shell script to execute the spark-submit
│  └── build.sbt # The project build configure
└── screenshots # Screenshots of some executation
```
