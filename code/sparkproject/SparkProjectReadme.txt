// commands
module load java/1.8.0_72  
module load spark/2.2.0

~/sbt/bin/./sbt
compile
package
crtl+z
./exec.sh
cat output.txt


// notice
In order to use "app", please run "randomforest" first to get the path to the model
"hdfs://babar.es.its.nyu.edu:8020/user/jy2234/test17/spark-random-forest-regression-model"


// folders and files
// these files are the same as:
//   /code/application/code/app
//   /code/training/crimesBigData/build/linear
//   /code/training/crimesBigData/build/randomforest
app
  src
    main
      App.scala
        -- To predicted the crimes rate using model built with random forest regression
        -- In this file, we hard coded 201701 inputs, and the output is the predicted crimes rate of month 201702
  exec.sh
  build.sbt
linear
  src
    main
      LinearRegression.scala
        -- To build model using linear regression: [training] predicted month 201502-201706, [testing] predicted month 201707
  exec.sh
    -- Shell script to execute the 
  build.sbt      
    -- To project build configure
randomforest
  src
    main
      RandomForestRegression.scala
        -- To build model using random forest regression: [training] predicted month 201502-201706, [testing] predicted month 201707
  exec.sh
  build.sbt