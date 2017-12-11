1.
Using this model to build a linear regression for each community:
https://spark.apache.org/docs/2.2.0/ml-classification-regression.html#linear-regression


2.
trainingData
The "trainingData" contains training sets for community 1 to community 77.
In each textfile, the format is: y 1:x1 2:x2 3:x3.
Each textfile has 30 lines.
Each line is the data for a month.
The data is from 2015/01 to 2017/06. (That's why each file has 30 lines.)


3.
testingData.txt
the format of this file is "2017,7,community,y,x1,x2,x3"


4.
In the paper, the author says that "The accuracy of estimation is evaluated by mean absolute error (MAE) and mean relative error (MRE)."

Therefore, please calculate the MAE and MRE for each linear regression.

You may also use the MSR instead of MAE and MRE:
https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/LinearRegressionWithSGDExample.scala