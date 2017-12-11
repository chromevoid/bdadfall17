import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.SparkSession

// /user/jy2234/training17

object LinearRegression {
  // Configuration for Spark.
  val conf = new SparkConf().setAppName("LinearRegression")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

  def train(trainFilePath: String): LinearRegressionModel = {
    // Training
    println("TRAINING.........")

    val trainingData = spark.read.format("libsvm").load(trainFilePath)

    // You can set the hyper parameters of linear regression at here.
    val lr: LinearRegression = new LinearRegression().setMaxIter(10)

    val model: LinearRegressionModel = lr.fit(trainingData)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = model.summary
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")

    return model
  }

  def test(testFilePath: String, model: LinearRegressionModel): Unit = {
    // Testing
    println("TESTING.........")

    val testData = spark.read.format("libsvm").load(testFilePath)

    val testSummary = model.evaluate(testData)
    println(s"RMSE: ${testSummary.rootMeanSquaredError}")

    // Predict
    //    val prediction = lrModel.transform(testData)
    //    prediction.select("label", "prediction").show(77)
  }

  def train_and_test(trainFilePath: String, testFilePath: String): LinearRegressionModel = {
    val model = train(trainFilePath)
    test(testFilePath, model)
    return model
  }


  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR") // Keep Spark quite.

    println("POI, GEO, TAXI.")
    train_and_test("/user/jy2234/training17/poi_geo_taxi_train.txt", "/user/jy2234/training17/poi_geo_taxi_test.txt")
    println("GEO, TAXI.")
    train_and_test("/user/jy2234/training17/geo_taxi_train.txt", "/user/jy2234/training17/geo_taxi_test.txt")

    // Save the LinearRegression model.
    // lrModel.write.overwrite().save("/tmp/spark-linear-regression-model")
  }
}