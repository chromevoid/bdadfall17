import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SparkSession

// /user/jy2234/training17

object RandomForestRegression {
  val conf = new SparkConf().setAppName("RandomForestRegression")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder.appName("RandomForestRegression").getOrCreate()

  def train(trainFilePath: String): RandomForestRegressionModel = {
    // Training
    println("TRAINING.........")

    val trainingData = spark.read.format("libsvm").load(trainFilePath)

    val rfr: RandomForestRegressor = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val model: RandomForestRegressionModel = rfr.fit(trainingData)

    // Evaluation of model
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label").setPredictionCol("prediction")
      .setMetricName("rmse")

    val prediction_on_train = model.transform(trainingData)
    val rmse = evaluator.evaluate(prediction_on_train)
    println("RMSE on train data = " + rmse)

    return model
  }

  def test(testFilePath: String, model: RandomForestRegressionModel): Unit = {
    // Testing
    println("TESTING.........")

    val testData = spark.read.format("libsvm").load(testFilePath)

    // Evaluation of model
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label").setPredictionCol("prediction")
      .setMetricName("rmse")

    val prediction_on_train = model.transform(testData)
    val rmse = evaluator.evaluate(prediction_on_train)
    println("RMSE on test data = " + rmse)
  }

  def train_and_test(trainFilePath: String, testFilePath: String): RandomForestRegressionModel = {
    val model = train(trainFilePath)
    test(testFilePath, model)
    return model
  }


  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR") // Keep Spark quite.

    println("GEO, TAXI.")
    train_and_test("/user/jy2234/training17/geo_taxi_train.txt", "/user/jy2234/training17/geo_taxi_test.txt")

    println("POI, GEO, TAXI.")
    val rfrModel = train_and_test("/user/jy2234/training17/poi_geo_taxi_train.txt", "/user/jy2234/training17/poi_geo_taxi_test.txt")
    rfrModel.write.overwrite().save("/user/jy2234/training17/spark-random-forest-regression-model")
    val model = RandomForestRegressionModel.load("/user/jy2234/training17/spark-random-forest-regression-model")
  }
}

