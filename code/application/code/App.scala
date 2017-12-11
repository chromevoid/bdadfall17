import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.{Vectors};

object App {
  val sf = new SparkConf().setAppName("Crimes Rate Inference").setMaster("local[*]")
  val sc = new SparkContext(sf)
  val spark = SparkSession.builder().appName("Crimes Rate Inference").getOrCreate()

  def main(args: Array[String]): Unit = {
      
    // PART I: GET CRIMES RATE

    // "community"
    val crimes_raw = sc.textFile("data/input1701/crimes/")

    // ("comunity", count)
    val crimes_count = crimes_raw.map((_, 1)).reduceByKey(_ + _)

    // ("community","population")
    val population = sc.textFile("data/input1701/population/").map(ln => (ln.split(",")(0), ln.split(",")(1)))

    // ("community",(count,"population"))
    val crimes_rate_join = crimes_count.join(population)

    // "community,rate"
    val crimes_rate = crimes_rate_join.map(ln => ln._1 + "," + (ln._2._1 / ln._2._2.toDouble))

    // ("community", "community,rate")
    val crimes_rate_key = crimes_rate.keyBy(_.split(",")(0))


    // PART II: GET GEO FEATURE

    // "community,toCommunity,distance"
    val distance = sc.textFile("data/input1701/distance")

    // ("toCommunity", "community,toCommunity,distance")
    val distance_key = distance.keyBy(_.split(",")(1))

    // ("toComm",("comm,toComm,dis","toComm,rate"))
    val geo_join = distance_key.join(crimes_rate_key)

    // "comm,geoInit"
    val geo_init = geo_join.map(ln => {
      val comm = ln._2._1.split(",")(0)
      val dis = ln._2._1.split(",")(2).toDouble
      val rate = ln._2._2.split(",")(1).toDouble
      val result = comm + "," + (dis * rate).toString
      result
    })

    // ("comm",["geoInit", ...])
    val geo_groupby = geo_init.map(ln => (ln.split(",")(0), ln.split(",")(1))).groupByKey()

    // "community,geo"
    val geo = geo_groupby.map(ln => {
      var result = ln._1
      val itStr = ln._2
      var sum: Double = 0.0
      for (each <- itStr) {
        sum = sum + each.toDouble
      }
      result = result + "," + sum.toString()
      result
    })


    // PART III: GET TAXI FEATURE

    // "pickup,dropoff"
    val taxi_raw = sc.textFile("data/input1701/taxi/")

    // "pickup,dropoff,count"
    val taxi_count_init = taxi_raw.map((_, 1)).reduceByKey(_ + _).map(ln => ln._1 + "," + ln._2.toString())

    // "pickup,dropoff,count"
    // if pickup == dropoff, then set count to zero
    val taxi_count_second = taxi_count_init.map(ln => {
      var result: String = ln.split(",")(0) + "," + ln.split(",")(1)
      if (ln.split(",")(0) == ln.split(",")(1)) {
        result = result + ",0"
      }
      else {
        result = result + "," + ln.split(",")(2)
      }
      result
    }).sortBy(_.split(",")(1).toInt).sortBy(_.split(",")(0).toInt)

    // ("dropoff,pickup",0)
    val taxi_help = sc.textFile("data/input1701/help/taxi_help.txt").map(ln => (ln.split(",")(1)+","+ln.split(",")(0),ln.split(",")(2).toInt))

    // ("dropoff,pickup",count)
    val taxi_count_key = taxi_count_second.map(ln => (ln.split(",")(1)+","+ln.split(",")(0),ln.split(",")(2).toInt))

    // (String, (Int, Option[Int]))
    // ("dropoff,pickup",(0,count))
    val taxi_count_join = taxi_help.leftOuterJoin(taxi_count_key)

    // ("dropoff","pickup,count")
    val taxi_count = taxi_count_join.map(ln => {
      val keyStr = ln._1
      val option = ln._2._2
      var result = keyStr + ","
      if (option == None) {
        result = result + "0"
      }
      else {
        val count = option.toString().split("\\(")(1).split("\\)")(0).toInt
        result = result + count.toString()
      }
      result
    }).map(ln => (ln.split(",")(0), ln.split(",")(1) + "," + ln.split(",")(2)))

    // ("dropoff",["pickup,count", ...])
    val taxi_count_groupby = taxi_count.groupByKey()

    // ("dropoff","sum")
    val taxi_sum = taxi_count_groupby.map(ln => {
      var s: Double = 0.0
      val keyStr = ln._1
      val itStr = ln._2
      for (each <- itStr) {
        s = s + each.split(",")(1).toDouble
      }
      val result: String = keyStr + "," + s.toString()
      result
    }).map(ln => (ln.split(",")(0), ln.split(",")(1)))


    // ("dropoff",("pickup,count","sum"))
    val taxi_weight_init = taxi_count.join(taxi_sum)

    // "dropoff,pickup,subFeature" = "comm,toComm,weight"
    val taxi_weight = taxi_weight_init.map(ln => {
      val keyStr = ln._1
      val valueStr = ln._2
      val pickup = valueStr._1.split(",")(0)
      val count: Double = valueStr._1.split(",")(1).toDouble
      val sum: Double = valueStr._2.toDouble
      val feature: Double = count / sum
      val result = keyStr + "," + pickup + "," + feature.toString()
      result
    })

    val taxi_weight_key = taxi_weight.keyBy(_.split(",")(1))

    // ("toComm",("comm,toComm,weight","toComm,rate"))
    val taxi_join = taxi_weight_key.join(crimes_rate_key)

    // "comm,taxiInit"
    val taxi_init = taxi_join.map(ln => {
      val comm = ln._2._1.split(",")(0)
      val weight = ln._2._1.split(",")(2).toDouble
      val rate = ln._2._2.split(",")(1).toDouble
      val result = comm + "," + (weight * rate).toString
      result
    })

    // ("comm",["taxiInit", ...])
    val taxi_groupby = taxi_init.map(ln => (ln.split(",")(0), ln.split(",")(1))).groupByKey()

    // "community,taxi"
    val taxi = taxi_groupby.map(ln => {
      var result = ln._1
      val itStr = ln._2
      var sum: Double = 0.0
      for (each <- itStr) {
        sum = sum + each.toDouble
      }
      result = result + "," + sum.toString()
      result
    })


    // PART IV: GET POI FEATURE

    // "community"
    val coffee_raw = sc.textFile("data/input1701/poi")

    // ("community",count)
    val coffee_count_key = coffee_raw.map((_, 1)).reduceByKey(_ + _)

    // ("community",0)
    val coffee_help = sc.textFile("data/input1701/help/coffee_help.txt").map(ln => (ln.split(",")(0),ln.split(",")(1).toInt))

    // (String, (Int, Option[Int]))
    // ("community",(0,count))
    val coffee_count_join = coffee_help.leftOuterJoin(coffee_count_key)

    // "community,count"
    val coffee = coffee_count_join.map(ln => {
      val keyStr = ln._1
      val option = ln._2._2
      var result = keyStr + ","
      if (option == None) {
        result = result + "0"
      }
      else {
        val count = option.toString().split("\\(")(1).split("\\)")(0).toInt
        result = result + count.toString()
      }
      result
    })

    // PART V: GET INPUT FORMAT
    // poi, geo, taxi
    // coffee.count()
    // coffee.take(10)
    // geo.count()
    // geo.take(10)
    // taxi.count()
    // taxi.take(10)

    val poi_feature = coffee.keyBy(_.split(",")(0))
    val geo_feature = geo.keyBy(_.split(",")(0))
    val taxi_feature = taxi.keyBy(_.split(",")(0))

    val geo_taxi = geo_feature.join(taxi_feature).map(ln => ln._1 + "," + ln._2._1.split(",")(1) + "," + ln._2._2.split(",")(1))
    val geo_taxi_feature = geo_taxi.keyBy(_.split(",")(0))
    val poi_geo_taxi = poi_feature.join(geo_taxi_feature).map(ln => ln._1 + "," + ln._2._1.split(",")(1) + "," + ln._2._2.split(",")(1) + "," + ln._2._2.split(",")(2))

    // PART VI: MODEL
    // 201702 actual crimes rate: 0.023445250900736254


    /* **********
    PART VI: MODEL
    // 201702 actual crimes rate: 0.023445250900736254
    * ********** */

    // Load trained model
    val modelPath = "/tmp/spark-random-forest-regression-model"
    val model: RandomForestRegressionModel = RandomForestRegressionModel.load(modelPath)

    // Convert RDD to DataFrame
    val feat = spark.createDataFrame(
      poi_geo_taxi.map(ln => {
        val parts = ln.split(",")
        val community = parts(0).toInt
        val poi_feat = parts(1).toDouble
        val geo_feat = parts(2).toDouble
        val taxi_feat = parts(3).toDouble
        (community, Vectors.dense(poi_feat, geo_feat, taxi_feat))
      })
    ).toDF("community", "features")

    // Prediction.
    val prediciton = model.setFeaturesCol("features").transform(feat).select("community", "prediction")

    prediciton.orderBy("community").show(77)
    prediciton.orderBy("community").coalesce(1).write.csv("prediction.csv")
  }
}
