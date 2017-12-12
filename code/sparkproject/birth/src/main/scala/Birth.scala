import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Birth {
  val conf = new SparkConf().setAppName("Birth")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder.appName("Birth").getOrCreate()

  def main(args: Array[String]): Unit = {
    val birth = sc.textFile("/user/jy2234/project/births-4cols")
      .filter(!_.contains("year")).filter(_.contains("1999,")).map(_.split(","))

    val crimes = birth.map(ln => {
      val year = ln(0).toInt + 16
      val community = ln(1)
      val count = ln(2).toDouble
      val crime = 0.0010509056545832782 * count + 0.024875505866072442
      val result = year + "," + community + "," + crime
      result
    })
    crimes.collect().foreach(println)
  }
}
