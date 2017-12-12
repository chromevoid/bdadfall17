spark-shell --packages com.databricks:spark-csv_2.10:1.4.0 

import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import sqlCtx._

val crimes = sqlCtx.read.format("com.databricks.spark.csv").
option("delimiter", ",").
option("header", "true").
option("inferSchema", "true").
load("/user/jy2234/project/cbRate/rate.csv")

crimes.dtypes.foreach(println)
crimes.show(3)

val crimesCount = crimes.groupBy("year","community").agg(sum("rate"))

crimesCount.show(3)

val births = sqlCtx.read.format("com.databricks.spark.csv").
option("delimiter", ",").
option("header", "true").
option("inferSchema", "true").
load("/user/jy2234/project/births-4cols/births-4cols.csv")

births.dtypes.foreach(println)
births.show(3)

val crimesBirths = crimesCount.join(
  births,
  crimesCount("year") <=> births("year") + 16
  && crimesCount("community") <=> births("community"),
  "inner"
)

crimesBirths.dtypes.foreach(println)
crimesBirths.show(3)

val newNames = Seq("yearCrimes", "communityCrimes", "rateCrimes", "yearBirths", "communityBirths", "countBirths", "rateBirths")
val crimesBirthsTable = crimesBirths.toDF(newNames: _*)

val crimesBirthsTrain = crimesBirthsTable.where("yearCrimes = 2015")
crimesBirthsTrain.persist()
val crimesBirthsCorr = crimesBirthsTrain.stat.corr("rateCrimes","rateBirths")
-- crimesBirthsCorr: Double = 0.6428782555948118

crimesBirthsTrain.rdd.coalesce(1,true).saveAsTextFile("/user/jy2234/project/crimesBirthsRate/")






