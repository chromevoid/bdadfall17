spark-shell --packages com.databricks:spark-csv_2.10:1.4.0

import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import sqlCtx._


val taxi = sqlCtx.read.format("com.databricks.spark.csv").
option("delimiter", ",").
option("header", "true").
option("inferSchema", "true").
load("/user/yz2444/project/taxi.csv")

taxi.dtypes.foreach(println)
taxi.show(3)

-- val taxiCount1 = taxi.select("pickup_community","endyear","enddate")
val taxiCount2 = taxi.select("dropoff_community","endyear","enddate")
val taxiCount3 = taxi.select("pickup_community","startyear","startdate")
-- val taxiCount4 = taxi.select("dropoff_community","startyear","startdate")

-- taxiCount1.show(5)
taxiCount2.show(5)
taxiCount3.show(5)
-- taxiCount4.show(5)


val taxiJoin1 = taxiCount2.unionAll(taxiCount3)
-- val taxiJoin2 = taxiJoin1.unionAll(taxiCount3)
-- val taxiJoin3 = taxiJoin2.unionAll(taxiCount4)

taxiJoin1.show(5)
taxiJoin1.count

val newNames = Seq("community", "year", "date")
val taxiJoin = taxiJoin1.toDF(newNames: _*)

val taxiFrequency  = taxiJoin.groupBy("community","year","date").count()


taxiFrequency.show(100)
taxiFrequency.count

val crimes = sqlCtx.read.format("com.databricks.spark.csv").
option("delimiter", ",").
option("header", "true").
option("inferSchema", "true").
load("/user/yz2444/project/crimes.csv")

crimes.dtypes.foreach(println)
crimes.show(3)

val crimesCount = crimes.groupBy("community","year","date").count()

val crimesTaxi = crimesCount.join(
  taxiFrequency,
  crimesCount("year") <=> taxiFrequency("year")
  && crimesCount("community") <=> taxiFrequency("community")
  && crimesCount("date") <=> taxiFrequency("date"),
  "inner"
)

crimesTaxi.dtypes.foreach(println)
crimesTaxi.show(3)

val newNames = Seq("communityCrimes", "yearCrimes", "dateCrimes","countCrimes","communityTaxi","yearTaxi","dateTaxi","countTaxi")
val crimesTaxiTable = crimesTaxi.toDF(newNames: _*)

val crimesTaxiTrain = crimesTaxiTable.where("yearCrimes = 2015")
val crimesTaxiCorr = crimesTaxiTrain.stat.corr("countCrimes","countTaxi")
-- crimesTaxiCorr: Double = 0.17146997101909367 2015
-- crimesTaxiCorr: Double = 0.17146997101909375 2016

