crimesBigData
  build
    LinearRegression.scala
      -- To build model using linear regression: [training] predicted month 201502-201706, [testing] predicted month 201707
    RandomForestRegrssion.scala
      -- To build model using random forest regression: [training] predicted month 201502-201706, [testing] predicted month 201707
    Results.txt
      -- The RMSE results of the two models with/without POI feature
  data
    input
      geo_taxi_test.txt
        -- Test set with geo and taxi features
      geo_taxi_train.txt
        -- Train set with geo and taxi features
      poi_geo_taxi_test.txt
        -- Test set with poi, geo ,and taxi features
      poi_geo_taxi_train.txt
        -- Train set with poi, geo ,and taxi features
    original
      crimesRate.txt
        -- Data: crimes rate from 201501-201707
      geoFeature.txt
        -- Data: geo feature from 201501-201707
      poiFeature.txt
        -- Data: poi feature from 201501-201707
      taxiFeature.txt
        -- Data: taxi feature from 201501-201707
  getData
    coffeeCountCode.txt
      -- To count the number of coffee shops in each community
    commRateAddComm.txt
      -- To add a community number column
    crimesRateCode.txt
      -- To calculate the crimes rate of each community
    distanceCode.txt
      -- To calculate the distance between every (community_i, community_j) pairs
    geoCode.txt
      -- To calculate the geo feature: Geo_i = SUM (Weight_jn * CommunityCrimesRate_jn)
    poiCode.txt
      -- To calculate the poi feature
    taxiFlow.txt
      -- To calculate the taxi feature: Taxi_i = SUM (Weigth_jn * CommunityCrimesRate_jn)
crimesBirths
  build
    lrCrimesBirths.txt
      -- Find a Linear Regression Equation by hand
    mllibCrimesBirths.txt
      -- Find a Linear Regression Equation by mllib
  data
    libsvmCrimesBirths.txt
      -- Data: libsvm format crimes-births dataset
  getData
    connectCrimesBirths.sql
      -- To connect crimes dataset and births dataset
    convertFromatLibsvmCrimesBirths.txt
      -- To convert the original format to Libsvm format



