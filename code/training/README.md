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


# Folders and Files
```
.
├── crimesBigData
│   ├── build
│   │   ├── linear * (see sparkproject folder)
│   │   ├── randomforest * (see sparkproject folder)
│   │   └── Results.txt # The RMSE results of the two models with/without POI feature
│   ├── data
│   │   ├── input
│   │   │   ├── geo_taxi_test.txt # Test set with geo and taxi features
│   │   │   ├── geo_taxi_train.txt # Train set with geo and taxi features
│   │   │   ├── poi_geo_taxi_test.txt # Test set with poi, geo ,and taxi features
│   │   │   └── poi_geo_taxi_train.txt # Train set with poi, geo ,and taxi features
│   │   └── original
│   │       ├── crimesRate.txt # Data: crimes rate from 201501-201707
│   │       ├── geoFeature.txt # Data: geo feature from 201501-201707
│   │       ├── poiFeature.txt # Data: poi feature from 201501-201707
│   │       └── taxiFeature.txt # Data: taxi feature from 201501-201707
│   └── getData * (execution order: crimesRateCode, commRateAddComm, coffeeCountCode -> distanceCode -> geoCode, poiCode, taxiFlow -> originalToInput)
│       ├── coffeeCountCode.txt # To count the number of coffee shops in each community
│       ├── commRateAddComm.txt # To add a community number column
│       ├── crimesRateCode.txt # To calculate the crimes rate of each community
│       ├── distanceCode.txt # To calculate the distance between every (community_i, community_j) pairs
│       ├── geoCode.txt # To calculate the all geo feature: Geo_i = SUM (Weight_jn * CommunityCrimesRate_jn)
│       ├── originalToInput.txt # To transform datas in the original file to the datas in the input file (format: libsvm)
│       ├── poiCode.txt # To calculate the all poi feature
│       └── taxiFlow.txt # To calculate the one month taxi feature: Taxi_i = SUM (Weigth_jn * CommunityCrimesRate_jn)
├── crimesBirths
│   ├── build
│   │   ├── lrCrimesBirths.txt # Find a Linear Regression Equation by hand
│   │   └── mllibCrimesBirths.txt # Find a Linear Regression Equation by mllib
│   ├── data
│   │   └── libsvmCrimesBirths.txt # Data: libsvm format crimes-births dataset
│   └── getData
│       ├── connectCrimesBirths.sql # To connect crimes dataset and births dataset
│       └── convertFromatLibsvmCrimesBirths.txt # To convert the original format to Libsvm format
└── screenshots # Screenshots of some executation
```
