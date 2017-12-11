crimesBigData
	coffeeCountCode.txt
		-- To count the number of coffee shops in each community
	commCrimesAddComm.txt
		-- To add a community number column
	crimesCountCode.txt
		-- To calculate the crimes rate of each community
	distanceCode.txt
		-- To calculate the distance between every (community_i, community_j) pairs
	geoCode.txt
		-- To calculate the geo feature: Geo_i = SUM (Weight_jn * Community_jn)
	poiCode.txt
	    -- To calculate the poi feature
crimesBirths
	connectCrimesBirths.sql
		-- To connect crimes dataset and births dataset
	convertFromatLibsvmCrimesBirths.txt
		-- To convert the original format to Libsvm format
	libsvmCrimesBirths.txt
		-- Data: libsvm format crimes-births dataset
	lrCrimesBirths.txt
		-- Find a Linear Regression Equation by hand
	mllibCrimesBirths.txt
		-- Find a Linear Regression Equation by mllib
model
	trainingCodeTemplate.txt
		-- The template code showing how to use mllib
	trainingDataCodeLRSGD.txt
		-- Reformat the dataset
		-- Build 5 models for comm1, comm2, comm3, comm4, comm5