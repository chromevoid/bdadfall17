# General Info
- Dumbo set spark 2.2.0:
	- `module load java/1.8.0_72`  
	- `module load spark/2.2.0`

# Build Compile Package
- install sbt
- under project folder
- run `/sbt/bin/./sbt` to start building project
- enter `compile` to compile
- enter `package` tp package
- press ctrl + Z to stop building
- run `./exec.sh`
- get an output.txt file


# Folders and Files
```
.
├── bigdata
│   ├── code
│   │   ├── app * (see sparkproject folder)
│   │   ├── App.txt # Application code for spark-shell
│   │   └── getAppInputs.txt # To get the inputs of the application: the crimes, taxi, and coffee of a certain month
│   ├── data
│   │   └── test17
│   │       ├── crimes # The crimes rate input of month January 2017
│   │       ├── distance # The normalized distance between two communities, used as the weight for calculating geo feature
│   │       ├── help # Two help files to help insert missing data in poi and taxi datasets
│   │       ├── poi # The poi input of month January 2017
│   │       ├── population # The population data of each community, used to calculate the crimes rate
│   │       └── taxi # The taxi input of month January 2017
│   └── output
│       └── output.csv # the crimes rate prediction of each community, January 2017
├── birth
│   ├── code
│   │   └── birth * (see sparkproject folder)
│   └── Birth.txt # Application code for spark-shell
└── AppDataFLow.txt # Introduce the data flow in our two applications
```
