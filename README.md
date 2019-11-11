# WILLClick :crystal_ball:

## Project presentation 

### Subject
The objective of this project is to determine whether or not a user will click on an ad based on the characteristics of the user, the ad and its context.

### Team 
- Julien Roumagnac [julien-roumagnac](https://github.com/julien-roumagnac)
- Quentin France [Franceq34](https://github.com/Franceq34)
- Audrey Samson [SamsonAudrey](https://github.com/SamsonAudrey)
- Nathan Traineau [NathanTraineau](https://github.com/NathanTraineau)  
## Geting Started 

### Requirements :bangbang: :warning:
This project was developed in scala with apache spark.

* Scala (Version 2.11.12)
* Spark (Version 2.3) 
* Sbt   (Version 1.2.8)

### Launching and Usage :rocket:

First you have to download the project 

```shell
git clone https://github.com/SamsonAudrey/WI.git
cd WI
```
Once placed in the project 

```shell
sbt
run [pathToData] [task]     
```
 
*[pathToData]* : replace it by your json file path

*[task]* : two possible values : train  **or** predict

*usage examples* :  run mypath/to/data/data.json predict  &nbsp; &nbsp;  **or**  &nbsp; &nbsp;   run mypath/to/data/data.json train

### Results

you will find the result csv in the PREDICTIONCSV folder 
