# WI 

## Geting Started 

### Requirements
This project was developed in scala with apache spark.

* Scala ( Version 2.11.12)
* Spark ( Version 2.3 ) 
* Sbt ( Version )

### Launching and Usage 

First you have to download the project 

```shell
git clone https://github.com/SamsonAudrey/WI.git
cd WI-master ???
```
Once placed in the project 

```shell
sbt
run [pathToData] [task]     
```
 
[pathToData]: replace it by your json file path

[task] : two possible values : *train*  **or** *predict*

usage examples :  *run mypath/to/data/data.json predict*   **or**     *run mypath/to/data/data.json train*
