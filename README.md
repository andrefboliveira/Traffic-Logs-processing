# Traffic-Logs-processing
Individual project titled "Traffic Logs processing" for the course "Computação em Nuvem" (Cloud Computing) for the school year 2016/2017 of the Master's degree in Informatics (Mestrado em Informática) at FCUL/ULisboa

## Project Stack
Hadoop (HDFS, MapReduce), Spark, Scala

## Details
Individual project

Development of a Big Data application to process network traffic logs an answer possible questions from the sample dataset.
The goals was to provide insight to an organization based on the analysis of this sample network traffic logs.

Implementation in both Apache Spark (Scala) and Hadoop MapReduce (Java).

For more information, please check the [attached report](Report%2045648%20-%20MapReduce%20Spark.pdf)

## Code structure
The general organization of this project is as follows:
- [dataset](./dataset) contains the dataset used for analysis (data is stored in Git Large File Storage)
- [Code](./Code) contains the developed code including both the code text and the compiled .JAR files
  - [InformationSent.java](./Code/InformationSent.java) contains the Hadoop MapReduce solution
  - [TrafficLogsProcessing.scala](./Code/TrafficLogsProcessing.scala) contains the Apache Spark using Scala solution
- [Final results](./Final%20results) contains the output for both Big Data applications

## Contributors
- André Filipe Bernardes Oliveira
