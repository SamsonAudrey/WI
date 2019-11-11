import java.io.File

import models.RFModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import tools.{DataCleaner, Metrics}

object Main extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("WiClick")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  /*println("What is the path of the dataFrame ? (please enter the path of a .json file)")
  val input = scala.io.StdIn.readLine()

  if (!new File(input).exists()) {
    println("This file doesn't exist.")
  }

  else {
    val folderPath = input.split("/").map(_.trim).toList.dropRight(1).mkString("/")
    val resultPath = folderPath.concat("/data-students-results")
    // /Users/audreysamson/Downloads/data-students.json*/

  var pathToDataJSON = "data-students.json"
  var model = "randomForest"
  var task = "predict"
  var usage = "Usage: sbt run path/to/data.json [task]"
  var defaultValues = "Default values ( sbt run ): " + pathToDataJSON + "  " + model + "  " + task
  var possibleValues = "Possibles values: \n[task]: predict or train"

  println(s"NB ARGS : ${args.length}")
  if(args.length > 0) {
    if(args(0) == "help" || args(0) == "usage") {

      println("\n"+usage)
      println(possibleValues)
      println("\n"+defaultValues)
      System.exit(0)
    }
    if(args.length > 1) {

    pathToDataJSON = args(0)
    task = args(1)
    println(s"path : ${args(0)} task : ${args(1)}")
    }
  }




    println(s"Reading $pathToDataJSON ")
    val dataStudentsRaw: DataFrame = spark.read.json(pathToDataJSON)

    println(s"Cleaning $pathToDataJSON ")

    val dataStudentsCleaned = DataCleaner.clean(dataStudentsRaw.limit(1000000))
  dataStudentsCleaned.show(10)


    // Indexing
    val indexedDataFrame = DataCleaner.transfromToIndexColumn(dataStudentsCleaned, Array("appOrSite", "size", "os", "timestamp", "publisher", "media", "user"))


    task match {

      case "train" =>  RFModel.train(indexedDataFrame)

      case "predict"=>   {

        val predictionDf = RFModel.predict(indexedDataFrame, RFModel.load())

        Metrics.show(predictionDf)
      }
    }






    spark.close()



  /**
    *
    * @param file : File to delete
    */
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }
  }




