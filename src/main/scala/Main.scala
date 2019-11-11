import java.io.File

import models.RFModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import tools.{DataCleaner, Metrics , Timer}

object Main extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("WiClick")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")




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

    val dataStudentsCleaned = DataCleaner.clean(dataStudentsRaw)





    task match {

      case "train" =>  RFModel.train(dataStudentsCleaned)

      case "predict"=>   {
        val myModel = RFModel.load()
        val predictionDf = RFModel.predict(dataStudentsCleaned, myModel )

        Metrics.show(predictionDf)

        var timerDataset = dataStudentsRaw.limit(1000)

        println ("CLEANING TIME FOR 1000 LINES ")
        Timer.time {

          timerDataset = DataCleaner.clean(timerDataset)


        }
        println ("TIME TO LOAD THE MODEL \n  ")
        Timer.time {

          RFModel.load()

        }

        println ("PREDICTING TIME FOR 1000 LINES \n  ")
        Timer.time {

          val predictionTimerDf = RFModel.predict(timerDataset, myModel )

        }

        //val res : DataFrame= res.withColumn("label" , predictionDf("prediction"))
        //res.coalesce(1).write.csv("res.csv")


      }
      case _ => println(" TASK unknown you should choose a task between train and predict ")
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




