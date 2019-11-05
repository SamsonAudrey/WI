import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("WiClick")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  println("What is the path of the dataFrame ? (please enter the path of a .json file)")
  val input = scala.io.StdIn.readLine()
  
  if (!new File(input).exists()) {
    println("This file doesn't exist.")
  }

  else {
    val folderPath = input.split("/").map(_.trim).toList.dropRight(1).mkString("/")
    val resultPath = folderPath.concat("/data-students-results")
    // /Users/audreysamson/Downloads/data-students.json

    println("Read")
    val dataStudentsRaw: DataFrame = spark.read.json(input)

    println("Cleaning")
    val dataStudentsCleaned = DataCleaner.clean(dataStudentsRaw)

    println(s"Write (in $resultPath folder)")
    deleteRecursively(new File(resultPath))
    dataStudentsCleaned.write.json(resultPath)
  }


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