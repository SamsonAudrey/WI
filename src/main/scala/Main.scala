import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {

  println("Spark")
  val spark: SparkSession = SparkSession
    .builder()
    .appName("WiClick")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("Read")
  val dataStudentsRaw: DataFrame = spark.read.json("/Users/audreysamson/Downloads/data-students.json")

  println("Cleaning")
  val dataStudentsCleaned = DataCleaner.clean(dataStudentsRaw)

  /*println("Select")
  val dataStudentsCleaned: DataFrame = DataCleaner.selectColumns(dataStudentsRaw)*/

  println("Write")
  val path = "/Users/audreysamson/Downloads/data-students"
  deleteRecursively(new File(path))
  dataStudentsCleaned.write.json(path)


  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }
}