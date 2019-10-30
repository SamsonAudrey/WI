import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {

  println("Spark")
  val spark: SparkSession = SparkSession
    .builder()
    .appName("WiClick")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("Lecture")
  val dataStudentsRaw: DataFrame = spark.read.json("/home/quentin/Téléchargements/data-students.json")

  println("Selection")
  val dataStudentsCleaned: DataFrame = DataCleaner.selectColumns(dataStudentsRaw)

  println("Ecriture")
  dataStudentsCleaned.write.json("/home/quentin/Téléchargements/data-students-cleaned.json")
}
