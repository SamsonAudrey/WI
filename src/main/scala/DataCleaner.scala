import org.apache.spark.sql.{DataFrame, SparkSession}

object DataCleaner {

  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "WiClick")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  /**
   * Keep the columns that are interesting
   * @param dataFrame
   */
  def selectColumns(dataFrame: DataFrame): DataFrame = {
    //val allColumns = Seq("network", "appOrSite", "timestamp", "size", "label", "os", "exchange", "bidFloor", "publisher", "media", "user", "interests", "type", "city", "impid")
    val columnsToKeep = Seq("appOrSite", "size", "label", "os", "bidFloor", "publisher", "media", "user", "interests")
    dataFrame.select(columnsToKeep.head, columnsToKeep.tail: _*)
  }

}
