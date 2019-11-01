import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataCleaner {

  val EMPTY_VAL = "N/A"

  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "WiClick")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  /**
   * Keep the columns that are interesting
   * @param dataFrame
    * @return DataFrame
   */
  def selectColumns(dataFrame: DataFrame): DataFrame = {
    //val allColumns = Seq("network", "appOrSite", "timestamp", "size", "label", "os", "exchange", "bidFloor", "publisher", "media", "user", "interests", "type", "city", "impid")
    val columnsToKeep = Seq("appOrSite", "size", "label", "os", "bidFloor", "publisher", "media", "user", "interests")
    dataFrame.select(columnsToKeep.head, columnsToKeep.tail: _*)
  }

  /**
    * @param dataFrame
    * @return DataFrame
    */
  def cleanOS(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(colName = "os",
      when(lower(col("os")).contains("android"), "android")
        .when(lower(col("os")).contains("ios"), "ios")
        .when(lower(col("os")).contains("windowsphone"), "windowsphone")
        .when(lower(col("os")).contains("rim"), "rim")
        .otherwise(EMPTY_VAL))
  }

  /**
    * @param dataFrame
    * @return DataFrame
    */
  def cleanBidFloor(dataFrame: DataFrame): DataFrame = {
    val avgBidFloor: DataFrame = dataFrame.select(avg("bidFloor") as "mean") //.agg(avg("bidFloor") as "mean")
    val mean = avgBidFloor.select(col("mean")).first.get(0)
    dataFrame.na.fill(mean.toString.toDouble,Seq("bidFloor"))
  }


  /**
    *
    * @param dataFrame
    * @return DataFrame
    */
  def clean(dataFrame: DataFrame): DataFrame = {
    val newDataFrame = dataFrame
    val dataFrameCleanCol = selectColumns(newDataFrame)
    val dataFrameCleanOS = cleanOS(dataFrameCleanCol)
    val dataFrameCleanBFloor = cleanBidFloor(dataFrameCleanOS)
    dataFrameCleanBFloor
  }

}
