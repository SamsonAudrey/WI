import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, _}

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
    val avgBidFloor: DataFrame = dataFrame.select(avg("bidFloor") as "mean")
    val mean = avgBidFloor.select(col("mean")).first.get(0)
    dataFrame.na.fill(mean.toString.toDouble,Seq("bidFloor"))
  }

  /**
    * @param dataFrame
    * @return DataFrame
    */
  def cleanPublisher(dataFrame: DataFrame): DataFrame = {
    dataFrame.na.fill(EMPTY_VAL,Seq("publisher"))
  }

  /**
    * @param dataFrame
    * @return DataFrame
    */
  def cleanMedia(dataFrame: DataFrame): DataFrame = {
    dataFrame.na.fill(EMPTY_VAL,Seq("media"))
  }

  /**
    * @param dataFrame
    * @return DataFrame
    */
  def cleanUser(dataFrame: DataFrame): DataFrame = {
    dataFrame.na.fill(EMPTY_VAL,Seq("user"))
  }

  /**
    * @param dataFrame
    * @return DataFrame
    */
  def cleanInterest(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(colName = "interests",
      when(col("interests").isNotNull, regexp_replace(dataFrame("interests"), "(-)[0-9]+", ""))
        .otherwise(EMPTY_VAL)
    )
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
    val dataFrameCleanPublisher = cleanPublisher(dataFrameCleanBFloor)
    val dataFrameCleanMedia = cleanMedia(dataFrameCleanPublisher)
    val dataFrameCleanUser = cleanUser(dataFrameCleanMedia)
    val dataFrameCleanInterests = cleanInterest(dataFrameCleanUser)
    dataFrameCleanInterests
  }

}
