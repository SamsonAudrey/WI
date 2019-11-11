package tools

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    val columnsToKeep = Seq("appOrSite", "timestamp", "size", "label", "os", "bidFloor", "publisher", "media", "user","interests","type","exchange")
    dataFrame.select(columnsToKeep.head, columnsToKeep.tail: _*)
  }


  /**
    * Replace empty columns of AppOrSite with "N/A"
    * @param dataFrame
    * @return DataFrame
    */

  def cleanAppOrSite(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(colName = "appOrSite",
      when(lower(col("appOrSite")).contains("app"), 1)
        .when(lower(col("appOrSite")).contains("site"), 2)
        .otherwise(3))

  }





  /**
    * @param dataFrame
    * @return DataFrame
    */
  def cleanOS(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(colName = "os",
      when(lower(col("os")).contains("android"), 1)
        .when(lower(col("os")).contains("ios"), 2)
        .when(lower(col("os")).contains("windows"), 3)
        .when(lower(col("os")).contains("rim"), 4)
        .otherwise(0))
  }

  /**
    * Replace empty columns of BidFloor with the average of all BidFloor values
    * @param dataFrame
    * @return DataFrame
    */
  def cleanBidFloor(dataFrame: DataFrame): DataFrame = {
    val avgBidFloor: DataFrame = dataFrame.select(avg("bidFloor") as "mean")
    val mean = avgBidFloor.select(col("mean")).first.get(0)
    dataFrame.na.fill(mean.toString.toDouble,Seq("bidFloor"))
  }

  /**
    * Replace empty columns of Publisher with "N/A"
    * @param dataFrame
    * @return DataFrame
    */
  def cleanPublisher(dataFrame: DataFrame): DataFrame = {
    dataFrame.na.fill(EMPTY_VAL,Seq("publisher"))
  }




  /**
    * Replace empty columns of User with "N/A"
    * @param dataFrame
    * @return DataFrame
    */
  def cleanUser(dataFrame: DataFrame): DataFrame = {
    dataFrame.na.fill(EMPTY_VAL,Seq("user"))
  }


  /**
    * Replace empty columns of Timestamp with "N/A"
    * @param dataFrame
    * @return DataFrame
    */
  def cleanTimestamp(dataFrame: DataFrame): DataFrame = {
    val df = dataFrame.withColumn("timestamp", hour(from_unixtime(col("timestamp"))))
    df.withColumn(colName = "timestamp",
      when(col("timestamp") >= 20, 1)
        .when(col("timestamp") >= 15 && col("timestamp") < 20, 2)
        .when(col("timestamp") >= 10 && col("timestamp") < 15, 3)
        .when(col("timestamp") >= 0 && col("timestamp") < 10, 4)
        .otherwise(5)
    )

  }
  def cleanInterests(dataFrame: DataFrame): DataFrame = {
    import spark.implicits._
    //Delete "IAB" and sub-categories of interests
    val dfWithoutIab = dataFrame.withColumn("interests", regexp_replace(dataFrame("interests"), "IAB|-[0-9]*", ""))
    //Fill N/A values
    val df_non_null = dfWithoutIab.na.fill("UNKNOWN", Seq("interests"))
    //Transform interests to Array of interest number
    var dfWithArray = df_non_null.withColumn("interests", split($"interests", ",").cast("array<String>"))
    //Create a new column for each interest with 0 (not interested) or 1 (interested)
    for (i <- 1 to 26) dfWithArray = dfWithArray.withColumn("IAB" + i.toString, array_contains(col("interests"), i.toString).cast("Int"))
    //dfWithArray.printSchema()
    //dfWithArray.show(10)
    dfWithArray
  }

  def cleanExchange(dataFrame: DataFrame): DataFrame = {
    val df_non_null = dataFrame.na.fill(0, Seq("exchange"))
    df_non_null.withColumn("exchange", when(col("exchange").contains("f8dd61fb7d4ebfa62cd6acceae3f5c69"), 1)
      .when(col("exchange").contains("c7a327a5027c1c4de094b0a9f33afad6"), 2)
      .when(col("exchange").contains("46135ae0b4946b5f2f74274e5618e697"), 3)
      .when(col("exchange").contains("fe86ac12a6d9ccaa8a2be14a80ace2f8"), 4)
      .otherwise(0)
    )
  }
  def cleanType(dataFrame: DataFrame): DataFrame = {
    val cleanDF = dataFrame.withColumn("type",
      when(col("type") === "CLICK", 4)
        .when(col("type") === "0", 0)
        .when(col("type") === "1", 1)
        .when(col("type") === "2", 2)
        .when(col("type") === "3", 3))
    cleanDF.na.fill(5, Seq("type"))
  }

  def cleanMedia(dataFrame: DataFrame): DataFrame = {
    val df_non_null = dataFrame.na.fill(EMPTY_VAL, Seq("media"))
    df_non_null.withColumn("media", when(col("media").contains("d476955e1ffb87c18490e87b235e48e7"), 1)
      .when(col("media").contains("343bc308e60156fb39cd2af57337a958"), 2)
      .otherwise(0)
    )
  }
  def cleanSize(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("size",
      when(col("size").isNotNull && col("size")(0).equals(col("size")(1)), 1)
        .when(col("size").isNotNull && col("size")(0) > col("size")(1), 2)
        .when(col("size").isNotNull && col("size")(0) < col("size")(1), 3)
        .otherwise(0)
    )
  }

  /**
    *
    * @param dataFrame
    * @return DataFrame
    */
  def cleanLabel(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(colName = "label",
      when(col("label") === "false", 1.0)
        .otherwise(0.0)
    )
  }


  /**
    * Clean the dataframe by cleaning every column
    * @param dataFrame
    * @return DataFrame
    */
  def clean(dataFrame: DataFrame): DataFrame = {
    val newDataFrame = dataFrame
    val dataFrameCleanCol = selectColumns(newDataFrame)
    val dataFrameCleanSize = cleanSize(dataFrameCleanCol)
    val dataFrameCleanAOS = cleanAppOrSite(dataFrameCleanSize)
    val dataFrameCleanOS = cleanOS(dataFrameCleanAOS)
    val dataFrameCleanBFloor = cleanBidFloor(dataFrameCleanOS)
    val dataFrameCleanPublisher = cleanPublisher(dataFrameCleanBFloor)
    val dataFrameCleanMedia = cleanMedia(dataFrameCleanPublisher)
    val dataFrameCleanUser = cleanUser(dataFrameCleanMedia)
    val dataFrameCleanInterests = cleanInterests(dataFrameCleanUser)
    val dataFrameCleanTimestamp = cleanTimestamp(dataFrameCleanInterests)
    val dataFrameCleanType = cleanType(dataFrameCleanTimestamp)
    val dataFrameCleanExchange = cleanExchange(dataFrameCleanType)
    val dataFrameCleanLabel = cleanLabel(dataFrameCleanExchange)
    dataFrameCleanLabel
  }


  def transfromToIndexColumn(dataFrame: DataFrame, cols: Array[String]): DataFrame = {
    var dataFrameIndex = dataFrame

    for (col <- cols) {
      val indexer = new StringIndexer().setInputCol(col).setOutputCol(col + "Index")
      val indexed = indexer.fit(dataFrameIndex).transform(dataFrameIndex).drop(col)
      dataFrameIndex = indexed
    }

    dataFrameIndex//.orderBy(rand())

  }

  def underSampleDF(df : DataFrame) : DataFrame = {

    println(df.count())
    val dfpos = df.filter(df("label") === 1.0)
    val nbPos = dfpos.count()
    val dfneg = df.filter(df("label") === 0.0).limit(nbPos.toInt)

    val df2 = dfpos.union(dfneg).orderBy(rand())

    df2

  }
  def predictionToRes(df : DataFrame) : DataFrame = {
    var df2 :DataFrame = null
    df2.withColumn("label",df("prediction"))


    df
  }

}
