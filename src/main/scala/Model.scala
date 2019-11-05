
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer, _}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object Model extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().config("spark.master","local").getOrCreate()
  spark.conf.set("spark.sql.codegen.wholeStage", false)
  import spark.implicits._

  print("\033[H\033[2J") // delete everything on the screen

  var df =  spark.read.format("json").load("data-students.json").limit(500)
  df = cleanLabel(df)
  // pas de string pour le model !
  df = cleanType(df)
  df = cleanSize(df)
  df.printSchema()
  df.show(10)
  println("NB ROWS : "+ df.count())


  val cols = Array("bidfloor","type")

  // VectorAssembler to add feature column
  // input columns - cols
  // feature column - features
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")
  val featureDf = assembler.transform(df)
  featureDf.printSchema()
  featureDf.show(5)
  println("features juste avant")

  val indexer = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("pred")
  //val labelDf = indexer.fit(featureDf).transform(featureDf)
  //labelDf.printSchema()

  println("Fonctionne")
  val seed = 5040
  val randomForestClassifier = new RandomForestClassifier()

  // RANDOM FOREST NORMAL
  /*println("MODEL NORMAL : ")

  val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

    .setImpurity("gini")
    .setMaxDepth(3)
    .setNumTrees(3)
    .setFeatureSubsetStrategy("auto")
    .setSeed(seed)
  val randomForestModel = randomForestClassifier.fit(trainingData)
  println(randomForestModel.toDebugString)

  val predictionDf = randomForestModel.transform(testData)
  predictionDf.show(10)

  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("pred")
    .setMetricName("areaUnderROC")

  // measure the accuracy
  val accuracy = evaluator.evaluate(predictionDf)
  println(accuracy) */

  println("MODEL PIPELINE : ")

  val Array(pipelineTrainingData, pipelineTestingData) = df.randomSplit(Array(0.7, 0.3), seed)

  // VectorAssembler and StringIndexer are transformers
  // LogisticRegression is the estimator
  val stages = Array(assembler, indexer, randomForestClassifier)

  // build pipeline
  val pipeline = new Pipeline().setStages(stages)
  val pipelineModel = pipeline.fit(pipelineTrainingData)

  // test model with test data
  val pipelinePredictionDf = pipelineModel.transform(pipelineTestingData)
  pipelinePredictionDf.show(10)

  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("pred")
    .setMetricName("areaUnderROC")

  // measure the accuracy
  val accuracy = evaluator.evaluate(pipelinePredictionDf)
  println(accuracy)

  val paramGrid = new ParamGridBuilder()
    .addGrid(randomForestClassifier.maxBins, Array(25, 28, 31))
    .addGrid(randomForestClassifier.maxDepth, Array(4, 6, 8))
    .addGrid(randomForestClassifier.impurity, Array("entropy", "gini"))
    .build()

  // define cross validation stage to search through the parameters
  // K-Fold cross validation with BinaryClassificationEvaluator
  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(4)

  // fit will run cross validation and choose the best set of parameters
  // this will take some time to run
  val cvModel = cv.fit(pipelineTrainingData)


  // test cross validated model with test data
  val cvPredictionDf = cvModel.transform(pipelineTestingData)
  cvPredictionDf.show(10)
  /*
   * output
  +-------------+-------+--------+-------+-------+------+-------+----------+-----------+----------+----------+-----------------+------+---+----------+---------+-------+----------+----------+--------+-------+--------------------+-----+--------------------+--------------------+----------+
  |creditability|balance|duration|history|purpose|amount|savings|employment|instPercent|sexMarried|guarantors|residenceDuration|assets|age|concCredit|apartment|credits|occupation|dependents|hasPhone|foreign|            features|label|       rawPrediction|         probability|prediction|
  +-------------+-------+--------+-------+-------+------+-------+----------+-----------+----------+----------+-----------------+------+---+----------+---------+-------+----------+----------+--------+-------+--------------------+-----+--------------------+--------------------+----------+
  |          0.0|    1.0|       6|      1|      6|  1198|      1|         5|          4|         2|         1|                4|     4| 35|         3|        3|      1|         3|         1|       1|      1|[1.0,6.0,1.0,6.0,...|  1.0|[9.44068018310895...|[0.47203400915544...|       1.0|
  |          0.0|    1.0|       6|      4|      2|  3384|      1|         3|          1|         1|         1|                4|     1| 44|         3|        1|      1|         4|         1|       2|      1|[1.0,6.0,4.0,2.0,...|  1.0|[16.4613425903485...|[0.82306712951742...|       0.0|
  |          0.0|    1.0|       9|      2|      3|  1366|      1|         2|          3|         2|         1|                4|     2| 22|         3|        1|      1|         3|         1|       1|      1|[1.0,9.0,2.0,3.0,...|  1.0|[10.0474979819529...|[0.50237489909764...|       0.0|
  |          0.0|    1.0|      12|      1|      0|   697|      1|         2|          4|         3|         1|                2|     3| 46|         1|        2|      2|         3|         1|       2|      1|[1.0,12.0,1.0,0.0...|  1.0|[7.16755347773376...|[0.35837767388668...|       1.0|
  |          0.0|    1.0|      12|      2|      4|   741|      2|         1|          4|         2|         1|                3|     2| 22|         3|        2|      1|         3|         1|       1|      1|[1.0,12.0,2.0,4.0...|  1.0|[7.83036979341298...|[0.39151848967064...|       1.0|
  |          0.0|    1.0|      18|      1|      0|  1442|      1|         4|          4|         3|         1|                4|     4| 32|         1|        3|      2|         2|         2|       1|      1|[1.0,18.0,1.0,0.0...|  1.0|[7.57249457752780...|[0.37862472887639...|       1.0|
  |          0.0|    1.0|      18|      2|      3|  3190|      1|         3|          2|         2|         1|                2|     1| 24|         3|        2|      1|         3|         1|       1|      1|[1.0,18.0,2.0,3.0...|  1.0|[11.1825215511680...|[0.55912607755840...|       0.0|
  |          0.0|    1.0|      18|      4|      2|  2124|      1|         3|          4|         2|         1|                4|     1| 24|         3|        1|      2|         3|         1|       1|      1|[1.0,18.0,4.0,2.0...|  1.0|[12.8914852461513...|[0.64457426230756...|       0.0|
  |          0.0|    1.0|      18|      4|      5|  1190|      1|         1|          2|         2|         1|                4|     4| 55|         3|        3|      3|         1|         2|       1|      1|[1.0,18.0,4.0,5.0...|  1.0|[12.5088357629865...|[0.62544178814932...|       0.0|
  |          0.0|    1.0|      20|      4|      0|  2235|      1|         3|          4|         4|         3|                2|     2| 33|         1|        1|      2|         3|         1|       1|      2|[1.0,20.0,4.0,0.0...|  1.0|[13.3533632679050...|[0.66766816339525...|       0.0|
  +-------------+-------+--------+-------+-------+------+-------+----------+-----------+----------+----------+-----------------+------+---+----------+---------+-------+----------+----------+--------+-------+--------------------+-----+--------------------+--------------------+----------+
  */

  // measure the accuracy of cross validated model
  // this model is more accurate than the old model
  val cvAccuracy = evaluator.evaluate(cvPredictionDf)
  println("CROOS VALIDATOR ACCURACY")
  println(cvAccuracy)

  def cleanLabel (df: DataFrame): DataFrame = {
    df.withColumn("label",
      when(
        col("label") === "true",
        1.0).otherwise(0.0)
      // if the column value label is equal to false set value to 0 otherwise set the value to 1
    )
  }
  private def cleanType (df: DataFrame): DataFrame = {
    df.withColumn("type",
      when(
        col("type").isNotNull and
          (col("type") === 1 or col("type") === 2 or col("type") === 3 or col("type") === 4),
        col("type").cast("Double")
      ).otherwise(-1).cast("Double")
    ).na.fill(-1, Seq("type"))
  }
  private def cleanSize (df: DataFrame): DataFrame = {

  df.withColumn("size",
    when(col("size").isNotNull,
      concat(col("size")(0).cast("STRING"), lit("x"), col("size")(1).cast("STRING"))
    ).otherwise("NO_VALUE")
  )
  /* Function comming from the org.apache.spark.sql.functions
  * withColumn: Create a new column with as name the first parameter
  * (or replace if a column with the same name exists)
  * contact: contact several column into a new one
  * lit: create a new column with the value given in parameter of the function
  */
  }
  spark.stop()
  }
