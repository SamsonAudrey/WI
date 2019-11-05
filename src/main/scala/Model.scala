import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer, _}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object Model  {

  // Assume that the DF is clean
  def train(df :DataFrame) {
    df.printSchema()
    df.show(10)
    println("NB ROWS : " + df.count())
    df.limit(100)

    // Choice of the features used to predict
    val cols = Array("bidfloor", "type")
    // Adding the features column to the DF
    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")
    val featureDf = assembler.transform(df)
    featureDf.printSchema()
    featureDf.show(5)

    // Creation of the prediction column linked to the label
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("pred")


    val seed = 5043
    val randomForestClassifier = new RandomForestClassifier()

    // Spliting Data in test and train set
    val Array(pipelineTrainingData, pipelineTestingData) = df.randomSplit(Array(0.75, 0.25), seed)

    println("MODEL PIPELINE : ")
    val stages = Array(assembler, indexer, randomForestClassifier)

    // build pipeline
    val pipeline = new Pipeline().setStages(stages)
    val pipelineModel = pipeline.fit(pipelineTrainingData)

    // test model with test data
    val pipelinePredictionDf = pipelineModel.transform(pipelineTestingData)
    pipelinePredictionDf.show(20)


    // evaluate the model
    val predictionsAndLabels = pipelinePredictionDf.select("pred", "label").rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))

    val metrics = new MulticlassMetrics(predictionsAndLabels)

    val confusionMatrix = metrics.confusionMatrix
    // compute the false positive rate per label
    val predictionColSchema = pipelinePredictionDf.schema("prediction")

    println(s" Confusion Matrix\n ${confusionMatrix.toString}\n")
    // Define evaluator and his metrics
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("weightedPrecision")
    // measure the accuracy
    val accuracy = evaluator.evaluate(pipelinePredictionDf)
    println(accuracy)


    // Values of parameters of the tree to try to optimize it
    val paramGrid = new ParamGridBuilder()
      .addGrid(randomForestClassifier.maxBins, Array(25, 28, 31))
      .addGrid(randomForestClassifier.numTrees, Array(3, 5, 7))
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


    // measure the accuracy of cross validated model
    // this model is more accurate than the old model
    val cvAccuracy = evaluator.evaluate(cvPredictionDf)
    println("CROOS VALIDATOR ACCURACY")
    println(cvAccuracy)



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
  }

}
